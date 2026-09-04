defmodule SelectoDBPostgreSQL.StreamProtocolConformanceTest do
  use ExUnit.Case, async: true

  alias SelectoDBPostgreSQL.Adapter

  test "two concurrent enumerations get isolated producers and complete row sequences" do
    test_pid = self()

    {:ok, stream} =
      Adapter.stream(self(), "select 1", [],
        receive_timeout: 500,
        stream_producer: fn send_chunk ->
          send(test_pid, {:producer_started, self()})
          send_chunk.([[1], [2]], ["id"])
          {:ok, :complete}
        end
      )

    enumerators = for _ <- 1..2, do: Task.async(fn -> Enum.to_list(stream) end)
    expected = [{[1], ["id"]}, {[2], ["id"]}]

    assert Enum.map(enumerators, &Task.await(&1, 2_000)) == [expected, expected]
    assert_receive {:producer_started, first_producer}
    assert_receive {:producer_started, second_producer}
    assert first_producer != second_producer
  end

  test "consumer cancellation retires a blocked producer" do
    test_pid = self()

    {:ok, stream} =
      Adapter.stream(self(), "select 1", [],
        queue_timeout: 25,
        receive_timeout: 500,
        stream_producer: fn send_chunk ->
          send(test_pid, {:producer_started, self()})
          send_chunk.([[1]], ["id"])

          receive do
            :release -> {:ok, :complete}
          end
        end
      )

    enumerator = Task.async(fn -> Enum.take(stream, 1) end)
    assert_receive {:producer_started, producer}
    monitor = Process.monitor(producer)

    assert Task.await(enumerator, 2_000) == [{[1], ["id"]}]
    assert_receive {:DOWN, ^monitor, :process, ^producer, _reason}, 500
  end

  test "fast producer cannot outrun demand and cancellation drains its protocol messages" do
    test_pid = self()

    {:ok, stream} =
      Adapter.stream(self(), "select 1", [],
        queue_timeout: 25,
        receive_timeout: 500,
        stream_producer: fn send_chunk ->
          send(test_pid, {:producer_started, self()})

          Enum.each(1..100, fn value ->
            send(test_pid, {:producer_attempt, value})
            send_chunk.([[value]], ["id"])
          end)

          {:ok, :complete}
        end
      )

    assert Enum.take(stream, 1) == [{[1], ["id"]}]
    assert_receive {:producer_started, producer}
    assert_receive {:producer_attempt, 1}
    refute_receive {:producer_attempt, 2}, 25
    refute Process.alive?(producer)
    assert stream_protocol_messages() == []
  end

  test "receive timeout is surfaced and retires the producer" do
    test_pid = self()

    {:ok, stream} =
      Adapter.stream(self(), "select 1", [],
        queue_timeout: 25,
        receive_timeout: 25,
        stream_producer: fn _send_chunk ->
          send(test_pid, {:producer_started, self()})
          Process.sleep(:infinity)
        end
      )

    assert_raise RuntimeError, ~r/Timed out waiting for streamed rows/, fn ->
      Enum.to_list(stream)
    end

    assert_receive {:producer_started, producer}
    monitor = Process.monitor(producer)
    assert_receive {:DOWN, ^monitor, :process, ^producer, _reason}, 500
  end

  test "transaction errors are surfaced without waiting for the receive timeout" do
    {:ok, stream} =
      Adapter.stream(self(), "select 1", [],
        receive_timeout: 5_000,
        stream_producer: fn _send_chunk -> {:error, :rolled_back} end
      )

    started_at = System.monotonic_time(:millisecond)

    assert_raise RuntimeError,
                 ~r/PostgreSQL stream transaction failed: :rolled_back/,
                 fn -> Enum.to_list(stream) end

    assert System.monotonic_time(:millisecond) - started_at < 1_000
  end

  @tag :postgres
  test "real cursor enumeration and early cancellation leave the connection reusable" do
    {:ok, connection} =
      Adapter.connect(SelectoDBPostgreSQL.Verification.ConnectionOptions.options())

    Process.unlink(connection)

    on_exit(fn ->
      if Process.alive?(connection), do: GenServer.stop(connection)
    end)

    assert {:ok, %{rows: [[backend_pid]]}} =
             Adapter.execute(connection, "SELECT pg_backend_pid()", [], [])

    assert {:ok, _} =
             Adapter.execute(
               connection,
               "CREATE TEMP TABLE selecto_stream_session_witness (id integer)",
               [],
               []
             )

    {:ok, complete_stream} =
      Adapter.stream(
        connection,
        "SELECT value FROM generate_series(1, 5) AS value ORDER BY value",
        [],
        max_rows: 2,
        receive_timeout: 2_000
      )

    enumerator = Task.async(fn -> Enum.to_list(complete_stream) end)

    assert Task.await(enumerator, 5_000) ==
             Enum.map(1..5, &{[&1], ["value"]})

    {:ok, cancelled_stream} =
      Adapter.stream(
        connection,
        "SELECT value FROM generate_series(1, 100) AS value ORDER BY value",
        [],
        max_rows: 1,
        queue_timeout: 100,
        receive_timeout: 2_000
      )

    assert Enum.take(cancelled_stream, 1) == [{[1], ["value"]}]
    assert stream_protocol_messages() == []
    assert {:ok, %{rows: [[1]]}} = Adapter.execute(connection, "SELECT 1", [], [])

    assert {:ok, %{rows: [[^backend_pid]]}} =
             Adapter.execute(connection, "SELECT pg_backend_pid()", [], [])

    assert {:ok, %{rows: [[0]]}} =
             Adapter.execute(
               connection,
               "SELECT count(*) FROM selecto_stream_session_witness",
               [],
               []
             )
  end

  defp stream_protocol_messages do
    {:messages, messages} = Process.info(self(), :messages)
    Enum.filter(messages, &stream_protocol_message?/1)
  end

  defp stream_protocol_message?({_ref, {:chunk, _rows, _columns}}), do: true
  defp stream_protocol_message?({_ref, {:done, _result}}), do: true
  defp stream_protocol_message?({_ref, {:producer_failed, _reason}}), do: true
  defp stream_protocol_message?(_message), do: false
end
