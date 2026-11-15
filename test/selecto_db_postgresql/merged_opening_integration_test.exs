defmodule SelectoDBPostgreSQL.MergedOpeningIntegrationTest do
  use ExUnit.Case, async: false

  alias SelectoDBPostgreSQL.Adapter
  alias SelectoDBPostgreSQL.Verification.ConnectionOptions

  @moduletag :postgres

  # Run the real Postgrex protocol, injecting only a failed ROLLBACK. This
  # proves that the adapter requests disconnect, rather than relying on a
  # socket which has already died to make the test pass.
  defmodule RollbackFailureProtocol do
    @behaviour DBConnection

    for {name, arity} <- DBConnection.behaviour_info(:callbacks),
        name not in [:connect, :disconnect, :handle_execute] do
      args = Macro.generate_arguments(arity, __MODULE__)
      defdelegate unquote(name)(unquote_splicing(args)), to: Postgrex.Protocol
    end

    def connect(opts) do
      with {:ok, state} <-
             Postgrex.Protocol.connect(Keyword.put(opts, :types, Postgrex.DefaultTypes)) do
        {:ok,
         Map.merge(state, %{
           test_owner: Keyword.fetch!(opts, :test_owner),
           fail_opening: Keyword.get(opts, :fail_opening, false),
           fail_rollback: Keyword.get(opts, :fail_rollback, false)
         })}
      end
    end

    def disconnect(error, state) do
      send(state.test_owner, {:disconnected, error})
      Postgrex.Protocol.disconnect(error, state)
    end

    def handle_execute(
          %Postgrex.TextQuery{statement: "ROLLBACK"},
          [],
          opts,
          %{fail_rollback: true} = state
        ) do
      {status, _} = Postgrex.Protocol.handle_status(opts, state)
      send(state.test_owner, {:rollback_attempted, status})
      {:error, DBConnection.ConnectionError.exception("injected rollback failure"), state}
    end

    def handle_execute(
          %Postgrex.TextQuery{statement: "BEGIN" <> _} = query,
          [],
          opts,
          %{fail_opening: true} = state
        ) do
      assert_sql = IO.iodata_to_binary(query.statement)
      send(state.test_owner, {:opening_sql, assert_sql})

      Postgrex.Protocol.handle_execute(
        %{query | statement: "BEGIN READ ONLY; SELECT 1 / 0"},
        [],
        opts,
        state
      )
    end

    def handle_execute(query, params, opts, state),
      do: Postgrex.Protocol.handle_execute(query, params, opts, state)
  end

  setup do
    {:ok, c} = Adapter.connect(Keyword.put(ConnectionOptions.options(), :pool_size, 1))
    Process.unlink(c)
    on_exit(fn -> if Process.alive?(c), do: GenServer.stop(c) end)
    %{c: c}
  end

  defp status(c), do: DBConnection.run(c, &DBConnection.status/1)

  test "canonical UNICODE succeeds and LATIN1 returns exactly the R65 refusal", %{c: c} do
    Postgrex.query!(c, "SET client_encoding = 'UNICODE'", [])

    assert {:ok, %{rows: [["café"]]}} =
             Adapter.execute(c, "SELECT $1::text", ["café"], selecto_read: true)

    assert status(c) == :idle

    Postgrex.query!(c, "SET client_encoding = 'LATIN1'", [])
    assert {:error, error} = Adapter.execute(c, "SELECT $1::text", ["café"], selecto_read: true)

    assert error == %Selecto.Error{
             type: :query_enforcement,
             message:
               "PostgreSQL the session's client_encoding is not UTF8: parameters would be misread",
             details: %{
               code: :query_rule_not_evaluable,
               reason: :client_encoding,
               adapter: :postgresql
             }
           }

    assert status(c) == :idle
  end

  test "failure in the opening SELECT after BEGIN rolls back" do
    opts = Keyword.merge(ConnectionOptions.options(), test_owner: self(), fail_opening: true)
    {:ok, c} = DBConnection.start_link(RollbackFailureProtocol, opts)
    Process.unlink(c)
    on_exit(fn -> if Process.alive?(c), do: GenServer.stop(c) end)

    assert {:error, %Postgrex.Error{postgres: %{code: :division_by_zero}}} =
             Adapter.execute(c, "SELECT 1", [], selecto_read: true, timeout: 1000)

    assert_receive {:opening_sql, sql}
    assert sql =~ "BEGIN READ ONLY; SELECT pg_catalog.set_config"

    assert status(c) == :idle
    assert Postgrex.query!(c, "SHOW statement_timeout", []).rows == [["0"]]
  end

  test "a failed business SELECT rolls back and the next read succeeds", %{c: c} do
    assert {:error, %Postgrex.Error{postgres: %{code: :division_by_zero}}} =
             Adapter.execute(c, "SELECT 1 / 0", [], selecto_read: true)

    assert status(c) == :idle
    assert {:ok, %{rows: [[1]]}} = Adapter.execute(c, "SELECT 1", [], selecto_read: true)
    assert status(c) == :idle
  end

  test "read-only and deadline apply locally; disabling read-only applies to generated reads", %{
    c: c
  } do
    assert {:ok, %{rows: [["on", timeout]]}} =
             Adapter.execute(
               c,
               "SELECT current_setting('transaction_read_only'), current_setting('statement_timeout')",
               [],
               selecto_read: true,
               timeout: 1000
             )

    assert timeout =~ ~r/^(\d+ms|1s)$/
    assert status(c) == :idle
    assert Postgrex.query!(c, "SHOW statement_timeout", []).rows == [["0"]]

    previous = Application.get_env(:selecto_db_postgresql, :read_only_reads)
    Application.put_env(:selecto_db_postgresql, :read_only_reads, false)

    on_exit(fn ->
      if is_nil(previous),
        do: Application.delete_env(:selecto_db_postgresql, :read_only_reads),
        else: Application.put_env(:selecto_db_postgresql, :read_only_reads, previous)
    end)

    assert {:ok, %{rows: [["off"]]}} =
             Adapter.execute(c, "SELECT current_setting('transaction_read_only')", [],
               selecto_read: true,
               selecto_generated: true
             )

    assert status(c) == :idle
  end

  test "server timeout cancels the read and leaves an idle reusable session", %{c: c} do
    assert {:error, %Postgrex.Error{postgres: %{code: :query_canceled}}} =
             Adapter.execute(c, "SELECT pg_catalog.pg_sleep(1)", [],
               selecto_read: true,
               timeout: 50
             )

    assert status(c) == :idle
  end

  test "a marker raised by business SQL is not translated into an encoding refusal", %{c: c} do
    Postgrex.query!(
      c,
      "CREATE FUNCTION pg_temp.raise_marker() RETURNS integer LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'selecto_guard_client_encoding_not_utf8'; END $$",
      []
    )

    assert {:error, %Postgrex.Error{postgres: %{pg_code: "P0001"}}} =
             Adapter.execute(c, "SELECT pg_temp.raise_marker()", [], selecto_read: true)

    assert status(c) == :idle
  end

  test "rollback failure disconnects an open or aborted session, retaining the first error" do
    for {sql, expected_status} <- [{"SELECT 1", :transaction}, {"SELECT 1 / 0", :error}] do
      opts =
        Keyword.merge(ConnectionOptions.options(),
          test_owner: self(),
          fail_rollback: true,
          backoff_type: :stop
        )

      {:ok, c} = DBConnection.start_link(RollbackFailureProtocol, opts)
      Process.unlink(c)

      DBConnection.run(c, fn conn ->
        assert DBConnection.status(conn) == :idle
        result = Adapter.execute(conn, sql, [], selecto_read: true)

        if expected_status == :error do
          assert {:error, %Postgrex.Error{postgres: %{code: :division_by_zero}}} = result
        else
          assert {:error, %DBConnection.ConnectionError{}} = result
        end

        assert_receive {:rollback_attempted, ^expected_status}
        assert DBConnection.status(conn) == :error
      end)

      assert_receive {:disconnected,
                      %DBConnection.ConnectionError{message: "Selecto could not undo a read"}},
                     1000

      if Process.alive?(c), do: GenServer.stop(c)
    end
  end
end
