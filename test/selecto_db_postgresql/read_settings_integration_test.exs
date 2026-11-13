defmodule SelectoDBPostgreSQL.ReadSettingsIntegrationTest do
  # The read_only_reads setting refuses at connect, not as an opaque failure
  # of the first read, and a read-only refusal on a stream carries its message, not the driver error.
  use ExUnit.Case, async: false

  alias SelectoDBPostgreSQL.Adapter

  @moduletag :postgres

  defp options, do: SelectoDBPostgreSQL.Verification.ConnectionOptions.options()

  test "a read_only_reads that is no boolean refuses at connect" do
    previous = Application.get_env(:selecto_db_postgresql, :read_only_reads, :unset)
    Application.put_env(:selecto_db_postgresql, :read_only_reads, "yes")

    on_exit(fn ->
      if previous == :unset,
        do: Application.delete_env(:selecto_db_postgresql, :read_only_reads),
        else: Application.put_env(:selecto_db_postgresql, :read_only_reads, previous)
    end)

    assert {:error, {:invalid_configuration, :read_only_reads}} = Adapter.connect(options())
  end

  test "a statement_cache left in the configuration warns: it is ignored" do
    Application.put_env(:selecto_db_postgresql, :statement_cache, 1024)
    on_exit(fn -> Application.delete_env(:selecto_db_postgresql, :statement_cache) end)

    log =
      ExUnit.CaptureLog.capture_log(fn ->
        {:ok, c} = Adapter.connect(Keyword.put(options(), :pool_size, 1))
        GenServer.stop(c)
      end)

    assert log =~ ":statement_cache is ignored"
  end

  test "a read's timeout stands: a failed undo does not replace it (R66)" do
    {:ok, c} = Adapter.connect(Keyword.put(options(), :pool_size, 1))
    Process.unlink(c)

    result =
      Adapter.execute(c, "SELECT pg_catalog.pg_sleep(5)", [], selecto_read: true, timeout: 200)

    assert {:error, error} = result
    refute match?(%{details: %{reason: :connection_broken}}, error)
    if Process.alive?(c), do: GenServer.stop(c)
  end

  test "the simple opening preserves read-only mode, the shorter host timeout and UTF8" do
    {:ok, c} = Adapter.connect(Keyword.put(options(), :pool_size, 1))
    Process.unlink(c)
    on_exit(fn -> if Process.alive?(c), do: GenServer.stop(c) end)
    Postgrex.query!(c, "SET statement_timeout = '400ms'", [])

    assert {:ok, %{rows: [["on", "400ms", "café"]]}} =
             Adapter.execute(
               c,
               "SELECT current_setting('transaction_read_only'), " <>
                 "current_setting('statement_timeout'), $1::text",
               ["café"],
               selecto_read: true,
               timeout: 5_000
             )

    assert [["off", "400ms"]] =
             Postgrex.query!(
               c,
               "SELECT current_setting('transaction_read_only'), current_setting('statement_timeout')",
               []
             ).rows

    Postgrex.query!(c, "SET client_encoding = 'LATIN1'", [])

    assert {:error, %{details: %{reason: :client_encoding}}} =
             Adapter.execute(c, "SELECT $1::text", ["café"], selecto_read: true, timeout: 5_000)

    Postgrex.query!(c, "SET client_encoding = 'UTF8'", [])
    assert Postgrex.query!(c, "SELECT 1", []).rows == [[1]]
  end

  test "a string cast to a number binds as text, for the server to convert" do
    fragment = %Selecto.Dialect.ComputedValue{
      operation: :cast,
      expression: {:param, "1"},
      type: "integer"
    }

    assert {:ok, sql} = SelectoDBPostgreSQL.Dialect.render_computed_value(fragment, nil)
    assert sql == ["CAST(CAST(", {:param, "1"}, " AS TEXT) AS ", "BIGINT", ")"]

    number = %{fragment | expression: {:param, 1}}

    assert {:ok, ["CAST(", {:param, 1}, " AS ", "BIGINT", ")"]} =
             SelectoDBPostgreSQL.Dialect.render_computed_value(number, nil)
  end

  test "a stream that writes refuses with its message, not a wrapped driver error" do
    {:ok, c} = Adapter.connect(Keyword.put(options(), :pool_size, 1))
    Process.unlink(c)
    sequence = "selecto_ro_#{System.unique_integer([:positive])}"
    {:ok, _} = Adapter.execute(c, "CREATE SEQUENCE public.#{sequence}", [], [])

    on_exit(fn ->
      {:ok, d} = Adapter.connect(Keyword.put(options(), :pool_size, 1))
      Adapter.execute(d, "DROP SEQUENCE IF EXISTS public.#{sequence}", [], [])
      GenServer.stop(d)
    end)

    {:ok, stream} =
      Adapter.stream(c, "SELECT pg_catalog.nextval('public.#{sequence}')", [], selecto_read: true)

    # Selecto.Error.to_exception/1's form: the refusal's own message, with no
    # driver error dumped into it.
    assert_raise(Selecto.Error.Refused, "PostgreSQL refuses a read that tried to write", fn ->
      Enum.to_list(stream)
    end)
  end

  describe "a stream opens lazily, its refusals come at the call (M6)" do
    setup do
      {:ok, c} = Adapter.connect(Keyword.put(options(), :pool_size, 1))
      Process.unlink(c)
      on_exit(fn -> if Process.alive?(c), do: GenServer.stop(c) end)
      %{c: c}
    end

    test "an opened stream holds nothing, and any process can enumerate it", %{c: c} do
      {:ok, stream} =
        Adapter.stream(c, "SELECT g FROM pg_catalog.generate_series(1, 3) g", [],
          selecto_read: true
        )

      # A pool of 1 is free, and no message reaches the opener.
      assert {:ok, %{rows: [[1]]}} = Adapter.execute(c, "SELECT 1", [], selecto_read: true)
      assert {:messages, []} = Process.info(self(), :messages)

      rows = Task.await(Task.async(fn -> Enum.map(stream, fn {[g], _} -> g end) end))
      assert rows == [1, 2, 3]
    end

    test "opened idle and enumerated after a host BEGIN: typed, and the host goes on", %{c: c} do
      {:ok, stream} = Adapter.stream(c, "SELECT 1", [], selecto_read: true)
      {:ok, _} = Adapter.execute(c, "BEGIN", [], [])

      error = assert_raise Selecto.Error.Refused, fn -> Enum.to_list(stream) end
      assert error.details.reason == :stream_in_transaction
      assert {:ok, %{rows: [[1]]}} = Adapter.execute(c, "SELECT 1", [], [])
      {:ok, _} = Adapter.execute(c, "ROLLBACK", [], [])
    end

    test "a refusal is the call's, typed", %{c: c} do
      assert {:error, %{details: %{reason: :hand_built_not_a_read}}} =
               Adapter.stream(c, "COMMIT", [], selecto_read: true)

      {:ok, _} = Adapter.execute(c, "SET client_encoding = 'LATIN1'", [], [])

      assert {:error, %{details: %{reason: :client_encoding}}} =
               Adapter.stream(c, "SELECT 1", [], selecto_read: true)

      {:ok, _} = Adapter.execute(c, "SET client_encoding = 'UTF8'", [], [])

      # Through the executor too: the reason survives.
      domain = %{
        name: "D",
        source: %{
          source_table: "pg_am",
          source_schema: "pg_catalog",
          primary_key: :oid,
          fields: [:amname],
          redact_fields: [],
          columns: %{amname: %{type: :string}},
          associations: %{}
        },
        schemas: %{},
        joins: %{}
      }

      assert {:ok, _} =
               Postgrex.transaction(c, fn t ->
                 assert {:error, %Selecto.Error{details: %{reason: :stream_in_transaction}}} =
                          domain
                          |> Selecto.configure(t, adapter: Adapter, validate: false)
                          |> Selecto.select(["amname"])
                          |> Selecto.execute_stream()
               end)

      assert {:ok, _} =
               Postgrex.transaction(c, fn t ->
                 assert {:error, %Selecto.Error{details: %{reason: :stream_in_transaction}}} =
                          Adapter.stream(t, "SELECT 1", [], selecto_read: true)
               end)
    end
  end
end
