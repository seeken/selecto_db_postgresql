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
    assert_raise(RuntimeError, "PostgreSQL refuses a read that tried to write", fn ->
      Enum.to_list(stream)
    end)
  end
end
