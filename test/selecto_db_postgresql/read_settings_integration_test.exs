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
