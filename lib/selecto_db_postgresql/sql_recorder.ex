defmodule SelectoDBPostgreSQL.SQLRecorder do
  @moduledoc false

  # Test support (R47): with `config :selecto_db_postgresql, record_sql: true`
  # every statement the adapter generates (session guards, relation and
  # type lookups, reads, streams, writes, re-reads) is recorded, so a suite
  # can lint all of them. Host SQL passed through `execute/4` without
  # `selecto_read` is not recorded. Off by default; recording is a no-op.

  @table :selecto_db_postgresql_sql

  @doc "Creates the recording table, owned by the calling (long-lived) process."
  def start do
    Application.put_env(:selecto_db_postgresql, :record_sql, true)
    ensure_table()
    :ok
  end

  def enabled?, do: Application.get_env(:selecto_db_postgresql, :record_sql) == true

  def record(sql, kind \\ :statement) do
    if enabled?() do
      ensure_table()
      :ets.insert(@table, {System.unique_integer([:monotonic]), kind, IO.iodata_to_binary(sql)})
    end

    sql
  end

  def statements do
    if :ets.whereis(@table) == :undefined,
      do: [],
      else:
        @table
        |> :ets.tab2list()
        |> Enum.sort()
        |> Enum.map(fn {_order, kind, sql} -> {kind, sql} end)
        |> Enum.uniq()
  end

  defp ensure_table do
    if :ets.whereis(@table) == :undefined do
      try do
        :ets.new(@table, [:named_table, :public, :ordered_set])
      rescue
        ArgumentError -> :ok
      end
    end
  end
end
