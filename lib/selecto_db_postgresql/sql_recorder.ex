defmodule SelectoDBPostgreSQL.SQLRecorder do
  @moduledoc false

  # Test support (R47, R48): with `config :selecto_db_postgresql, record_sql:
  # true` every statement the adapter sends to the driver is recorded, at the
  # driver call itself, so nothing Selecto runs can escape the suite's lint.
  # The host's own SQL (`execute/4` without `selecto_read`) is never Selecto's
  # to rewrite (R48): its text is noted and left out, as are transaction
  # control statements. Off by default; recording is then a no-op.

  @table :selecto_db_postgresql_sql

  @control ~r/\A\s*(?:BEGIN|COMMIT|ROLLBACK|SAVEPOINT|RELEASE|SET\s|SHOW\s|LOCK\s)/i

  @doc "Creates the recording table, owned by the calling (long-lived) process."
  def start do
    Application.put_env(:selecto_db_postgresql, :record_sql, true)
    ensure_table()
    :ok
  end

  def enabled?, do: Application.get_env(:selecto_db_postgresql, :record_sql) == true

  @doc "Records a statement about to reach the driver; returns it."
  def driver(sql), do: insert(sql, :driver)

  @doc "Notes the host's own statement text; returns it."
  def host(sql), do: insert(sql, :host)

  defp insert(sql, kind) do
    if enabled?() do
      ensure_table()
      text = text(sql)
      :ets.insert(@table, {System.unique_integer([:monotonic]), kind, text})
    end

    sql
  end

  defp text(%{statement: statement}), do: IO.iodata_to_binary(statement)
  defp text(sql), do: IO.iodata_to_binary(sql)

  @doc "Selecto's own statements, in order, without duplicates."
  def statements do
    if :ets.whereis(@table) == :undefined do
      []
    else
      entries = @table |> :ets.tab2list() |> Enum.sort()
      host = for {_order, :host, sql} <- entries, into: MapSet.new(), do: sql

      entries
      |> Enum.flat_map(fn
        {_order, :driver, sql} -> [sql]
        _host -> []
      end)
      |> Enum.reject(&(MapSet.member?(host, &1) or Regex.match?(@control, &1)))
      |> Enum.uniq()
    end
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
