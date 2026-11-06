defmodule SelectoDBPostgreSQL.ExactInstantDialectTest do
  use ExUnit.Case, async: true
  alias SelectoDBPostgreSQL.{Adapter, Dialect}

  defp fragment(type \\ :utc_datetime) do
    %{
      __struct__: Selecto.Dialect.Instant.Comparison,
      operation: :lt,
      expression: ~s("items"."instant"),
      value: {:param, "-0.000000001"},
      source_type: type,
      storage: nil
    }
  end

  test "timestamp precision remains numeric and the nanosecond operand stays independently bound" do
    assert {:ok, rendered} = Dialect.render_instant_comparison(fragment(), %{})
    {sql, params} = Selecto.SQL.Params.finalize(rendered, adapter: Adapter)
    assert sql =~ "'J'" and sql =~ "'SSSS.US'" and sql =~ "AT TIME ZONE 'UTC'"
    assert sql =~ "< CAST(CAST($1 AS TEXT) AS NUMERIC)"
    refute sql =~ "TO_TIMESTAMP"
    refute sql =~ "-0.000000001"
    assert params == ["-0.000000001"]
    assert {:ok, epoch} = Dialect.render_instant_comparison(fragment(:epoch_datetime), %{})

    assert {~s|(CAST("items"."instant" AS NUMERIC) < CAST(CAST($1 AS TEXT) AS NUMERIC))|,
            ["-0.000000001"]} ==
             Selecto.SQL.Params.finalize(epoch, adapter: Adapter)
  end

  test "declared UTC physical timestamp storage avoids session-zone interpretation" do
    assert {:ok, rendered} =
             Dialect.render_instant_comparison(%{fragment() | storage: :naive_utc}, %{})

    {sql, _} = Selecto.SQL.Params.finalize(rendered, adapter: Adapter)
    refute sql =~ "AT TIME ZONE"
  end

  test "invalid fragment shapes, source kinds, operations and operands are refused" do
    for bad <- [
          Map.put(fragment(), :sql, "raw"),
          Map.delete(fragment(), :__struct__),
          %{fragment() | operation: "=; SELECT 1"},
          %{fragment() | source_type: :integer},
          %{fragment() | value: {:param, "0.0000000001"}},
          %{fragment() | value: {:param, "NaN"}},
          %{fragment() | expression: {:raw, "sql"}},
          %{fragment() | storage: :local}
        ] do
      assert {:error, %Selecto.Error{details: %{unsupported_feature: :exact_instant_comparisons}}} =
               Dialect.render_instant_comparison(bad, %{})
    end
  end
end
