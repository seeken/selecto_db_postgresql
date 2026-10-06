defmodule SelectoDBPostgreSQL.ComputedValueDialectTest do
  use ExUnit.Case, async: true

  alias SelectoDBPostgreSQL.{Adapter, Dialect}

  defp fragment(operation, expression, options \\ []) do
    %{
      __struct__: Selecto.Dialect.ComputedValue,
      operation: operation,
      expression: expression,
      type: Keyword.get(options, :type),
      path: Keyword.get(options, :path, [])
    }
  end

  test "canonical cast targets and parameter markers keep their PostgreSQL behavior" do
    for {type, target} <- [
          {"string", "TEXT"},
          {"integer", "BIGINT"},
          {"decimal", "NUMERIC"},
          {"boolean", "BOOLEAN"},
          {"date", "DATE"},
          {"utc_datetime", "TIMESTAMPTZ"}
        ] do
      assert {:ok, rendered} =
               Dialect.render_computed_value(fragment(:cast, {:param, nil}, type: type), %{})

      assert Selecto.SQL.Params.finalize(rendered, adapter: Adapter) ==
               {"CAST($1 AS #{target})", [nil]}
    end
  end

  test "JSON text extraction preserves expression and ordered path bindings" do
    path = Enum.map(["items", "0", "sku"], &{:param, &1})
    expression = ["COALESCE(payload, ", {:param, "{}"}, ")"]

    assert {:ok, rendered} =
             Dialect.render_computed_value(fragment(:json_text, expression, path: path), %{})

    assert Selecto.SQL.Params.finalize(rendered, adapter: Adapter) ==
             {"JSONB_EXTRACT_PATH_TEXT(CAST(COALESCE(payload, $1) AS JSONB), $2, $3, $4)",
              ["{}", "items", "0", "sku"]}
  end

  test "a closed shape, canonical target and bound path are required" do
    valid = fragment(:cast, {:param, 1}, type: "integer")

    invalid = [
      Map.put(valid, :sql, "injected"),
      Map.delete(valid, :__struct__),
      %{valid | operation: :raw},
      %{valid | type: "BIGINT); SELECT 1"},
      %{valid | expression: {:raw, "sql"}},
      %{valid | expression: [[], ""]},
      fragment(:json_text, "payload"),
      fragment(:json_text, "payload", path: ["raw path"]),
      fragment(:json_text, "payload", path: [{:param, "a'b"}])
    ]

    for value <- invalid do
      assert {:error, %Selecto.Error{details: %{unsupported_feature: :computed_value}}} =
               Dialect.render_computed_value(value, %{})
    end
  end
end
