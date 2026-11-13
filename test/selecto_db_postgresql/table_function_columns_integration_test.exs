defmodule SelectoDBPostgreSQL.TableFunctionColumnsIntegrationTest do
  use ExUnit.Case, async: false

  alias SelectoDBPostgreSQL.Adapter
  alias SelectoDBPostgreSQL.Verification.ConnectionOptions

  @moduletag :postgres

  setup do
    {:ok, conn} = Adapter.connect(Keyword.put(ConnectionOptions.options(), :pool_size, 1))
    Process.unlink(conn)
    on_exit(fn -> if Process.alive?(conn), do: GenServer.stop(conn) end)

    Postgrex.query!(
      conn,
      "CREATE TEMP TABLE function_roots (id integer, tags text[], numbers integer[], lo integer, hi integer)",
      []
    )

    Postgrex.query!(
      conn,
      "INSERT INTO function_roots VALUES (1, ARRAY['a', 'b'], ARRAY[10, 20], 1, 2)",
      []
    )

    Postgrex.query!(
      conn,
      "CREATE FUNCTION pg_temp.atlas_pairs() RETURNS TABLE (key text, value integer) LANGUAGE SQL AS 'VALUES (''left'', 7)'",
      []
    )

    domain = %{
      name: "Function roots",
      source: %{
        source_table: "function_roots",
        source_schema: "pg_temp",
        primary_key: :id,
        fields: [:id, :tags, :numbers, :lo, :hi],
        redact_fields: [],
        associations: %{},
        columns: %{
          id: %{type: :integer},
          tags: %{type: {:array, :string}},
          numbers: %{type: {:array, :integer}},
          lo: %{type: :integer},
          hi: %{type: :integer}
        }
      },
      schemas: %{},
      joins: %{}
    }

    %{conn: conn, domain: domain, base: Selecto.configure(domain, conn, adapter: Adapter)}
  end

  for {function, argument, expected} <- [
        {:jsonb_each, ~s|{"left":7}|, [["left", 7]]},
        {:json_each, ~s|{"left":7}|, [["left", 7]]},
        {:jsonb_each_text, ~s|{"left":7}|, [["left", "7"]]},
        {:json_each_text, ~s|{"left":7}|, [["left", "7"]]}
      ] do
    test "#{function} keeps both key and value columns", %{conn: conn, base: base} do
      assert_function(
        conn,
        base,
        unquote(function),
        [unquote(argument)],
        ["key", "value"],
        unquote(Macro.escape(expected))
      )
    end
  end

  for {function, argument, column, expected} <- [
        {:jsonb_array_elements, "[1,2]", "value", [[1], [2]]},
        {:jsonb_array_elements_text, "[1,2]", "value", [["1"], ["2"]]},
        {:json_array_elements, "[1,2]", "value", [[1], [2]]},
        {:json_array_elements_text, "[1,2]", "value", [["1"], ["2"]]},
        {:jsonb_object_keys, ~s|{"left":7}|, "f", [["left"]]},
        {:json_object_keys, ~s|{"left":7}|, "f", [["left"]]}
      ] do
    test "#{function} retains its native output name", %{conn: conn, base: base} do
      assert_function(
        conn,
        base,
        unquote(function),
        [unquote(argument)],
        [unquote(column)],
        unquote(Macro.escape(expected))
      )
    end
  end

  test "generate_series and regexp_matches retain their output names", %{conn: conn, base: base} do
    assert_function(
      conn,
      base,
      :generate_series,
      [{:ref, "selecto_root.lo"}, {:ref, "selecto_root.hi"}],
      ["f"],
      [[1], [2]]
    )

    assert_function(conn, base, :regexp_matches, ["ab", "(a)(b)"], ["f"], [
      [["a", "b"]]
    ])

    assert_function(conn, base, :regexp_split_to_table, ["a,b", ","], ["f"], [
      ["a"],
      ["b"]
    ])
  end

  test "arbitrary named composite functions keep their complete signature", %{
    conn: conn,
    base: base
  } do
    assert_function(conn, base, :"pg_temp.atlas_pairs", [], ["key", "value"], [["left", 7]])
  end

  test "registered table UDFs preserve their declared return columns", %{
    conn: conn,
    domain: domain
  } do
    functions = %{
      "pairs" => %{
        kind: :table,
        sql_name: "pg_temp.atlas_pairs",
        args: [],
        returns: %{columns: %{key: %{type: :string}, value: %{type: :integer}}},
        allowed_in: [:lateral]
      }
    }

    query =
      domain
      |> Map.put(:functions, functions)
      |> Selecto.configure(conn, adapter: Adapter)
      |> Selecto.lateral_join(:cross, Selecto.udf_table("pairs", []), "f")
      |> Selecto.select(["id", "f.key", "f.value"])

    {sql, params} = Selecto.to_sql(query)
    statement = Postgrex.prepare!(conn, "", sql)
    assert %{rows: [[1, "left", 7]]} = Postgrex.execute!(conn, statement, params)
  end

  test "generic multi-array unnest keeps both native columns", %{conn: conn, base: base} do
    query =
      base
      |> Selecto.lateral_join(
        :cross,
        {:function, :unnest, [{:ref, "selecto_root.tags"}, {:ref, "selecto_root.numbers"}]},
        "f"
      )
      |> Selecto.select([{:raw_sql, "f.*"}])

    {sql, params} = Selecto.to_sql(query)
    refute sql =~ "AS f(value)"
    statement = Postgrex.prepare!(conn, "", sql)

    assert %{columns: ["unnest", "unnest"], rows: [["a", 10], ["b", 20]]} =
             Postgrex.execute!(conn, statement, params)
  end

  defp assert_function(conn, base, function, arguments, columns, expected) do
    arguments =
      if String.starts_with?(Atom.to_string(function), "json"),
        do: Enum.map(arguments, &Jason.decode!/1),
        else: arguments

    query =
      base
      |> Selecto.lateral_join(:cross, {:function, function, arguments}, "f")
      |> Selecto.select(Enum.map(columns, &{:raw_sql, "f." <> &1}))

    {sql, params} = Selecto.to_sql(query)
    refute sql =~ "AS f(value)"
    statement = Postgrex.prepare!(conn, "", sql)
    result = Postgrex.execute!(conn, statement, params)
    assert result.columns == columns
    assert result.rows == expected
  end
end
