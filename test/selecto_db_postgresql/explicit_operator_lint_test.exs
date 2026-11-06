defmodule SelectoDBPostgreSQL.ExplicitOperatorLintTest do
  # R46 FINAL-2 / R47: the SQL Selecto executes on PostgreSQL names every
  # comparison's operator. Over a corpus of executed-form statements (reads
  # prepared with their catalog operator types, writes compiled for
  # execution) no bare comparison operator, IN, BETWEEN or LIKE remains
  # outside OPERATOR(...) (assignments excepted).
  use ExUnit.Case, async: false

  alias Selecto.Write.Command
  alias SelectoDBPostgreSQL.{Adapter, WriteCompiler}

  @moduletag :postgres

  setup do
    {:ok, c} =
      Adapter.connect(
        Keyword.put(SelectoDBPostgreSQL.Verification.ConnectionOptions.options(), :pool_size, 1)
      )

    Process.unlink(c)
    schema = "selecto_lint_#{System.unique_integer([:positive])}"

    for sql <- [
          "CREATE SCHEMA #{schema}",
          "CREATE DOMAIN #{schema}.org AS varchar(16)",
          "CREATE TABLE #{schema}.sites (id int PRIMARY KEY, org_key #{schema}.org NOT NULL, name text)",
          "CREATE TABLE #{schema}.items (id int PRIMARY KEY, org_key #{schema}.org NOT NULL, " <>
            "site_id int, note varchar(20), ts timestamptz, n numeric(10,2))"
        ],
        do: {:ok, _} = Adapter.execute(c, sql, [], [])

    on_exit(fn ->
      if Process.alive?(c) do
        Adapter.execute(c, "DROP SCHEMA #{schema} CASCADE", [], [])
        GenServer.stop(c)
      end
    end)

    %{connection: c, schema: schema}
  end

  defp domain(schema) do
    %{
      name: "items",
      source: %{
        source_table: "items",
        source_schema: schema,
        primary_key: :id,
        tenant_field: :org_key,
        fields: [:id, :org_key, :site_id, :note, :ts, :n],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          org_key: %{type: :string},
          site_id: %{type: :integer},
          note: %{type: :string},
          ts: %{type: :utc_datetime},
          n: %{type: :decimal}
        },
        associations: %{
          site: %{
            queryable: :sites,
            field: :site,
            owner_key: :site_id,
            related_key: :id,
            cardinality: :one
          }
        }
      },
      schemas: %{
        sites: %{
          source_table: "sites",
          source_schema: schema,
          primary_key: :id,
          tenant_field: :org_key,
          fields: [:id, :org_key, :name],
          redact_fields: [],
          columns: %{id: %{type: :integer}, org_key: %{type: :string}, name: %{type: :string}},
          associations: %{}
        }
      },
      joins: %{site: %{type: :left, name: "site"}},
      required_filters: [{"n", {:gte, "0.00"}}]
    }
  end

  @filters [
    [{"note", "x"}],
    [{"note", {:!=, "x"}}],
    [{"note", {:gt, "a"}}, {"note", {:lte, "z"}}],
    [{"id", {:in, [1, 2]}}],
    [{"id", {:not_in, [3]}}],
    [{"id", [4, 5]}],
    [{"id", {:between, 1, 9}}],
    [{"ts", {:between, "2024-01-01 00:00:00", "2024-02-01 00:00:00"}}],
    [{"note", {:like, "a%"}}],
    [{"note", {:starts_with, "a"}}],
    [{"note", {:case_insensitive_like, "a%"}}],
    [{"site.name", "s"}],
    [{"site_id", {:ref, "id"}}]
  ]

  defp read_sql(c, schema, filters) do
    selecto =
      domain(schema)
      |> Selecto.configure(c, adapter: Adapter, validate: false, rollup_sort_fix: false)
      |> Selecto.with_tenant("t1")
      |> Selecto.select(["id", "site.name"])
      |> Selecto.filter(filters)

    {:ok, prepared} = Selecto.Executor.prepare_read(selecto)
    {sql, _params} = Selecto.to_sql(prepared)
    sql
  end

  defp write_sqls(schema) do
    scope = {:eq, {:field, "org_key"}, {:literal, "t1"}}

    commands = [
      %{
        operation: :update,
        relation: "#{schema}.items",
        predicate: {:and, [{:eq, {:field, "id"}, {:literal, 1}}, scope]},
        assignments: [%{field: "note", value: {:literal, "y"}}],
        expected_cardinality: :many
      },
      %{
        operation: :delete,
        relation: "#{schema}.items",
        predicate: {:and, [{:in, {:field, "id"}, [{:literal, 1}, {:literal, 2}]}, scope]},
        expected_cardinality: :many
      },
      %{
        operation: :update,
        relation: "#{schema}.items",
        predicate:
          {:and,
           [{:gte, {:field, "id"}, {:literal, 1}}, {:neq, {:field, "note"}, {:literal, "q"}}]},
        assignments: [%{field: "site_id", value: {:literal, 2}}],
        expected_cardinality: :many,
        metadata: %{
          foreign_key_guards: [
            %{
              field: :site_id,
              relation: "#{schema}.sites",
              target_field: :id,
              tenant_field: :org_key,
              tenant_value: "t1"
            }
          ]
        }
      }
    ]

    for attrs <- commands do
      {:ok, command} = Command.new(attrs)
      {:ok, statement} = WriteCompiler.compile(command, explicit_operators: true)
      statement.text
    end
  end

  # Bare comparisons left after removing literals, OPERATOR(...) groups and
  # UPDATE ... SET assignments.
  defp bare_comparisons(sql) do
    sql
    |> String.replace(~r/'(?:[^']|'')*'/, "''")
    |> String.replace(~r/OPERATOR\([^)]*\)/, " OP ")
    |> String.replace(~r/\bSET\b.*?(?=\bWHERE\b|\bFROM\b|\bRETURNING\b|$)/is, " SET ")
    |> then(fn text ->
      Regex.scan(
        ~r/(?<![<>!=:|])(?:<>|!=|<=|>=|=|<|>)(?![=>])|\b(?:IN|BETWEEN|LIKE|ILIKE)\b/i,
        text
      )
    end)
    |> List.flatten()
  end

  test "executed-form reads name every comparison operator", %{connection: c, schema: schema} do
    for filters <- @filters do
      sql = read_sql(c, schema, filters)
      assert sql =~ "OPERATOR(pg_catalog.", sql
      assert bare_comparisons(sql) == [], "#{inspect(filters)}: #{sql}"
    end
  end

  test "executed-form writes name every comparison operator", %{schema: schema} do
    # A column the compiler has no operator type for stays plain until the
    # adapter's last step; no extension type here, so that step needs no
    # column facts.
    probe = fn _sql, _params -> {:ok, %{rows: [[false, "read committed", []]]}} end

    for sql <- write_sqls(schema) do
      assert {:ok, prepared} = SelectoDBPostgreSQL.ExplicitSQL.prepare(probe, sql)
      assert bare_comparisons(prepared) == [], prepared
    end
  end
end
