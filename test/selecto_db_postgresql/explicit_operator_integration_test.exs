defmodule SelectoDBPostgreSQL.ExplicitOperatorIntegrationTest do
  # R46 FINAL-2 (r46final regression): an always-true operator in a
  # reset-path schema that exactly matches a column's type (a domain over
  # text, int4, timestamptz or date; a user enum) never decides a generated
  # comparison: reads stay in scope and a tenant-t1 update of tenant t2's
  # row applies to nothing.
  use ExUnit.Case, async: false

  alias Selecto.Write.Command
  alias SelectoDBPostgreSQL.Adapter

  @moduletag :postgres

  @cases [
    {"domain over text", "CREATE DOMAIN %s.k AS text", "%s.k", "'t1'", "'t2'", "t1", :string,
     "="},
    {"domain over int4", "CREATE DOMAIN %s.k AS int4", "%s.k", "1", "2", 1, :integer, "="},
    {"user enum", "CREATE TYPE %s.k AS ENUM ('t1', 't2')", "%s.k", "'t1'", "'t2'", "t1", :string,
     "="},
    {"domain over timestamptz", "CREATE DOMAIN %s.k AS timestamptz", "%s.k",
     "'2024-01-02 00:00:00+00'", "'2023-01-02 00:00:00+00'", nil, :utc_datetime, ">="},
    {"domain over date", "CREATE DOMAIN %s.k AS date", "%s.k", "'2024-01-02'", "'2023-01-02'",
     nil, :date, ">="}
  ]

  defp options, do: SelectoDBPostgreSQL.Verification.ConnectionOptions.options()

  defp exec!(c, sql), do: {:ok, _} = Adapter.execute(c, sql, [], [])

  defp domain(declared, required) do
    %{
      name: "w46",
      source: %{
        source_table: "w46",
        primary_key: :id,
        tenant_field: if(required, do: nil, else: :org_key),
        fields: [:id, :org_key, :note],
        redact_fields: [],
        columns: %{id: %{type: :integer}, org_key: %{type: declared}, note: %{type: :string}},
        associations: %{}
      },
      schemas: %{},
      joins: %{},
      required_filters: required || []
    }
  end

  for {label, setup, operand, v1, v2, tenant, declared, op} <- @cases do
    test "an exact-type operator on a #{label} never decides a comparison" do
      schema = "selecto_r46_#{System.unique_integer([:positive])}"
      sub = &String.replace(&1, "%s", schema)
      {:ok, admin} = Adapter.connect(Keyword.put(options(), :pool_size, 1))

      for sql <- [
            "CREATE SCHEMA #{schema}",
            sub.(unquote(setup)),
            "CREATE TABLE #{schema}.w46 (id int PRIMARY KEY, org_key #{sub.(unquote(operand))} NOT NULL, note text)",
            "INSERT INTO #{schema}.w46 VALUES (1, #{unquote(v1)}, 'in'), (2, #{unquote(v2)}, 'SECRET')",
            "CREATE FUNCTION #{schema}.w_yes(#{sub.(unquote(operand))}, #{sub.(unquote(operand))}) " <>
              "RETURNS boolean LANGUAGE sql IMMUTABLE AS 'SELECT true'",
            "CREATE OPERATOR #{schema}.#{unquote(op)} (LEFTARG = #{sub.(unquote(operand))}, " <>
              "RIGHTARG = #{sub.(unquote(operand))}, FUNCTION = #{schema}.w_yes)"
          ],
          do: exec!(admin, sql)

      {:ok, c} =
        Adapter.connect(
          options()
          |> Keyword.put(:pool_size, 1)
          |> Keyword.put(:parameters, search_path: schema)
        )

      try do
        tenant = unquote(Macro.escape(tenant))

        {selecto, scope} =
          if tenant do
            {domain(unquote(declared), nil)
             |> Selecto.configure(c, adapter: Adapter, validate: false, rollup_sort_fix: false)
             |> Selecto.with_tenant(tenant), {:eq, {:field, "org_key"}, {:literal, tenant}}}
          else
            bound =
              if unquote(declared) == :date, do: ~D[2024-01-01], else: ~U[2024-01-01 00:00:00Z]

            {domain(unquote(declared), [{"org_key", {">=", bound}}])
             |> Selecto.configure(c, adapter: Adapter, validate: false, rollup_sort_fix: false),
             {:gte, {:field, "org_key"}, {:literal, bound}}}
          end

        assert {:ok, {[[1, "in"]], _, _}} =
                 selecto
                 |> Selecto.select(["id", "note"])
                 |> Selecto.order_by(["id"])
                 |> Selecto.execute()

        {:ok, command} =
          Command.new(%{
            operation: :update,
            relation: "w46",
            predicate: {:and, [{:eq, {:field, "id"}, {:literal, 2}}, scope]},
            assignments: [%{field: "note", value: {:literal, "PWNED"}}],
            expected_cardinality: :many,
            metadata: %{post_image: %{predicate: scope, key: ["id"]}}
          })

        assert {:ok, %{affected_rows: 0}} = Adapter.execute_write_unsafe(c, command, [])

        assert {:ok, %{rows: [["SECRET"]]}} =
                 Adapter.execute(c, "SELECT note FROM w46 WHERE id = 2", [], [])
      after
        GenServer.stop(c)
        exec!(admin, "DROP SCHEMA #{schema} CASCADE")
        GenServer.stop(admin)
      end
    end
  end

  # pg_r46_residual / pg_r46_retarget ports (R47): with an always-true
  # =(varchar, varchar) and a lower(varchar) returning 't1' in the reset-path
  # schema, a scope-keyed join, a required computed lower(...) predicate and
  # retarget membership (IN and EXISTS) stay exact.
  test "scope-keyed joins, computed functions and retarget membership never resolve by name" do
    schema = "selecto_r47_#{System.unique_integer([:positive])}"
    {:ok, admin} = Adapter.connect(Keyword.put(options(), :pool_size, 1))

    for sql <- [
          "CREATE SCHEMA #{schema}",
          "CREATE TABLE #{schema}.lr_acc (pk int PRIMARY KEY, id varchar(10), site varchar(10), secret varchar(30))",
          "INSERT INTO #{schema}.lr_acc VALUES (1, 'A', 's1', 'mine'), (2, 'A', 's2', 'OTHER-SITE-SECRET'), (3, 'B', 's1', 'OTHER-KEY')",
          "CREATE TABLE #{schema}.lr_root (id int PRIMARY KEY, org_key varchar(10) NOT NULL, site varchar(10), acc_id varchar(10))",
          "INSERT INTO #{schema}.lr_root VALUES (1, 't1', 's1', 'A'), (2, 'T1', 's2', 'A'), (3, 't2', 's2', 'B')",
          "CREATE FUNCTION #{schema}.ev_eq(varchar, varchar) RETURNS boolean LANGUAGE sql IMMUTABLE AS 'SELECT true'",
          "CREATE OPERATOR #{schema}.= (LEFTARG = varchar, RIGHTARG = varchar, FUNCTION = #{schema}.ev_eq)",
          "CREATE FUNCTION #{schema}.lower(varchar) RETURNS text LANGUAGE sql IMMUTABLE AS 'SELECT ''t1''::text'"
        ],
        do: exec!(admin, sql)

    {:ok, c} =
      Adapter.connect(
        options()
        |> Keyword.put(:pool_size, 1)
        |> Keyword.put(:parameters, search_path: schema)
      )

    rel = fn table, columns ->
      %{
        source_table: table,
        primary_key: if(table == "lr_acc", do: :pk, else: :id),
        fields: Map.keys(columns),
        redact_fields: [],
        columns: columns,
        associations: %{}
      }
    end

    domain = %{
      name: "L",
      source:
        rel.("lr_root", %{
          id: %{type: :integer},
          org_key: %{type: :string},
          site: %{type: :string},
          acc_id: %{type: :string},
          org_lc: %{
            type: :string,
            computed: %{kind: :expression, expression: ["lower", ["field", "org_key"]]}
          }
        })
        |> Map.put(:tenant_field, :org_key)
        |> Map.put(:associations, %{
          acc: %{
            queryable: :acc,
            field: :acc,
            owner_key: :acc_id,
            related_key: :id,
            cardinality: :one,
            source_scope_key: :site,
            target_scope_key: :site
          }
        }),
      schemas: %{
        acc:
          rel.("lr_acc", %{
            pk: %{type: :integer},
            id: %{type: :string},
            site: %{type: :string},
            secret: %{type: :string}
          })
      },
      joins: %{acc: %{type: :left, name: "acc"}}
    }

    try do
      # The evil objects win hand-written SQL.
      assert {:ok, %{rows: [["t1"]]}} =
               Adapter.execute(c, "SELECT lower(CAST('T2' AS varchar))", [], [])

      joined =
        domain
        |> Selecto.configure(c, adapter: Adapter, validate: false, rollup_sort_fix: false)
        |> Selecto.with_tenant("t1")
        |> Selecto.select(["id", "acc.secret"])
        |> Selecto.order_by(["id"])
        |> Selecto.execute()

      assert {:ok, {[[1, "mine"]], _, _}} = joined

      required =
        domain
        |> Map.put(:required_filters, [{"org_lc", "t1"}])
        |> Selecto.configure(c, adapter: Adapter, validate: false, rollup_sort_fix: false)
        |> Selecto.select(["id"])
        |> Selecto.order_by(["id"])
        |> Selecto.execute()

      # pg_catalog.lower: 't1' and 'T1' lower to 't1'; never every row.
      assert {:ok, {[[1], [2]], _, _}} = required

      for strategy <- [:in, :exists] do
        retargeted =
          domain
          |> Selecto.configure(c, adapter: Adapter, validate: false, rollup_sort_fix: false)
          |> Selecto.with_tenant("t1")
          |> Selecto.retarget("acc", strategy: strategy)
          |> Selecto.select(["secret"])
          |> Selecto.order_by(["secret"])
          |> Selecto.execute()

        assert {:ok, {[["mine"]], _, _}} = retargeted, inspect(strategy)
      end
    after
      GenServer.stop(c)
      exec!(admin, "DROP SCHEMA #{schema} CASCADE")
      GenServer.stop(admin)
    end
  end
end
