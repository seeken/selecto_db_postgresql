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
end
