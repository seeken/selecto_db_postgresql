defmodule SelectoDBPostgreSQL.ExplicitOperatorIntegrationTest do
  # Selecto's own catalog SQL, and citext keys under governed writes.
  use ExUnit.Case, async: false

  alias Selecto.Write.Command
  alias SelectoDBPostgreSQL.Adapter

  @moduletag :postgres

  defp options, do: SelectoDBPostgreSQL.Verification.ConnectionOptions.options()

  defp exec!(c, sql), do: {:ok, _} = Adapter.execute(c, sql, [], [])

  # R48: Selecto's own guard SQL names its operators with exact-signature
  # casts: an always-zero &(smallint, integer) in the reset path cannot hide
  # a statement-level trigger from the bound-scope write check.
  test "a reset-path operator cannot blind the statement-trigger check" do
    schema = "selecto_r48_#{System.unique_integer([:positive])}"
    {:ok, admin} = Adapter.connect(Keyword.put(options(), :pool_size, 1))

    for sql <- [
          "CREATE SCHEMA #{schema}",
          "CREATE TABLE #{schema}.w48 (id int PRIMARY KEY, org_key text NOT NULL, note text)",
          "INSERT INTO #{schema}.w48 VALUES (1, 't1', 'in'), (2, 't2', 'SECRET')",
          "CREATE FUNCTION #{schema}.w48_stmt() RETURNS trigger LANGUAGE plpgsql AS " <>
            "$$ BEGIN UPDATE #{schema}.w48 SET note = 'PWNED' WHERE id = 2; RETURN NULL; END $$",
          "CREATE TRIGGER w48_tr AFTER UPDATE ON #{schema}.w48 FOR EACH STATEMENT " <>
            "WHEN (pg_trigger_depth() < 1) EXECUTE FUNCTION #{schema}.w48_stmt()",
          "CREATE FUNCTION #{schema}.zero(smallint, integer) RETURNS integer LANGUAGE sql " <>
            "IMMUTABLE AS 'SELECT 0'",
          "CREATE OPERATOR #{schema}.& (LEFTARG = smallint, RIGHTARG = integer, FUNCTION = #{schema}.zero)"
        ],
        do: exec!(admin, sql)

    {:ok, c} =
      Adapter.connect(
        options()
        |> Keyword.put(:pool_size, 1)
        |> Keyword.put(:parameters, search_path: schema)
      )

    try do
      scope = {:eq, {:field, "org_key"}, {:literal, "t1"}}

      {:ok, command} =
        Command.new(%{
          operation: :update,
          relation: "w48",
          predicate: {:and, [{:eq, {:field, "id"}, {:literal, 1}}, scope]},
          assignments: [%{field: "note", value: {:literal, "own"}}],
          expected_cardinality: :many,
          metadata: %{post_image: %{predicate: scope, key: ["id"]}}
        })

      assert {:error, %{details: %{code: :query_rule_not_evaluable}}} =
               Adapter.execute_write_unsafe(c, command, [])

      assert {:ok, %{rows: [["SECRET"]]}} =
               Adapter.execute(c, "SELECT note FROM w48 WHERE id = 2", [], [])
    after
      GenServer.stop(c)
      exec!(admin, "DROP SCHEMA #{schema} CASCADE")
      GenServer.stop(admin)
    end
  end

  # R48: a citext join key compares with citext's operator when both sides
  # are citext; a citext column against a text column follows native
  # resolution (text), whatever the operand order.
  test "citext keys keep citext semantics; mixed citext and text compare as native" do
    prefix = System.get_env("SELECTO_TEST_DATABASE_PREFIX", "selecto")
    database = "#{prefix}_cj_#{System.unique_integer([:positive])}"
    {:ok, admin} = Adapter.connect(Keyword.put(options(), :pool_size, 1))
    exec!(admin, ~s|CREATE DATABASE "#{database}"|)

    try do
      {:ok, c} =
        Adapter.connect(
          options()
          |> Keyword.put(:pool_size, 1)
          |> Keyword.put(:database, database)
        )

      for sql <- [
            "CREATE EXTENSION citext",
            "CREATE TABLE cj_par (id int PRIMARY KEY, org_key text, code citext, tt text)",
            "INSERT INTO cj_par VALUES (1, 't1', 'Abc', 'abc'), (2, 't1', 'xyz', 'y'), (3, 't2', 'ABC', 'q')",
            "CREATE TABLE cj_kid (id int PRIMARY KEY, org_key text, par_code citext, label text)",
            "INSERT INTO cj_kid VALUES (10, 't1', 'ABC', 'k-upper'), (11, 't1', 'abc', 'k-lower'), (12, 't1', 'XYZ', 'k-xyz')"
          ],
          do: exec!(c, sql)

      GenServer.stop(c)

      {:ok, c} =
        Adapter.connect(
          options()
          |> Keyword.put(:pool_size, 1)
          |> Keyword.put(:database, database)
        )

      rel = fn table, columns, assoc ->
        %{
          source_table: table,
          primary_key: :id,
          tenant_field: :org_key,
          fields: Map.keys(columns),
          redact_fields: [],
          columns: columns,
          associations: assoc
        }
      end

      domain = %{
        name: "K",
        source:
          rel.(
            "cj_kid",
            %{
              id: %{type: :integer},
              org_key: %{type: :string},
              par_code: %{type: :string},
              label: %{type: :string}
            },
            %{
              par: %{
                queryable: :par,
                field: :par,
                owner_key: :par_code,
                related_key: :code,
                cardinality: :one
              }
            }
          ),
        schemas: %{
          par:
            rel.(
              "cj_par",
              %{
                id: %{type: :integer},
                org_key: %{type: :string},
                code: %{type: :string},
                tt: %{type: :string}
              },
              %{}
            )
        },
        joins: %{par: %{type: :left, name: "par"}}
      }

      joined =
        domain
        |> Selecto.configure(c, adapter: Adapter, validate: false, rollup_sort_fix: false)
        |> Selecto.with_tenant("t1")
        |> Selecto.select(["id", "par.id"])
        |> Selecto.order_by(["id"])
        |> Selecto.execute()

      # Native citext join: ABC and abc meet Abc (1), XYZ meets xyz (2).
      assert {:ok, {[[10, 1], [11, 1], [12, 2]], _, _}} = joined

      par =
        %{domain | source: domain.schemas.par, schemas: %{}, joins: %{}}
        |> Map.update!(:source, &Map.put(&1, :associations, %{}))

      for {left, right} <- [{"code", "tt"}, {"tt", "code"}] do
        result =
          par
          |> Selecto.configure(c, adapter: Adapter, validate: false, rollup_sort_fix: false)
          |> Selecto.with_tenant("t1")
          |> Selecto.select(["id"])
          |> Selecto.filter({left, {:ref, right}})
          |> Selecto.order_by(["id"])
          |> Selecto.execute()

        # citext = text resolves as text = text: 'Abc' <> 'abc'.
        assert {:ok, {[], _, _}} = result, "#{left} = #{right}"
      end

      GenServer.stop(c)
    after
      exec!(admin, ~s|DROP DATABASE IF EXISTS "#{database}" WITH (FORCE)|)
      GenServer.stop(admin)
    end
  end
end
