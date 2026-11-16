defmodule SelectoDBPostgreSQL.PostImageIntegrationTest do
  # Scope is checked in RETURNING after coercion, defaults, generated values
  # and BEFORE triggers. Host AFTER/deferred triggers, rules and owner DDL
  # are outside the governed-write boundary. No stored row is re-read.
  use ExUnit.Case, async: false

  alias Selecto.Write.{Command, Error}
  alias SelectoDBPostgreSQL.Adapter

  @moduletag :postgres

  setup do
    schema = "selecto_post_image_#{System.unique_integer([:positive])}"
    options = SelectoDBPostgreSQL.Verification.ConnectionOptions.options()
    {:ok, setup} = Adapter.connect(options)
    exec!(setup, "CREATE SCHEMA #{schema}")
    GenServer.stop(setup)

    # The schema is the connection's startup search_path (R29).
    {:ok, connection} = Adapter.connect(Keyword.put(options, :parameters, search_path: schema))

    Process.unlink(connection)

    on_exit(fn ->
      if Process.alive?(connection) do
        Adapter.execute(connection, "DROP SCHEMA IF EXISTS #{schema} CASCADE", [], [])
        GenServer.stop(connection)
      end
    end)

    exec!(
      connection,
      "CREATE TABLE tw (id int PRIMARY KEY, org_key text, site text, name text, touched int DEFAULT 0)"
    )

    exec!(connection, "INSERT INTO tw (id, org_key, site, name) VALUES (1, 'T1', 'S1', 'a')")
    %{connection: connection, schema: schema}
  end

  defp exec!(connection, sql), do: {:ok, _} = Adapter.execute(connection, sql, [], [])

  defp update(assignments, scope, relation \\ "tw") do
    {:ok, command} =
      Command.new(%{
        operation: :update,
        relation: relation,
        predicate: {:and, [{:eq, {:field, "id"}, {:literal, 1}}, scope]},
        assignments: Enum.map(assignments, fn {f, v} -> %{field: f, value: {:literal, v}} end),
        expected_cardinality: {:exactly, 1},
        metadata: %{post_image: %{predicate: scope, key: ["id"]}}
      })

    command
  end

  defp insert(values, guard) do
    {:ok, command} =
      Command.new(%{
        operation: :insert,
        relation: "tw",
        assignments: Enum.map(values, fn {f, v} -> %{field: f, value: {:literal, v}} end),
        returning: [:id],
        metadata: %{post_image: %{predicate: guard, key: ["id"]}}
      })

    command
  end

  defp site(value), do: {:eq, {:field, "site"}, {:literal, value}}
  defp tenant(value), do: {:eq, {:field, "org_key"}, {:literal, value}}

  defp stored(connection) do
    {:ok, %{rows: rows}} =
      Adapter.execute(connection, "SELECT id, org_key, site, name FROM tw ORDER BY id", [], [])

    rows
  end

  defp violation?(result),
    do: match?({:error, %Error{details: %{code: :query_rule_violation}}}, result)

  test "composite post-image keys bind integer and text columns, including no returned rows", %{
    connection: c
  } do
    exec!(
      c,
      "CREATE TABLE compound_key (tenant_id int NOT NULL, id int NOT NULL, token text NOT NULL, name text, UNIQUE (tenant_id, id, token))"
    )

    exec!(
      c,
      "INSERT INTO compound_key VALUES (7, 1, 'first', 'a'), (7, 2, 'second', 'b'), (8, 1, 'first', 'foreign')"
    )

    scope = {:eq, {:field, "tenant_id"}, {:literal, 7}}

    command = fn predicate, expected ->
      {:ok, value} =
        Command.new(%{
          operation: :update,
          relation: "compound_key",
          predicate: {:and, [scope, predicate]},
          assignments: [%{field: "name", value: {:literal, "updated"}}],
          expected_cardinality: {:exactly, expected},
          metadata: %{post_image: %{predicate: scope, key: ["tenant_id", "id", "token"]}}
        })

      value
    end

    assert {:ok, %{affected_rows: 2}} =
             Adapter.execute_write_unsafe(
               c,
               command.({:gt, {:field, "id"}, {:literal, 0}}, 2),
               []
             )

    assert {:error, %Error{type: :cardinality_mismatch}} =
             Adapter.execute_write_unsafe(
               c,
               command.({:eq, {:field, "id"}, {:literal, 999}}, 1),
               []
             )

    assert {:ok, %{rows: [[7, 1, "updated"], [7, 2, "updated"], [8, 1, "foreign"]]}} =
             Adapter.execute(
               c,
               "SELECT tenant_id, id, name FROM compound_key ORDER BY tenant_id, id",
               [],
               []
             )
  end

  test "the private scope receipt is a physical table key, stripped by position", %{connection: c} do
    exec!(c, "ALTER TABLE tw ADD COLUMN selecto_post_image_key_0 INTEGER DEFAULT 123")

    command = %{
      update([{"name", "updated"}], site("S1"))
      | returning: ["selecto_post_image_key_0", "name"]
    }

    assert {:ok, result} = Adapter.execute_write_unsafe(c, command, [])
    assert result.rows == [%{"selecto_post_image_key_0" => 123, "name" => "updated"}]
    command = %{update([{"site", "S9"}], site("S1")) | returning: ["selecto_post_image_key_0"]}
    assert violation?(Adapter.execute_write_unsafe(c, command, []))
    assert stored(c) == [[1, "T1", "S1", "updated"]]
  end

  test "a caller column cannot replace the appended scope decision", %{connection: c} do
    exec!(c, "ALTER TABLE tw ADD COLUMN selecto_post_image_truth INTEGER DEFAULT 1")
    command = %{update([{"site", "S9"}], site("S1")) | returning: ["selecto_post_image_truth"]}
    assert violation?(Adapter.execute_write_unsafe(c, command, []))
    assert stored(c) == [[1, "T1", "S1", "a"]]
  end

  test "an update that assigns the scoped column rolls back", %{connection: c} do
    assert violation?(Adapter.execute_write_unsafe(c, update([{"site", "S9"}], site("S1")), []))
    assert stored(c) == [[1, "T1", "S1", "a"]]
  end

  test "BEFORE row triggers that leave the scope roll back", %{connection: c} do
    exec!(
      c,
      "CREATE FUNCTION tw_low() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.org_key := lower(NEW.org_key); RETURN NEW; END $$"
    )

    exec!(
      c,
      "CREATE TRIGGER tw_b BEFORE INSERT OR UPDATE ON tw FOR EACH ROW EXECUTE FUNCTION tw_low()"
    )

    assert violation?(Adapter.execute_write_unsafe(c, update([{"name", "u"}], tenant("T1")), []))

    assert violation?(
             Adapter.execute_write_unsafe(
               c,
               insert([{"id", 2}, {"org_key", "T1"}, {"site", "S1"}], tenant("T1")),
               []
             )
           )
  end

  test "a tenant upsert checks its returned scope after BEFORE triggers",
       %{connection: c} do
    exec!(
      c,
      "CREATE TABLE tu (id int PRIMARY KEY, org_key text NOT NULL, code text NOT NULL, name text, UNIQUE (org_key, code))"
    )

    exec!(c, "INSERT INTO tu VALUES (1, 'T1', 'a', 'x')")

    upsert = fn id, name ->
      %Command{
        operation: :upsert,
        relation: "tu",
        assignments: [
          %{field: "id", value: {:literal, id}},
          %{field: "org_key", value: {:literal, "T1"}},
          %{field: "code", value: {:literal, "a"}},
          %{field: "name", value: {:literal, name}}
        ],
        predicate: nil,
        returning: :none,
        expected_cardinality: {:exactly, 1},
        metadata: %{
          conflict_target: ["org_key", "code"],
          declared_conflict_targets: [["org_key", "code"]],
          upsert_update_fields: ["name"],
          tenant_fields: ["org_key"],
          post_image: %{predicate: tenant("T1"), key: ["id"]}
        }
      }
    end

    assert {:ok, %{affected_rows: 1}} = Adapter.execute_write_unsafe(c, upsert.(2, "y"), [])

    exec!(
      c,
      "CREATE FUNCTION tu_low() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.org_key := lower(NEW.org_key); RETURN NEW; END $$"
    )

    exec!(c, "CREATE TRIGGER tu_b BEFORE UPDATE ON tu FOR EACH ROW EXECUTE FUNCTION tu_low()")

    assert violation?(Adapter.execute_write_unsafe(c, upsert.(2, "z"), []))

    {:ok, %{rows: rows}} = Adapter.execute(c, "SELECT id, org_key, code, name FROM tu", [], [])
    assert rows == [[1, "T1", "a", "y"]]
  end

  test "a managed write runs READ COMMITTED on a REPEATABLE READ default connection",
       %{connection: c, schema: schema} do
    exec!(
      c,
      "CREATE FUNCTION tw_iso() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.name := current_setting('transaction_isolation'); RETURN NEW; END $$"
    )

    exec!(c, "CREATE TRIGGER tw_i BEFORE UPDATE ON tw FOR EACH ROW EXECUTE FUNCTION tw_iso()")

    options =
      SelectoDBPostgreSQL.Verification.ConnectionOptions.options()
      |> Keyword.put(:pool_size, 1)
      |> Keyword.put(:parameters,
        default_transaction_isolation: "repeatable read",
        search_path: schema
      )

    {:ok, rr} = Postgrex.start_link(options)

    assert %{rows: [["repeatable read"]]} =
             Postgrex.query!(rr, "SHOW default_transaction_isolation", [])

    assert {:ok, %{affected_rows: 1}} =
             Adapter.execute_write_unsafe(rr, update([{"name", "u"}], site("S1")), [])

    assert stored(c) == [[1, "T1", "S1", "read committed"]]
    GenServer.stop(rr)
  end

  test "inside the caller's transaction a refused write undoes its own DML", %{connection: c} do
    # w3exrev16 item 18: the caller ignores the refusal and commits.
    assert {:ok, {refused, applied}} =
             Postgrex.transaction(c, fn conn ->
               refused =
                 Adapter.execute_write_unsafe(conn, update([{"site", "S9"}], site("S1")), [])

               applied =
                 Adapter.execute_write_unsafe(conn, update([{"name", "u"}], site("S1")), [])

               {refused, applied}
             end)

    assert violation?(refused)
    assert {:ok, %{affected_rows: 1}} = applied
    assert stored(c) == [[1, "T1", "S1", "u"]]
  end

  test "a table without a unique key checks every RETURNING tuple",
       %{connection: c} do
    exec!(c, "CREATE TABLE dk (id int NOT NULL, site text, name text)")
    exec!(c, "INSERT INTO dk VALUES (1, 'S1', 'x'), (1, 'S1', 'y')")

    command = fn name, assignments ->
      {:ok, command} =
        Command.new(%{
          operation: :update,
          relation: "dk",
          predicate: {:and, [{:eq, {:field, "name"}, {:literal, name}}, site("S1")]},
          assignments: Enum.map(assignments, fn {f, v} -> %{field: f, value: {:literal, v}} end),
          expected_cardinality: {:exactly, 1},
          metadata: %{post_image: %{predicate: site("S1"), key: ["id"]}}
        })

      command
    end

    assert {:ok, _} = Adapter.execute_write_unsafe(c, command.("x", [{"name", "u"}]), [])

    # The decoy row with the same id stays in scope; the written row left it.
    assert violation?(Adapter.execute_write_unsafe(c, command.("y", [{"site", "S9"}]), []))

    {:ok, %{rows: rows}} = Adapter.execute(c, "SELECT site, name FROM dk ORDER BY name", [], [])
    assert rows == [["S1", "u"], ["S1", "y"]]
  end

  test "a generated scope column is checked in RETURNING", %{connection: c} do
    exec!(
      c,
      "CREATE TABLE gc (id int PRIMARY KEY, site text, g text GENERATED ALWAYS AS (upper(site)) STORED)"
    )

    exec!(c, "INSERT INTO gc (id, site) VALUES (1, 's1')")
    g = {:eq, {:field, "g"}, {:literal, "S1"}}
    assert violation?(Adapter.execute_write_unsafe(c, update([{"site", "s9"}], g, "gc"), []))
  end

  test "ordinary triggers keep working; results keep their shape", %{connection: c} do
    exec!(
      c,
      "CREATE FUNCTION tw_touch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.touched := OLD.touched + 1; RETURN NEW; END $$"
    )

    exec!(c, "CREATE TRIGGER tw_t BEFORE UPDATE ON tw FOR EACH ROW EXECUTE FUNCTION tw_touch()")
    exec!(c, "CREATE TABLE tw_audit (id int, name text)")

    exec!(
      c,
      "CREATE FUNCTION tw_aud() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO tw_audit VALUES (NEW.id, NEW.name); RETURN NULL; END $$"
    )

    exec!(c, "CREATE TRIGGER tw_au AFTER INSERT ON tw FOR EACH ROW EXECUTE FUNCTION tw_aud()")

    assert {:ok, %{affected_rows: 1, rows: []}} =
             Adapter.execute_write_unsafe(c, update([{"name", "u"}], site("S1")), [])

    assert {:ok, %{affected_rows: 1, rows: [%{"id" => 2}]}} =
             Adapter.execute_write_unsafe(
               c,
               insert([{"id", 2}, {"org_key", "T1"}, {"site", "S1"}, {"name", "n"}], site("S1")),
               []
             )

    assert stored(c) == [[1, "T1", "S1", "u"], [2, "T1", "S1", "n"]]
    {:ok, %{rows: audit}} = Adapter.execute(c, "SELECT * FROM tw_audit", [], [])
    assert audit == [[2, "n"]]
  end

  test "a query-member lateral keeps its output alias", %{connection: c} do
    exec!(c, "CREATE TABLE mg_root (id int PRIMARY KEY, org_key text)")
    exec!(c, "INSERT INTO mg_root VALUES (1, 't1')")
    exec!(c, "CREATE TABLE mg_use (id int PRIMARY KEY, org_key text, amount int)")
    exec!(c, "INSERT INTO mg_use VALUES (1,'t1',1),(2,'T1',10),(3,'t1 ',100)")

    rel = fn table, cols ->
      %{
        source_table: table,
        primary_key: :id,
        fields: cols |> Map.keys() |> Enum.sort(),
        redact_fields: [],
        columns: Map.new(cols, fn {n, t} -> {n, %{type: t}} end),
        associations: %{}
      }
    end

    domain = %{
      name: "MG",
      source:
        rel.("mg_root", %{id: :integer, org_key: :string}) |> Map.put(:tenant_field, :org_key),
      schemas: %{use: rel.("mg_use", %{id: :integer, org_key: :string, amount: :integer})},
      joins: %{},
      query_members: %{
        laterals: %{
          lat: %{
            source: "use",
            query: %{"select" => [%{"as" => "s", "aggregate" => "sum", "field" => "amount"}]},
            correlations: %{org_key: "org_key"},
            join_type: "left"
          }
        }
      }
    }

    query =
      domain
      |> Selecto.configure(c, adapter: Adapter, validate: false)
      |> Selecto.with_tenant("t1")
      |> Selecto.with_lateral(:lat)
      |> Selecto.select(["id", "lat.s"])

    assert {:ok, {[[1, 1]], _, _}} = Selecto.execute(query)
  end
end
