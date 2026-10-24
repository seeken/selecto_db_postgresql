defmodule SelectoDBPostgreSQL.PostImageIntegrationTest do
  # TW-09 post-image (user decision 2026-10-24): a guarded INSERT/UPDATE's
  # stored rows must stay inside the engine tenant and the trusted/host scope
  # (`metadata.post_image`, set by SelectoUpdato), whatever the update
  # assigns, BEFORE/AFTER row triggers rewrite or a generated column derives.
  # The statement returns its rows' keys and one tagged SELECT re-reads them
  # inside the transaction; statement-level and deferrable triggers, rules,
  # views and tables without a usable key are refused. Ordinary triggers
  # keep working. Query-member laterals keep their output names.
  use ExUnit.Case, async: false

  alias Selecto.Write.{Command, Error}
  alias SelectoDBPostgreSQL.Adapter

  @moduletag :postgres

  setup do
    {:ok, connection} =
      Adapter.connect(SelectoDBPostgreSQL.Verification.ConnectionOptions.options())

    Process.unlink(connection)
    schema = "selecto_post_image_#{System.unique_integer([:positive])}"
    exec!(connection, "CREATE SCHEMA #{schema}")
    exec!(connection, "SET search_path TO #{schema}")

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

  defp not_evaluable?(result),
    do: match?({:error, %Error{details: %{code: :query_rule_not_evaluable}}}, result)

  test "an update that assigns the scoped column rolls back", %{connection: c} do
    assert violation?(Adapter.execute_write_unsafe(c, update([{"site", "S9"}], site("S1")), []))
    assert stored(c) == [[1, "T1", "S1", "a"]]
  end

  test "BEFORE and AFTER row triggers that leave the scope roll back", %{connection: c} do
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

    exec!(c, "DROP TRIGGER tw_b ON tw")

    exec!(
      c,
      "CREATE FUNCTION tw_alow() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.org_key <> lower(NEW.org_key) THEN UPDATE tw SET org_key = lower(org_key) WHERE id = NEW.id; END IF; RETURN NULL; END $$"
    )

    exec!(
      c,
      "CREATE TRIGGER tw_a AFTER INSERT OR UPDATE ON tw FOR EACH ROW EXECUTE FUNCTION tw_alow()"
    )

    assert violation?(Adapter.execute_write_unsafe(c, update([{"name", "u"}], tenant("T1")), []))
    assert stored(c) == [[1, "T1", "S1", "a"]]
  end

  test "statement-level and deferrable triggers, rules and views are refused", %{connection: c} do
    exec!(
      c,
      "CREATE FUNCTION tw_noop() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$"
    )

    exec!(
      c,
      "CREATE TRIGGER tw_s AFTER UPDATE ON tw FOR EACH STATEMENT EXECUTE FUNCTION tw_noop()"
    )

    assert not_evaluable?(
             Adapter.execute_write_unsafe(c, update([{"name", "u"}], site("S1")), [])
           )

    exec!(c, "DROP TRIGGER tw_s ON tw")

    exec!(
      c,
      "CREATE CONSTRAINT TRIGGER tw_d AFTER UPDATE ON tw DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION tw_noop()"
    )

    assert not_evaluable?(
             Adapter.execute_write_unsafe(c, update([{"name", "u"}], site("S1")), [])
           )

    exec!(c, "DROP TRIGGER tw_d ON tw")

    exec!(c, "CREATE TABLE tw_log (id int)")
    exec!(c, "CREATE RULE tw_r AS ON UPDATE TO tw DO ALSO INSERT INTO tw_log VALUES (NEW.id)")

    assert not_evaluable?(
             Adapter.execute_write_unsafe(c, update([{"name", "u"}], site("S1")), [])
           )

    exec!(c, "DROP RULE tw_r ON tw")

    exec!(c, "CREATE VIEW tw_v AS SELECT * FROM tw")

    refute match?(
             {:ok, _},
             Adapter.execute_write_unsafe(c, update([{"name", "u"}], site("S1"), "tw_v"), [])
           )

    assert stored(c) == [[1, "T1", "S1", "a"]]
  end

  test "a deferrable trigger anywhere, partition statement triggers and inheritance children are refused",
       %{connection: c} do
    exec!(
      c,
      "CREATE FUNCTION tw_noop() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$"
    )

    # A deferred trigger on an audit table fires at commit, after the check.
    exec!(c, "CREATE TABLE tw_audit (id int)")

    exec!(
      c,
      "CREATE CONSTRAINT TRIGGER tw_audit_d AFTER INSERT ON tw_audit DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION tw_noop()"
    )

    assert not_evaluable?(
             Adapter.execute_write_unsafe(c, update([{"name", "u"}], site("S1")), [])
           )

    exec!(c, "DROP TRIGGER tw_audit_d ON tw_audit")

    # A statement trigger on a leaf partition.
    exec!(c, "CREATE TABLE pt (id int PRIMARY KEY, site text, name text) PARTITION BY RANGE (id)")
    exec!(c, "CREATE TABLE pt_1 PARTITION OF pt FOR VALUES FROM (0) TO (1000)")
    exec!(c, "INSERT INTO pt VALUES (1, 'S1', 'a')")

    exec!(
      c,
      "CREATE TRIGGER pt_s AFTER UPDATE ON pt_1 FOR EACH STATEMENT EXECUTE FUNCTION tw_noop()"
    )

    assert not_evaluable?(
             Adapter.execute_write_unsafe(c, update([{"name", "u"}], site("S1"), "pt"), [])
           )

    exec!(c, "DROP TRIGGER pt_s ON pt_1")

    assert {:ok, _} =
             Adapter.execute_write_unsafe(c, update([{"name", "u"}], site("S1"), "pt"), [])

    # An inheritance child can hold a decoy row with the parent's key.
    exec!(c, "CREATE TABLE ih (id int PRIMARY KEY, site text, name text)")
    exec!(c, "CREATE TABLE ih_child () INHERITS (ih)")
    exec!(c, "INSERT INTO ih VALUES (1, 'S1', 'a')")

    assert not_evaluable?(
             Adapter.execute_write_unsafe(c, update([{"name", "u"}], site("S1"), "ih"), [])
           )

    assert stored(c) == [[1, "T1", "S1", "a"]]
  end

  test "a tenant upsert is re-read; a trigger moving it out of the tenant rolls back",
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

  test "a deferrable trigger created while the write waits on a row lock is caught after the DML",
       %{connection: c, schema: schema} do
    # w3exrev16 item 14: session B holds the row, creates a deferrable
    # trigger on the audit table the write's row trigger inserts into, and
    # commits; the write then fires its row trigger (queueing the deferred
    # event) and the database-wide check, which runs after the DML in the
    # re-read, must see the new trigger.
    exec!(c, "CREATE TABLE tw_audit (id int)")

    exec!(
      c,
      "CREATE FUNCTION tw_aud() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO tw_audit VALUES (NEW.id); RETURN NULL; END $$"
    )

    exec!(c, "CREATE TRIGGER tw_au AFTER UPDATE ON tw FOR EACH ROW EXECUTE FUNCTION tw_aud()")

    exec!(
      c,
      "CREATE FUNCTION tw_noop() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$"
    )

    options = SelectoDBPostgreSQL.Verification.ConnectionOptions.options()
    {:ok, b} = Postgrex.start_link(Keyword.put(options, :pool_size, 1))
    {:ok, watcher} = Postgrex.start_link(Keyword.put(options, :pool_size, 1))
    parent = self()

    holder =
      Task.async(fn ->
        Postgrex.transaction(b, fn conn ->
          Postgrex.query!(conn, "SET LOCAL search_path TO #{schema}", [])
          Postgrex.query!(conn, "SELECT id FROM tw WHERE id = 1 FOR UPDATE", [])
          send(parent, :locked)

          receive do
            :create ->
              Postgrex.query!(
                conn,
                "CREATE CONSTRAINT TRIGGER tw_audit_d AFTER INSERT ON tw_audit DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION tw_noop()",
                []
              )
          end
        end)
      end)

    assert_receive :locked, 5_000

    writer =
      Task.async(fn ->
        Adapter.execute_write_unsafe(c, update([{"name", "u"}], site("S1")), [])
      end)

    # Wait until the write is blocked on the row lock.
    waiting = fn ->
      %{rows: [[n]]} =
        Postgrex.query!(
          watcher,
          "SELECT count(*) FROM pg_locks WHERE NOT granted AND locktype = 'transactionid'",
          []
        )

      n
    end

    assert Enum.any?(1..100, fn _ -> waiting.() > 0 or (Process.sleep(50) && false) end)
    send(holder.pid, :create)
    assert {:ok, _} = Task.await(holder, 10_000)
    assert not_evaluable?(Task.await(writer, 10_000))
    assert stored(c) == [[1, "T1", "S1", "a"]]
    {:ok, %{rows: audit}} = Adapter.execute(c, "SELECT * FROM tw_audit", [], [])
    assert audit == []
    GenServer.stop(b)
    GenServer.stop(watcher)
  end

  test "a guarded write outside READ COMMITTED is refused", %{connection: c} do
    # A REPEATABLE READ or SERIALIZABLE snapshot would hide a trigger
    # committed after the transaction's first statement from the check.
    {:ok, result} =
      Postgrex.transaction(c, fn conn ->
        Postgrex.query!(conn, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ", [])
        Adapter.execute_write_unsafe(conn, update([{"name", "u"}], site("S1")), [])
      end)

    assert not_evaluable?(result)
  end

  test "a host REPEATABLE READ transaction cannot see a trigger committed after its snapshot",
       %{connection: c, schema: schema} do
    # w3exrev16 item 16: the catalog checks read the transaction snapshot, so
    # a deferrable trigger committed after it would pass and still fire at
    # commit. Guarded writes are refused outside READ COMMITTED.
    options = SelectoDBPostgreSQL.Verification.ConnectionOptions.options()
    {:ok, b} = Postgrex.start_link(Keyword.put(options, :pool_size, 1))
    Postgrex.query!(b, "SET search_path TO #{schema}", [])

    {:ok, result} =
      Postgrex.transaction(c, fn conn ->
        Postgrex.query!(conn, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ", [])
        Postgrex.query!(conn, "SELECT count(*) FROM tw", [])

        Postgrex.query!(
          b,
          "CREATE FUNCTION tw_dlow() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.org_key <> lower(NEW.org_key) THEN UPDATE tw SET org_key = lower(org_key) WHERE id = NEW.id; END IF; RETURN NULL; END $$",
          []
        )

        Postgrex.query!(
          b,
          "CREATE CONSTRAINT TRIGGER tw_dl AFTER UPDATE ON tw DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION tw_dlow()",
          []
        )

        Adapter.execute_write_unsafe(conn, update([{"name", "u"}], tenant("T1")), [])
      end)

    assert {:error,
            %Error{details: %{code: :query_rule_not_evaluable, reason: :transaction_isolation}}} =
             result

    assert stored(c) == [[1, "T1", "S1", "a"]]
    GenServer.stop(b)
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

  defp delete(scope, relation \\ "tw", bound \\ true) do
    {:ok, command} =
      Command.new(%{
        operation: :delete,
        relation: relation,
        predicate: scope,
        expected_cardinality: :many,
        metadata: if(bound, do: %{bound_scope: true}, else: %{})
      })

    command
  end

  defp pwn!(c, table \\ "tw") do
    exec!(
      c,
      "CREATE OR REPLACE FUNCTION tw_pwn() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN UPDATE tw SET name = 'pwned' WHERE org_key = 'T2'; RETURN NULL; END $$"
    )

    exec!(
      c,
      "CREATE TRIGGER #{table}_pwn AFTER DELETE ON #{table} FOR EACH STATEMENT EXECUTE FUNCTION tw_pwn()"
    )
  end

  test "a bound-scope delete is refused under statement-level DELETE triggers and rules",
       %{connection: c} do
    # w3exp5 item 17: the statement trigger rewrote the other tenant's row.
    exec!(c, "INSERT INTO tw (id, org_key, site, name) VALUES (2, 'T2', 'S2', 'b')")
    pwn!(c)
    assert not_evaluable?(Adapter.execute_write_unsafe(c, delete(tenant("T1")), []))
    assert stored(c) == [[1, "T1", "S1", "a"], [2, "T2", "S2", "b"]]
    exec!(c, "ALTER TABLE tw DISABLE TRIGGER tw_pwn")

    exec!(c, "CREATE TABLE tw_log (id int)")
    exec!(c, "CREATE RULE tw_r AS ON DELETE TO tw DO ALSO INSERT INTO tw_log VALUES (OLD.id)")
    assert not_evaluable?(Adapter.execute_write_unsafe(c, delete(tenant("T1")), []))
    exec!(c, "DROP RULE tw_r ON tw")

    # Across the partition and inheritance trees.
    exec!(c, "CREATE TABLE pt (id int, org_key text) PARTITION BY RANGE (id)")
    exec!(c, "CREATE TABLE pt_1 PARTITION OF pt FOR VALUES FROM (0) TO (1000)")
    exec!(c, "INSERT INTO pt VALUES (1, 'T1')")
    pwn!(c, "pt_1")
    assert not_evaluable?(Adapter.execute_write_unsafe(c, delete(tenant("T1"), "pt"), []))

    exec!(c, "CREATE TABLE ih (id int, org_key text)")
    exec!(c, "CREATE TABLE ih_child () INHERITS (ih)")
    exec!(c, "INSERT INTO ih VALUES (1, 'T1')")
    pwn!(c, "ih_child")
    assert not_evaluable?(Adapter.execute_write_unsafe(c, delete(tenant("T1"), "ih"), []))

    # A view is refused; an unbound delete is not this check's concern.
    exec!(c, "CREATE VIEW tw_v AS SELECT * FROM tw")
    refute match?({:ok, _}, Adapter.execute_write_unsafe(c, delete(tenant("T1"), "tw_v"), []))
    assert stored(c) == [[1, "T1", "S1", "a"], [2, "T2", "S2", "b"]]

    assert {:ok, %{affected_rows: 1}} =
             Adapter.execute_write_unsafe(c, delete(tenant("T1"), "tw", false), [])

    exec!(c, "ALTER TABLE tw ENABLE TRIGGER tw_pwn")
    exec!(c, "DROP TRIGGER tw_pwn ON tw")
    assert {:ok, %{affected_rows: 0}} = Adapter.execute_write_unsafe(c, delete(tenant("T1")), [])
    assert stored(c) == [[2, "T2", "S2", "b"]]
  end

  test "a statement DELETE trigger created while the delete waits on a row lock is refused",
       %{connection: c, schema: schema} do
    exec!(c, "INSERT INTO tw (id, org_key, site, name) VALUES (2, 'T2', 'S2', 'b')")

    exec!(
      c,
      "CREATE FUNCTION tw_pwn() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN UPDATE tw SET name = 'pwned' WHERE org_key = 'T2'; RETURN NULL; END $$"
    )

    options = SelectoDBPostgreSQL.Verification.ConnectionOptions.options()
    {:ok, b} = Postgrex.start_link(Keyword.put(options, :pool_size, 1))
    {:ok, watcher} = Postgrex.start_link(Keyword.put(options, :pool_size, 1))
    parent = self()

    holder =
      Task.async(fn ->
        Postgrex.transaction(b, fn conn ->
          Postgrex.query!(conn, "SET LOCAL search_path TO #{schema}", [])
          Postgrex.query!(conn, "SELECT id FROM tw WHERE id = 1 FOR UPDATE", [])
          send(parent, :locked)

          receive do
            :create ->
              Postgrex.query(
                conn,
                "CREATE TRIGGER tw_pwn AFTER DELETE ON tw FOR EACH STATEMENT EXECUTE FUNCTION tw_pwn()",
                []
              )
          end
        end)
      end)

    assert_receive :locked, 5_000
    writer = Task.async(fn -> Adapter.execute_write_unsafe(c, delete(tenant("T1")), []) end)

    waiting = fn ->
      %{rows: [[n]]} =
        Postgrex.query!(
          watcher,
          "SELECT count(*) FROM pg_locks WHERE NOT granted AND locktype = 'transactionid'",
          []
        )

      n
    end

    assert Enum.any?(1..100, fn _ -> waiting.() > 0 or (Process.sleep(50) && false) end)
    send(holder.pid, :create)
    _ = Task.await(holder, 15_000)
    # Either the trigger creation deadlocks against the delete's ROW
    # EXCLUSIVE lock, or it commits and the after-DML check refuses.
    refute match?({:ok, _}, Task.await(writer, 15_000)) and
             Enum.any?(stored(c), &(List.last(&1) == "pwned"))

    {:ok, %{rows: rows}} = Adapter.execute(c, "SELECT name FROM tw WHERE id = 2", [], [])
    assert rows == [["b"]]
    GenServer.stop(b)
    GenServer.stop(watcher)
  end

  test "a table without a usable unique key is refused", %{connection: c} do
    exec!(c, "CREATE TABLE dk (id int NOT NULL, site text, name text)")
    exec!(c, "CREATE INDEX dk_id ON dk (id)")
    exec!(c, "INSERT INTO dk VALUES (1, 'S1', 'x'), (1, 'S2', 'y')")

    {:ok, command} =
      Command.new(%{
        operation: :update,
        relation: "dk",
        predicate: {:and, [{:eq, {:field, "name"}, {:literal, "x"}}, site("S1")]},
        assignments: [%{field: "name", value: {:literal, "u"}}],
        expected_cardinality: {:exactly, 1},
        metadata: %{post_image: %{predicate: site("S1"), key: ["id"]}}
      })

    assert not_evaluable?(Adapter.execute_write_unsafe(c, command, []))
  end

  test "a generated scope column is re-read", %{connection: c} do
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
