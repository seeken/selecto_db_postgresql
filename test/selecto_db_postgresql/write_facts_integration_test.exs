Code.require_file("../support/custom_uuid.exs", __DIR__)

defmodule SelectoDBPostgreSQL.WriteFactsIntegrationTest do
  use ExUnit.Case, async: false

  alias Selecto.Write.Command
  alias SelectoDBPostgreSQL.Adapter
  alias SelectoDBPostgreSQL.Verification.ConnectionOptions

  @moduletag :postgres
  @moduletag :requires_db

  setup do
    {:ok, connection} = Adapter.connect(Keyword.put(ConnectionOptions.options(), :pool_size, 1))
    Process.unlink(connection)
    on_exit(fn -> if Process.alive?(connection), do: GenServer.stop(connection) end)
    %{connection: connection}
  end

  defp insert(connection, relation, value) do
    {:ok, command} =
      Command.new(%{
        operation: :insert,
        relation: relation,
        assignments: [%{field: "org", value: {:literal, value}}],
        metadata: %{tenant_fields: ["org"]},
        expected_cardinality: {:exactly, 1}
      })

    Adapter.execute_write_unsafe(connection, command, [])
  end

  test "fresh descriptions follow DDL on a reused connection", %{connection: c} do
    uuid = "0000000a-0000-0000-0000-000000000001"
    Postgrex.query!(c, "CREATE TEMP TABLE selecto_write_facts (org text)", [])
    assert {:ok, _} = insert(c, "selecto_write_facts", String.upcase(uuid))
    Postgrex.query!(c, "TRUNCATE selecto_write_facts", [])
    Postgrex.query!(c, "ALTER TABLE selecto_write_facts ALTER org TYPE uuid USING org::uuid", [])

    assert {:error, %{details: %{reason: :invalid_uuid}}} =
             insert(c, "selecto_write_facts", String.upcase(uuid))

    assert {:ok, _} = insert(c, "selecto_write_facts", uuid)
    Postgrex.query!(c, "ALTER TABLE selecto_write_facts ALTER org TYPE text USING org::text", [])
    assert {:ok, _} = insert(c, "selecto_write_facts", "not-a-uuid")
  end

  test "domain UUID descriptions and DISCARD ALL never reuse a stale plan", %{connection: c} do
    uuid = "0000000a-0000-0000-0000-000000000001"
    Postgrex.query!(c, "CREATE DOMAIN pg_temp.selecto_uuid_domain AS uuid", [])

    Postgrex.query!(
      c,
      "CREATE TEMP TABLE selecto_write_facts (org pg_temp.selecto_uuid_domain)",
      []
    )

    assert {:error, %{details: %{reason: :invalid_uuid}}} =
             insert(c, "selecto_write_facts", "bad")

    assert {:ok, _} = insert(c, "selecto_write_facts", uuid)
    Postgrex.query!(c, "DISCARD ALL", [])
    Postgrex.query!(c, "CREATE TEMP TABLE selecto_write_facts (org text)", [])
    assert {:ok, _} = insert(c, "selecto_write_facts", "bad")
    assert %{rows: [[0]]} = Postgrex.query!(c, "SELECT count(*) FROM pg_prepared_statements", [])

    opts =
      ConnectionOptions.options()
      |> Keyword.put(:pool_size, 1)
      |> Keyword.put(:types, SelectoDBPostgreSQL.Verification.CustomUUIDTypes)

    {:ok, custom} = Adapter.connect(opts)
    Process.unlink(custom)

    try do
      Postgrex.query!(custom, "CREATE DOMAIN pg_temp.selecto_uuid_domain AS uuid", [])

      Postgrex.query!(
        custom,
        "CREATE TEMP TABLE selecto_write_facts (org pg_temp.selecto_uuid_domain)",
        []
      )

      assert {:error, %{details: %{reason: :invalid_uuid}}} =
               insert(custom, "selecto_write_facts", String.upcase(uuid))

      assert {:ok, _} = insert(custom, "selecto_write_facts", uuid)
    after
      GenServer.stop(custom)
    end
  end

  test "ParameterStatus follows SET LOCAL rollback, reset and connection reuse", %{connection: c} do
    Postgrex.query!(c, "CREATE TEMP TABLE selecto_write_facts (org text)", [])

    DBConnection.run(c, fn conn ->
      Postgrex.query!(conn, "BEGIN", [])
      Postgrex.query!(conn, "SET LOCAL client_encoding = LATIN1", [])

      assert {:error, %{details: %{reason: :client_encoding}}} =
               insert(conn, "selecto_write_facts", "café")

      Postgrex.query!(conn, "ROLLBACK", [])
    end)

    assert {:ok, _} = insert(c, "selecto_write_facts", "café")
    Postgrex.query!(c, "SET client_encoding = LATIN1", [])

    assert {:error, %{details: %{reason: :client_encoding}}} =
             insert(c, "selecto_write_facts", "café")

    Postgrex.query!(c, "RESET client_encoding", [])
    assert {:ok, _} = insert(c, "selecto_write_facts", "café")

    assert %{rows: [["café"], ["café"]]} =
             Postgrex.query!(c, "SELECT org FROM selecto_write_facts", [])
  end

  test "an owned write deadline cannot leave its write committed", %{connection: c} do
    table = "selecto_write_deadline_#{System.unique_integer([:positive])}"
    Process.unlink(c)
    Postgrex.query!(c, "CREATE TABLE #{table} (org text)", [])

    Postgrex.query!(
      c,
      "CREATE FUNCTION #{table}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(0.3); RETURN NEW; END $$",
      []
    )

    Postgrex.query!(
      c,
      "CREATE TRIGGER slow BEFORE INSERT ON #{table} FOR EACH ROW EXECUTE FUNCTION #{table}()",
      []
    )

    {:ok, observer} = Adapter.connect(Keyword.put(ConnectionOptions.options(), :pool_size, 1))
    Process.unlink(observer)

    on_exit(fn ->
      Postgrex.query!(observer, "DROP TABLE #{table}", [])
      Postgrex.query!(observer, "DROP FUNCTION #{table}()", [])
      GenServer.stop(observer)
    end)

    {:ok, command} =
      Command.new(%{
        operation: :insert,
        relation: table,
        assignments: [%{field: "org", value: {:literal, "mine"}}],
        expected_cardinality: {:exactly, 1}
      })

    assert {:error, _} = Adapter.execute_write_unsafe(c, command, timeout: 40)
    # The observer waits on any outstanding write before checking its outcome.
    assert {:ok, _} =
             Postgrex.transaction(observer, fn conn ->
               Postgrex.query!(conn, "LOCK TABLE #{table} IN ACCESS EXCLUSIVE MODE", [])
               assert %{rows: [[0]]} = Postgrex.query!(conn, "SELECT count(*) FROM #{table}", [])
             end)
  end
end
