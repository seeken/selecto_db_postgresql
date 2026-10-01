# selecto_updato is not a dependency of this adapter, and its
# SelectoUpdato.GovernedWrite is the only module Selecto.Write.Authorization
# lets issue an authorization. This stand-in of the same name, compiled only
# for this suite, plays the governed entry point so the adapter's governed
# callbacks can be exercised with a real authorization.
defmodule SelectoUpdato.GovernedWrite do
  @moduledoc false

  alias Selecto.Write.Authorization

  # Matching the result keeps issue/1 out of tail position, so this module's
  # frame is on the stack when Authorization checks its caller.
  def authorize!(write) do
    {:ok, authorization} = Authorization.issue(write)
    authorization
  end

  def prepared(write, context) do
    with {:ok, authorization} <- Authorization.issue(write) do
      {:ok, write, context, authorization}
    end
  end
end

defmodule SelectoDBPostgreSQL.GovernedWriteBoundaryIntegrationTest do
  use ExUnit.Case, async: false

  alias Selecto.Write.{Authorization, Batch, Command, Error, Graph, Result}
  alias Selecto.Write.Graph.{Node, Row}
  alias SelectoDBPostgreSQL.Adapter
  alias SelectoUpdato.GovernedWrite

  @moduletag :postgres

  @seed [[1, 7, "a"], [2, 7, "b"], [3, 8, "c"]]

  setup do
    {:ok, connection} =
      Adapter.connect(SelectoDBPostgreSQL.Verification.ConnectionOptions.options())

    Process.unlink(connection)

    on_exit(fn ->
      if Process.alive?(connection), do: GenServer.stop(connection)
    end)

    execute!(connection, """
    CREATE TEMP TABLE selecto_governed_writes (
      id bigint PRIMARY KEY,
      tenant_id bigint NOT NULL,
      name text NOT NULL
    )
    """)

    execute!(
      connection,
      "INSERT INTO selecto_governed_writes VALUES (1, 7, 'a'), (2, 7, 'b'), (3, 8, 'c')"
    )

    %{connection: connection, selecto: %Selecto{adapter: Adapter, connection: connection}}
  end

  test "the normal execute API refuses a raw command, batch, and graph", %{
    connection: connection,
    selecto: selecto
  } do
    for write <- [update(1, "direct"), batch!(), graph!()] do
      assert {:error, %Error{type: :ungoverned_write}} =
               Adapter.execute_write(connection, write, [])

      assert {:error, %Error{type: :ungoverned_write}} = Selecto.Write.execute(selecto, write)
    end

    assert rows(connection) == @seed
  end

  test "a raw prepared write is refused and rolled back", %{connection: connection} do
    assert {:error, %Error{type: :ungoverned_write}} =
             Adapter.execute_prepared_write(connection, fn _loader ->
               {:ok, update(1, "direct"), %{}}
             end)

    assert rows(connection) == @seed
  end

  test "a lookalike, spent, or mismatched authorization is refused", %{connection: connection} do
    command = update(1, "forged")
    forged = struct!(Authorization, ref: make_ref())

    assert {:error, %Error{type: :ungoverned_write}} =
             Adapter.execute_write(connection, command, authorization: forged)

    authorization = GovernedWrite.authorize!(command)

    assert {:error, %Error{type: :ungoverned_write}} =
             Adapter.execute_write(connection, update(1, "other"), authorization: authorization)

    authorization = GovernedWrite.authorize!(command)

    assert {:ok, %Result{affected_rows: 1}} =
             Adapter.execute_write(connection, command, authorization: authorization)

    execute!(connection, "UPDATE selecto_governed_writes SET name = 'a' WHERE id = 1")

    assert {:error, %Error{type: :ungoverned_write}} =
             Adapter.execute_write(connection, command, authorization: authorization)

    assert rows(connection) == @seed
  end

  test "a governed command, batch, and graph execute", %{
    connection: connection,
    selecto: selecto
  } do
    for write <- [update(2, "governed"), batch!(), graph!()] do
      authorization = GovernedWrite.authorize!(write)

      assert {:ok, _result} =
               Selecto.Write.execute(selecto, write, authorization: authorization)
    end

    assert rows(connection) == [
             [1, 7, "batch"],
             [2, 7, "governed"],
             [3, 8, "c"],
             [4, 7, "batch"],
             [5, 7, "graph"]
           ]
  end

  test "a governed prepared write executes", %{connection: connection, selecto: selecto} do
    assert {:ok, %Result{affected_rows: 1}} =
             Selecto.Write.execute_prepared(selecto, fn _loader ->
               GovernedWrite.prepared(update(3, "prepared"), %{})
             end)

    assert [[3, 8, "prepared"]] = Enum.filter(rows(connection), &(hd(&1) == 3))
  end

  test "the unsafe primitives still execute for trusted tooling", %{connection: connection} do
    assert {:ok, %Result{affected_rows: 1}} =
             Adapter.execute_write_unsafe(connection, update(1, "unsafe"))

    assert {:ok, %Result{affected_rows: 1}} =
             Adapter.execute_prepared_write_unsafe(connection, fn _loader ->
               {:ok, update(2, "unsafe"), %{}}
             end)

    assert rows(connection) == [[1, 7, "unsafe"], [2, 7, "unsafe"], [3, 8, "c"]]
  end

  defp update(id, name) do
    {:ok, command} =
      Command.new(%{
        operation: :update,
        relation: :selecto_governed_writes,
        assignments: [%{field: :name, value: {:literal, name}}],
        predicate: {:eq, {:field, :id}, {:literal, id}}
      })

    command
  end

  defp insert(id, name) do
    {:ok, command} =
      Command.new(%{
        operation: :insert,
        relation: :selecto_governed_writes,
        assignments: [
          %{field: :id, value: {:literal, id}},
          %{field: :tenant_id, value: {:literal, 7}},
          %{field: :name, value: {:literal, name}}
        ],
        returning: [:id]
      })

    command
  end

  defp batch! do
    {:ok, batch} = Batch.new([update(1, "batch"), insert(4, "batch")])
    batch
  end

  defp graph! do
    {:ok, graph} =
      Graph.new(
        [
          %Node{
            id: "root",
            path: [],
            relation: :selecto_governed_writes,
            strategy: :ordered,
            rows: [%Row{id: "root", path: [], command: insert(5, "graph")}]
          }
        ],
        {"root", "root"}
      )

    graph
  end

  defp rows(connection) do
    %{rows: rows} =
      execute!(connection, "SELECT id, tenant_id, name FROM selecto_governed_writes ORDER BY id")

    rows
  end

  defp execute!(connection, sql) do
    {:ok, result} = Adapter.execute(connection, sql, [], [])
    result
  end
end
