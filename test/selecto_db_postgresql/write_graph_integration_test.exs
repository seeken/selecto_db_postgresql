defmodule SelectoDBPostgreSQL.WriteGraphIntegrationTest do
  use ExUnit.Case, async: false

  alias Selecto.Write.{Command, Error, Graph}
  alias Selecto.Write.Graph.{Binding, Node, Row}
  alias SelectoDBPostgreSQL.Adapter

  @moduletag :postgres

  setup do
    {:ok, connection} =
      Adapter.connect(SelectoDBPostgreSQL.Verification.ConnectionOptions.options())

    Process.unlink(connection)

    on_exit(fn ->
      if Process.alive?(connection), do: GenServer.stop(connection)
    end)

    execute!(connection, """
    CREATE TEMP TABLE selecto_graph_orders (
      id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      tenant_id integer NOT NULL,
      reference text NOT NULL
    )
    """)

    execute!(connection, """
    CREATE TEMP TABLE selecto_graph_items (
      id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      tenant_id integer NOT NULL,
      order_id bigint NOT NULL REFERENCES selecto_graph_orders(id),
      sku text NOT NULL,
      quantity integer NOT NULL
    )
    """)

    %{connection: connection}
  end

  test "executes generated-key insert and owned sync atomically", %{connection: connection} do
    assert {:ok, insert_result} = Adapter.execute_write(connection, insert_graph!())
    assert [%{"id" => order_id}] = insert_result.rows

    assert {:ok, %{rows: [[first_id, "A"], [second_id, "B"]]}} =
             Adapter.execute(
               connection,
               "SELECT id, sku FROM selecto_graph_items WHERE order_id = $1 ORDER BY id",
               [order_id],
               []
             )

    assert {:ok, sync_result} =
             Adapter.execute_write(connection, sync_graph!(order_id, first_id))

    expected_strategy =
      case Adapter.server_version_major(connection) do
        {:ok, major} when major >= 17 -> :merge
        _ -> :ordered_fallback
      end

    assert sync_result.metadata.node_strategies["items"] == expected_strategy

    assert {:ok, %{rows: [[^first_id, "A-updated", 4], [new_id, "C", 1]]}} =
             Adapter.execute(
               connection,
               "SELECT id, sku, quantity FROM selecto_graph_items WHERE order_id = $1 ORDER BY id",
               [order_id],
               []
             )

    assert new_id != second_id
  end

  test "ownership mismatch rolls back root and child changes", %{connection: connection} do
    assert {:ok, result} = Adapter.execute_write(connection, insert_graph!())
    [%{"id" => order_id}] = result.rows

    execute!(
      connection,
      "INSERT INTO selecto_graph_orders (tenant_id, reference) VALUES (7, 'OTHER')"
    )

    assert {:ok, %{rows: [[other_order_id]]}} =
             Adapter.execute(
               connection,
               "SELECT id FROM selecto_graph_orders WHERE reference = 'OTHER'",
               [],
               []
             )

    execute!(
      connection,
      "INSERT INTO selecto_graph_items (tenant_id, order_id, sku, quantity) VALUES (7, $1, 'FOREIGN', 1)",
      [other_order_id]
    )

    assert {:ok, %{rows: [[foreign_id]]}} =
             Adapter.execute(
               connection,
               "SELECT id FROM selecto_graph_items WHERE order_id = $1",
               [other_order_id],
               []
             )

    assert {:error, %Error{type: :cardinality_mismatch}} =
             Adapter.execute_write(connection, sync_graph!(order_id, foreign_id))

    assert {:ok, %{rows: [["SO-100"]]}} =
             Adapter.execute(
               connection,
               "SELECT reference FROM selecto_graph_orders WHERE id = $1",
               [order_id],
               []
             )
  end

  defp insert_graph! do
    root =
      command!(%{
        operation: :insert,
        relation: :selecto_graph_orders,
        assignments: [
          %{field: :tenant_id, value: {:literal, 7}},
          %{field: :reference, value: {:literal, "SO-100"}}
        ],
        returning: [:id]
      })

    child_rows =
      [{"0", "A"}, {"1", "B"}]
      |> Enum.map(fn {row_id, sku} ->
        command =
          command!(%{
            operation: :insert,
            relation: :selecto_graph_items,
            assignments: [
              %{field: :tenant_id, value: {:literal, 7}},
              %{field: :sku, value: {:literal, sku}},
              %{field: :quantity, value: {:literal, 1}}
            ],
            returning: [:id]
          })

        %Row{
          id: row_id,
          path: [:items, String.to_integer(row_id)],
          command: command,
          bindings: [order_binding()]
        }
      end)

    graph!([
      root_node(root),
      %Node{
        id: "items",
        path: [:items],
        relation: :selecto_graph_items,
        strategy: :ordered,
        rows: child_rows
      }
    ])
  end

  defp sync_graph!(order_id, existing_item_id) do
    root =
      command!(%{
        operation: :update,
        relation: :selecto_graph_orders,
        assignments: [%{field: :reference, value: {:literal, "SO-100-R"}}],
        predicate:
          {:and,
           [
             {:eq, {:field, :id}, {:literal, order_id}},
             {:eq, {:field, :tenant_id}, {:literal, 7}}
           ]},
        returning: [:id],
        expected_cardinality: {:exactly, 1}
      })

    existing =
      command!(%{
        operation: :update,
        relation: :selecto_graph_items,
        assignments: [
          %{field: :sku, value: {:literal, "A-updated"}},
          %{field: :quantity, value: {:literal, 4}}
        ],
        predicate:
          {:and,
           [
             {:eq, {:field, :id}, {:literal, existing_item_id}},
             {:eq, {:field, :tenant_id}, {:literal, 7}}
           ]},
        returning: [:id],
        expected_cardinality: {:exactly, 1}
      })

    new =
      command!(%{
        operation: :insert,
        relation: :selecto_graph_items,
        assignments: [
          %{field: :tenant_id, value: {:literal, 7}},
          %{field: :sku, value: {:literal, "C"}},
          %{field: :quantity, value: {:literal, 1}}
        ],
        returning: [:id],
        expected_cardinality: {:exactly, 1}
      })

    sync_predicate =
      {:and,
       [
         {:eq, {:field, :order_id}, {:generated, "root", "root", :id}},
         {:eq, {:field, :tenant_id}, {:literal, 7}}
       ]}

    graph!([
      root_node(root),
      %Node{
        id: "items",
        path: [:items],
        relation: :selecto_graph_items,
        strategy: :sync,
        identity_fields: [:id],
        field_types: %{
          id: :integer,
          tenant_id: :integer,
          order_id: :integer,
          sku: :string,
          quantity: :integer
        },
        sync_predicate: sync_predicate,
        delete_missing?: true,
        rows: [
          %Row{
            id: "0",
            path: [:items, 0],
            command: existing,
            bindings: [order_binding()],
            metadata: %{identity: %{id: existing_item_id}}
          },
          %Row{
            id: "1",
            path: [:items, 1],
            command: new,
            bindings: [order_binding()],
            metadata: %{identity: %{}}
          }
        ]
      }
    ])
  end

  defp root_node(command) do
    %Node{
      id: "root",
      path: [],
      relation: :selecto_graph_orders,
      strategy: :ordered,
      rows: [%Row{id: "root", path: [], command: command}]
    }
  end

  defp order_binding do
    %Binding{field: :order_id, from_node: "root", from_row: "root", from_field: :id}
  end

  defp graph!(nodes) do
    {:ok, graph} = Graph.new(nodes, {"root", "root"})
    graph
  end

  defp command!(attrs) do
    {:ok, command} = Command.new(attrs)
    command
  end

  defp execute!(connection, sql, params \\ []) do
    {:ok, result} = Adapter.execute(connection, sql, params, [])
    result
  end
end
