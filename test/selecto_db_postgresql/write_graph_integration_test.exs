defmodule SelectoDBPostgreSQL.WriteGraphIntegrationTest do
  use ExUnit.Case, async: false

  alias Selecto.Write.{
    CandidateRequest,
    CandidateState,
    Command,
    Error,
    Graph,
    RecordRequest,
    RecordState
  }

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

    assert [
             %{path: [:items, 0], operation: :create, identity: %{"id" => first_created_id}},
             %{path: [:items, 1], operation: :create, identity: %{"id" => second_created_id}}
           ] = insert_result.metadata.nested_outcomes

    assert [
             %{client_identity: "new-0", identity: %{"id" => mapped_first_id}},
             %{client_identity: "new-1", identity: %{"id" => mapped_second_id}}
           ] = insert_result.metadata.identity_mappings

    assert {mapped_first_id, mapped_second_id} == {first_created_id, second_created_id}

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

    assert [
             %{path: [:items, 0], operation: :update, identity: %{"id" => ^first_id}},
             %{path: [:items, 1], operation: :create, identity: %{"id" => created_id}}
           ] = sync_result.metadata.nested_outcomes

    assert [%{client_identity: "new-C", identity: %{"id" => mapped_created_id}}] =
             sync_result.metadata.identity_mappings

    assert mapped_created_id == created_id

    assert {:ok, %{rows: [[^first_id, "A-updated", 4], [new_id, "C", 1]]}} =
             Adapter.execute(
               connection,
               "SELECT id, sku, quantity FROM selecto_graph_items WHERE order_id = $1 ORDER BY id",
               [order_id],
               []
             )

    assert new_id != second_id
    assert new_id == created_id
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

  test "returns a sanitized native constraint error for a duplicate write", %{
    connection: connection
  } do
    execute!(
      connection,
      "ALTER TABLE selecto_graph_orders ADD CONSTRAINT graph_orders_reference_key UNIQUE (reference)"
    )

    assert {:ok, _result} = Adapter.execute_write(connection, insert_graph!())

    assert {:error,
            %Error{
              type: :native_constraint_violation,
              details: %{
                adapter: :postgresql,
                write_stage: :execution_failed,
                category: :unique_violation,
                constraint: "graph_orders_reference_key",
                recoverable?: true
              }
            }} = Adapter.execute_write(connection, insert_graph!())
  end

  test "loads protected candidate state and executes the prepared write in one transaction", %{
    connection: connection
  } do
    assert {:ok, insert_result} = Adapter.execute_write(connection, insert_graph!())
    [%{"id" => order_id}] = insert_result.rows

    parent = parent_update!(order_id, "SO-100-PREPARED")
    request = candidate_request(parent)
    test_pid = self()

    prepare = fn loader ->
      assert {:ok,
              %CandidateState{
                complete?: true,
                protection: :locked,
                rows: rows,
                revision: %{parent_id: ^order_id}
              }} = loader.(request)

      send(test_pid, {:candidate_rows, rows})
      {:ok, parent, %{candidate_loaded?: true}}
    end

    assert {:ok, %Selecto.Write.Result{operation: :update, affected_rows: 1}} =
             Adapter.execute_prepared_write(connection, prepare)

    assert_receive {:candidate_rows,
                    [
                      %{"id" => _, "quantity" => 1, "sku" => "A"},
                      %{
                        "id" => _,
                        "quantity" => 1,
                        "sku" => "B"
                      }
                    ]}

    assert {:ok, %{rows: [["SO-100-PREPARED"]]}} =
             Adapter.execute(
               connection,
               "SELECT reference FROM selecto_graph_orders WHERE id = $1",
               [order_id],
               []
             )
  end

  test "loads one protected root record for a prepared partial update", %{connection: connection} do
    assert {:ok, insert_result} = Adapter.execute_write(connection, insert_graph!())
    [%{"id" => order_id}] = insert_result.rows

    parent = parent_update!(order_id, "SO-100-ROOT-PREPARED")

    request = %RecordRequest{
      operation: :update,
      relation: :selecto_graph_orders,
      predicate: parent.predicate,
      fields: ["id", "reference", "tenant_id"]
    }

    prepare = fn loader ->
      assert {:ok,
              %RecordState{
                complete?: true,
                protection: :locked,
                values: %{"id" => ^order_id, "reference" => "SO-100", "tenant_id" => 7}
              }} = loader.(request)

      {:ok, parent, %{record_loaded?: true}}
    end

    assert {:ok, %Selecto.Write.Result{operation: :update, affected_rows: 1}} =
             Adapter.execute_prepared_write(connection, prepare)
  end

  test "candidate overflow rejects and rolls back the prepared write", %{connection: connection} do
    assert {:ok, insert_result} = Adapter.execute_write(connection, insert_graph!())
    [%{"id" => order_id}] = insert_result.rows
    parent = parent_update!(order_id, "MUST-ROLL-BACK")
    request = %{candidate_request(parent) | max_rows: 1}

    assert {:error, %Error{type: :candidate_state_limit_exceeded}} =
             Adapter.execute_prepared_write(connection, fn loader ->
               with {:ok, _state} <- loader.(request), do: {:ok, parent, %{}}
             end)

    assert {:ok, %{rows: [["SO-100"]]}} =
             Adapter.execute(
               connection,
               "SELECT reference FROM selecto_graph_orders WHERE id = $1",
               [order_id],
               []
             )
  end

  test "prepared governed graph locks its root before a membership sync", %{
    connection: connection
  } do
    assert {:ok, insert_result} = Adapter.execute_write(connection, insert_graph!())
    [%{"id" => order_id}] = insert_result.rows

    assert {:ok, %{rows: [[first_id]]}} =
             Adapter.execute(
               connection,
               "SELECT id FROM selecto_graph_items WHERE order_id = $1 ORDER BY id LIMIT 1",
               [order_id],
               []
             )

    graph =
      sync_graph!(order_id, first_id)
      |> Map.update!(:metadata, &Map.put(&1, :membership_parent_lock, %{parent_key: :id}))

    assert {:ok, %Selecto.Write.Result{operation: :graph}} =
             Adapter.execute_prepared_write(connection, fn _candidate_loader ->
               {:ok, graph, %{governed: true}}
             end)

    assert {:ok, %{rows: [["SO-100-R"]]}} =
             Adapter.execute(
               connection,
               "SELECT reference FROM selecto_graph_orders WHERE id = $1",
               [order_id],
               []
             )
  end

  defp parent_update!(order_id, reference) do
    command!(%{
      operation: :update,
      relation: :selecto_graph_orders,
      assignments: [%{field: :reference, value: {:literal, reference}}],
      predicate:
        {:and,
         [
           {:eq, {:field, :id}, {:literal, order_id}},
           {:eq, {:field, :tenant_id}, {:literal, 7}}
         ]},
      returning: [:id],
      expected_cardinality: {:exactly, 1}
    })
  end

  defp candidate_request(parent) do
    %CandidateRequest{
      operation: :update,
      representation: :delta,
      relationship: "items",
      path: [:items],
      parent_command: parent,
      parent_key: :id,
      child_relation: :selecto_graph_items,
      child_key: :order_id,
      identity_fields: ["id"],
      fields: ["id", "sku", "quantity"],
      max_rows: 1_000
    }
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
          bindings: [order_binding()],
          metadata: %{client_identity: "new-#{row_id}", semantic_operation: :create}
        }
      end)

    graph!([
      root_node(root),
      %Node{
        id: "items",
        path: [:items],
        relation: :selecto_graph_items,
        strategy: :ordered,
        identity_fields: [:id],
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
            metadata: %{identity: %{id: existing_item_id}, semantic_operation: :update}
          },
          %Row{
            id: "1",
            path: [:items, 1],
            command: new,
            bindings: [order_binding()],
            metadata: %{identity: %{}, client_identity: "new-C", semantic_operation: :create}
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
