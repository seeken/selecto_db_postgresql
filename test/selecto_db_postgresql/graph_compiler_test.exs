defmodule SelectoDBPostgreSQL.GraphCompilerTest do
  use ExUnit.Case, async: true

  alias Selecto.Write.{Command, Graph}
  alias Selecto.Write.Graph.{Binding, Node, Row}
  alias SelectoDBPostgreSQL.{Adapter, GraphCompiler}

  test "PostgreSQL 17 preview uses MERGE RETURNING for an eligible owned sync node" do
    graph = graph!()

    assert {:ok, preview} =
             Adapter.preview_write(:unused, graph, server_version_major: 17)

    assert length(preview.statements) == 2
    assert preview.metadata.merge_nodes == ["items"]

    merge = Enum.at(preview.statements, 1)
    assert merge.text =~ "MERGE INTO \"order_items\" AS target"
    assert merge.text =~ "WHEN MATCHED AND"
    assert merge.text =~ "WHEN NOT MATCHED"
    assert merge.text =~ "WHEN NOT MATCHED BY SOURCE"
    assert merge.text =~ "merge_action()"
    assert merge.text =~ "RETURNING"
    refute merge.text =~ "Repo"
  end

  test "PostgreSQL 15 and 16 use atomic ordered fallback when MERGE lacks required semantics" do
    for server_major <- [15, 16] do
      assert {:ok, preview} =
               Adapter.preview_write(:unused, graph!(), server_version_major: server_major)

      assert preview.metadata.merge_nodes == []
      assert length(preview.statements) == 4
      refute Enum.any?(preview.statements, &String.contains?(&1.text, "MERGE INTO"))

      assert Enum.at(preview.statements, 1).text =~ "UPDATE \"order_items\""
      assert Enum.at(preview.statements, 2).text =~ "INSERT INTO \"order_items\""
      assert Enum.at(preview.statements, 3).text =~ "DELETE FROM \"order_items\""
    end
  end

  test "MERGE result validation fails closed when any submitted source row had no action" do
    node = graph!().nodes |> Enum.at(1)

    query_result = %{
      columns: ["__selecto_row_id", "__selecto_action", "id"],
      rows: [["0", "UPDATE", 11]],
      num_rows: 1
    }

    assert {:error, %{type: :cardinality_mismatch, details: %{actual_row_ids: ["0"]}}} =
             GraphCompiler.merge_results(node, query_result)
  end

  test "an extra row predicate makes the node ineligible rather than silently weakening policy" do
    graph = graph!()
    node = Enum.at(graph.nodes, 1)
    [existing, new] = node.rows

    guarded = %{
      existing.command
      | predicate:
          {:and, [existing.command.predicate, {:eq, {:field, :state}, {:literal, "editable"}}]}
    }

    guarded_node = %{node | rows: [%{existing | command: guarded}, new]}

    refute GraphCompiler.merge_eligible?(guarded_node, 17)
  end

  test "direct adapter graph calls validate before preview or transaction dispatch" do
    malformed = %Graph{nodes: [:not_a_node], root: {"root", "root"}}

    assert {:error, %{type: :invalid_graph}} =
             Adapter.preview_write(:unused, malformed, server_version_major: 17)

    assert {:error, %{type: :invalid_graph}} =
             Adapter.execute_write(:unused, malformed, server_version_major: 17)
  end

  defp graph! do
    root =
      command!(%{
        operation: :insert,
        relation: :orders,
        assignments: [
          %{field: :tenant_id, value: {:literal, 7}},
          %{field: :reference, value: {:literal, "SO-100"}}
        ],
        returning: [:id]
      })

    existing =
      command!(%{
        operation: :update,
        relation: :order_items,
        assignments: [%{field: :quantity, value: {:literal, 3}}],
        predicate:
          {:and,
           [
             {:eq, {:field, :id}, {:literal, 11}},
             {:eq, {:field, :tenant_id}, {:literal, 7}}
           ]},
        returning: [:id],
        expected_cardinality: {:exactly, 1}
      })

    new =
      command!(%{
        operation: :insert,
        relation: :order_items,
        assignments: [
          %{field: :sku, value: {:literal, "NEW"}},
          %{field: :quantity, value: {:literal, 1}},
          %{field: :tenant_id, value: {:literal, 7}}
        ],
        returning: [:id],
        expected_cardinality: {:exactly, 1}
      })

    binding = %Binding{
      field: :order_id,
      from_node: "root",
      from_row: "root",
      from_field: :id
    }

    nodes = [
      %Node{
        id: "root",
        path: [],
        relation: :orders,
        strategy: :ordered,
        rows: [%Row{id: "root", path: [], command: root}]
      },
      %Node{
        id: "items",
        path: [:items],
        relation: :order_items,
        strategy: :sync,
        identity_fields: [:id],
        field_types: %{
          id: :integer,
          sku: :string,
          quantity: :integer,
          tenant_id: :integer,
          order_id: :integer
        },
        sync_predicate:
          {:and,
           [
             {:eq, {:field, :order_id}, {:generated, "root", "root", :id}},
             {:eq, {:field, :tenant_id}, {:literal, 7}}
           ]},
        delete_missing?: true,
        rows: [
          %Row{
            id: "0",
            path: [:items, 0],
            command: existing,
            bindings: [binding],
            metadata: %{identity: %{id: 11}}
          },
          %Row{
            id: "1",
            path: [:items, 1],
            command: new,
            bindings: [binding],
            metadata: %{identity: %{}}
          }
        ]
      }
    ]

    {:ok, graph} = Graph.new(nodes, {"root", "root"})
    graph
  end

  defp command!(attrs) do
    {:ok, command} = Command.new(attrs)
    command
  end
end
