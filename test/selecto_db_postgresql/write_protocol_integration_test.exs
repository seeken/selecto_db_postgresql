defmodule SelectoDBPostgreSQL.WriteProtocolIntegrationTest do
  use ExUnit.Case, async: false

  alias Selecto.Write.{Batch, Command, Error, Graph}
  alias Selecto.Write.Graph.{Node, Row}
  alias SelectoDBPostgreSQL.Adapter

  @moduletag :postgres

  @positions [first: 0, middle: 1, last: 2]

  setup do
    {:ok, connection} =
      Adapter.connect(SelectoDBPostgreSQL.Verification.ConnectionOptions.options())

    Process.unlink(connection)

    on_exit(fn ->
      if Process.alive?(connection), do: GenServer.stop(connection)
    end)

    execute!(connection, """
    CREATE TEMP TABLE selecto_protocol_atomicity (
      id integer PRIMARY KEY,
      state text NOT NULL CHECK (state <> 'forbidden')
    )
    """)

    execute!(connection, """
    INSERT INTO selecto_protocol_atomicity (id, state)
    VALUES (1, 'original-1'), (2, 'original-2'), (3, 'original-3')
    """)

    %{connection: connection}
  end

  for shape <- [:batch, :graph],
      fault <- [:execution_failed, :cardinality_mismatch],
      {position, index} <- @positions do
    test "#{shape} rolls back a #{fault} at the #{position} step", %{connection: connection} do
      shape = unquote(shape)
      fault = unquote(fault)
      index = unquote(index)

      write = write_plan(shape, fault, index)

      assert {:error, %Error{type: ^fault}} = Adapter.execute_write(connection, write)
      assert database_rows(connection) == original_rows()
    end
  end

  defp write_plan(:batch, fault, index) do
    commands = commands(fault, index)
    {:ok, batch} = Batch.new(commands)
    batch
  end

  defp write_plan(:graph, fault, index) do
    nodes =
      commands(fault, index)
      |> Enum.with_index()
      |> Enum.map(fn {command, node_index} ->
        node_id = "node-#{node_index}"

        %Node{
          id: node_id,
          path: [node_index],
          relation: :selecto_protocol_atomicity,
          strategy: :ordered,
          rows: [%Row{id: "row-#{node_index}", path: [node_index], command: command}]
        }
      end)

    {:ok, graph} = Graph.new(nodes, {"node-0", "row-0"})
    graph
  end

  defp commands(fault, fault_index) do
    Enum.map(0..2, fn index ->
      fault? = index == fault_index

      attrs = %{
        operation: :update,
        relation: :selecto_protocol_atomicity,
        assignments: [
          %{
            field: :state,
            value:
              {:literal,
               if(fault? and fault == :execution_failed,
                 do: "forbidden",
                 else: "changed-#{index + 1}"
               )}
          }
        ],
        predicate: {:eq, {:field, :id}, {:literal, index + 1}},
        expected_cardinality:
          if(fault? and fault == :cardinality_mismatch,
            do: {:exactly, 2},
            else: {:exactly, 1}
          )
      }

      {:ok, command} = Command.new(attrs)
      command
    end)
  end

  defp original_rows,
    do: [[1, "original-1"], [2, "original-2"], [3, "original-3"]]

  defp database_rows(connection) do
    {:ok, %{rows: rows}} =
      Adapter.execute(
        connection,
        "SELECT id, state FROM selecto_protocol_atomicity ORDER BY id",
        [],
        []
      )

    rows
  end

  defp execute!(connection, sql) do
    {:ok, result} = Adapter.execute(connection, sql, [], [])
    result
  end
end
