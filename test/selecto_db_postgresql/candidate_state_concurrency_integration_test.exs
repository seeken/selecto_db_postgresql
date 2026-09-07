defmodule SelectoDBPostgreSQL.CandidateStateConcurrencyIntegrationTest do
  use ExUnit.Case, async: false

  alias Selecto.Write.{CandidateRequest, CandidateState, Command, Result}
  alias SelectoDBPostgreSQL.Adapter

  @moduletag :postgres

  setup do
    options = SelectoDBPostgreSQL.Verification.ConnectionOptions.options()
    {:ok, first} = Adapter.connect(options)
    {:ok, second} = Adapter.connect(options)
    Process.unlink(first)
    Process.unlink(second)

    suffix = System.unique_integer([:positive])
    orders = "selecto_candidate_lock_orders_#{suffix}"
    items = "selecto_candidate_lock_items_#{suffix}"

    execute!(first, "CREATE TABLE #{orders} (id bigint PRIMARY KEY, reference text NOT NULL)")

    execute!(
      first,
      "CREATE TABLE #{items} (id bigint PRIMARY KEY, order_id bigint NOT NULL REFERENCES #{orders}(id), quantity integer NOT NULL)"
    )

    execute!(first, "INSERT INTO #{orders} (id, reference) VALUES (42, 'initial')")
    execute!(first, "INSERT INTO #{items} (id, order_id, quantity) VALUES (11, 42, 1)")

    on_exit(fn ->
      if Process.alive?(first) do
        execute!(first, "DROP TABLE IF EXISTS #{items}")
        execute!(first, "DROP TABLE IF EXISTS #{orders}")
      end

      for connection <- [first, second], Process.alive?(connection) do
        GenServer.stop(connection)
      end
    end)

    %{first: first, second: second, orders: orders, items: items}
  end

  test "serializes candidate writers and reloads state after the first commit", context do
    %{first: first, second: second, orders: orders, items: items} = context
    request = candidate_request(orders, items)
    test_pid = self()

    first_task =
      Task.async(fn ->
        Adapter.execute_prepared_write(first, fn loader ->
          assert {:ok, %CandidateState{rows: [%{"id" => 11, "quantity" => 1}]}} =
                   loader.(request)

          send(test_pid, :first_loaded)

          receive do
            :release_first -> :ok
          after
            2_000 -> flunk("first writer was not released")
          end

          {:ok, child_update(items, 4), %{writer: :first}}
        end)
      end)

    assert_receive :first_loaded

    second_task =
      Task.async(fn ->
        send(test_pid, :second_started)

        Adapter.execute_prepared_write(second, fn loader ->
          with {:ok, %CandidateState{} = state} <- loader.(request) do
            send(test_pid, {:second_loaded, state.rows})
            {:ok, parent_update(orders, "second"), %{writer: :second}}
          end
        end)
      end)

    assert_receive :second_started
    refute_receive {:second_loaded, _rows}, 150

    send(first_task.pid, :release_first)

    assert {:ok, %Result{operation: :update, affected_rows: 1}} = Task.await(first_task)
    assert_receive {:second_loaded, [%{"id" => 11, "quantity" => 4}]}, 2_000
    assert {:ok, %Result{operation: :update, affected_rows: 1}} = Task.await(second_task)

    assert {:ok, %{rows: [[4, "second"]]}} =
             Adapter.execute(
               first,
               "SELECT i.quantity, o.reference FROM #{items} i JOIN #{orders} o ON o.id = i.order_id WHERE o.id = 42",
               [],
               []
             )
  end

  defp candidate_request(orders, items) do
    %CandidateRequest{
      operation: :update,
      representation: :delta,
      relationship: "items",
      path: [:items],
      parent_command: parent_update(orders, "unused"),
      parent_key: :id,
      child_relation: items,
      child_key: :order_id,
      identity_fields: ["id"],
      fields: ["id", "quantity"],
      max_rows: 10
    }
  end

  defp parent_update(orders, reference) do
    command!(%{
      operation: :update,
      relation: orders,
      assignments: [%{field: :reference, value: {:literal, reference}}],
      predicate: {:eq, {:field, :id}, {:literal, 42}},
      returning: [:id],
      expected_cardinality: {:exactly, 1}
    })
  end

  defp child_update(items, quantity) do
    command!(%{
      operation: :update,
      relation: items,
      assignments: [%{field: :quantity, value: {:literal, quantity}}],
      predicate: {:eq, {:field, :id}, {:literal, 11}},
      returning: [:id],
      expected_cardinality: {:exactly, 1}
    })
  end

  defp command!(attrs) do
    {:ok, command} = Command.new(attrs)
    command
  end

  defp execute!(connection, statement) do
    assert {:ok, _result} = Adapter.execute(connection, statement, [], [])
  end
end
