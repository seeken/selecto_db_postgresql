defmodule SelectoDBPostgreSQL.WriteRollbackIntegrationTest do
  use ExUnit.Case, async: false

  alias Selecto.Write.{Command, Error}
  alias SelectoDBPostgreSQL.Adapter

  @moduletag :postgres

  setup do
    {:ok, connection} =
      Adapter.connect(SelectoDBPostgreSQL.Verification.ConnectionOptions.options())

    Process.unlink(connection)

    on_exit(fn ->
      if Process.alive?(connection), do: GenServer.stop(connection)
    end)

    {:ok, _} =
      Adapter.execute(
        connection,
        "CREATE TEMP TABLE selecto_write_rollback (id integer PRIMARY KEY, tenant_id integer NOT NULL, state text NOT NULL)",
        [],
        []
      )

    {:ok, _} =
      Adapter.execute(
        connection,
        "INSERT INTO selecto_write_rollback (id, tenant_id, state) VALUES (1, 7, 'done'), (2, 7, 'open')",
        [],
        []
      )

    {:ok, _} =
      Adapter.execute(
        connection,
        "CREATE TEMP TABLE selecto_committed_effects (item_id integer NOT NULL, state text NOT NULL)",
        [],
        []
      )

    %{connection: connection}
  end

  test "cardinality mismatch rolls back every tentative mutation", %{connection: connection} do
    {:ok, command} =
      Command.new(%{
        operation: :update,
        relation: :selecto_write_rollback,
        assignments: [%{field: :state, value: {:literal, "archived"}}],
        predicate:
          {:and,
           [
             {:in, {:field, :id}, [{:literal, 1}, {:literal, 2}]},
             {:eq, {:field, :tenant_id}, {:literal, 7}},
             {:eq, {:field, :state}, {:literal, "done"}}
           ]},
        expected_cardinality: {:exactly, 2}
      })

    assert {:error,
            %Error{
              type: :cardinality_mismatch,
              details: %{actual: 1, expected: {:exactly, 2}}
            }} = Adapter.execute_write(connection, command)

    assert {:ok, %{rows: [[1, "done"], [2, "open"]]}} =
             Adapter.execute(
               connection,
               "SELECT id, state FROM selecto_write_rollback ORDER BY id",
               [],
               []
             )
  end

  test "a committed-effect sink writes through the active transaction connection", %{
    connection: connection
  } do
    command = update_state_command!("archived")

    sink = fn transaction_connection, result, %{write: ^command} ->
      assert %DBConnection{} = transaction_connection
      assert result.affected_rows == 1

      assert {:ok, _query} =
               Postgrex.query(
                 transaction_connection,
                 "INSERT INTO selecto_committed_effects (item_id, state) VALUES ($1, $2)",
                 [1, "archived"]
               )

      :ok
    end

    assert {:ok, %{affected_rows: 1}} =
             Adapter.execute_write(connection, command, committed_effect_sink: sink)

    assert {:ok, %{rows: [["archived"]]}} =
             Adapter.execute(
               connection,
               "SELECT state FROM selecto_write_rollback WHERE id = 1",
               [],
               []
             )

    assert {:ok, %{rows: [[1, "archived"]]}} =
             Adapter.execute(
               connection,
               "SELECT item_id, state FROM selecto_committed_effects",
               [],
               []
             )
  end

  test "a committed-effect failure rolls back both the mutation and tentative effect", %{
    connection: connection
  } do
    command = update_state_command!("archived")

    sink = fn transaction_connection, _result, _context ->
      assert {:ok, _query} =
               Postgrex.query(
                 transaction_connection,
                 "INSERT INTO selecto_committed_effects (item_id, state) VALUES ($1, $2)",
                 [1, "must roll back"]
               )

      {:error, :forced_failure}
    end

    assert {:error, %Error{type: :committed_effect_failed}} =
             Adapter.execute_write(connection, command, committed_effect_sink: sink)

    assert {:ok, %{rows: [["done"]]}} =
             Adapter.execute(
               connection,
               "SELECT state FROM selecto_write_rollback WHERE id = 1",
               [],
               []
             )

    assert {:ok, %{rows: [[0]]}} =
             Adapter.execute(
               connection,
               "SELECT count(*) FROM selecto_committed_effects",
               [],
               []
             )
  end

  defp update_state_command!(state) do
    {:ok, command} =
      Command.new(%{
        operation: :update,
        relation: :selecto_write_rollback,
        assignments: [%{field: :state, value: {:literal, state}}],
        predicate: {:eq, {:field, :id}, {:literal, 1}},
        expected_cardinality: {:exactly, 1},
        returning: [:id, :state]
      })

    command
  end
end
