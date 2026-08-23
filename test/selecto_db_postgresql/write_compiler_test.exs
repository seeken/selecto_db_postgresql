defmodule SelectoDBPostgreSQL.WriteCompilerTest do
  use ExUnit.Case, async: true

  alias Selecto.Write.{Batch, Command, Error}
  alias SelectoDBPostgreSQL.Adapter

  defmodule EctoTransactionProbeRepo do
    def __adapter__, do: Ecto.Adapters.Postgres

    def transaction(fun, opts) do
      send(self(), {:ecto_transaction, opts})

      try do
        {:ok, fun.()}
      catch
        {:rollback, reason} -> {:error, reason}
      end
    end

    def rollback(reason), do: throw({:rollback, reason})
  end

  test "previews a parameterized insert without any ORM or application configuration" do
    command = command!(:insert, returning: [:id])

    assert {:ok, %{statements: [%{text: sql, params: ["new item", 7]}]}} =
             Adapter.preview_write(:unused, command, context: %{tenant_id: 7})

    assert sql ==
             "INSERT INTO \"items\" (\"name\", \"tenant_id\") VALUES ($1, $2) RETURNING \"id\""
  end

  test "requires a predicate for updates and compiles tenant context as a bound parameter" do
    command = command!(:update)

    assert {:ok, %{statements: [%{text: sql, params: ["renamed", 7]}]}} =
             Adapter.preview_write(:unused, command, context: %{tenant_id: 7})

    assert sql == "UPDATE \"items\" SET \"name\" = $1 WHERE \"tenant_id\" = $2"
  end

  test "compiles a non-empty IN predicate with one parameter per target id" do
    {:ok, command} =
      Command.new(%{
        operation: :update,
        relation: :items,
        assignments: [%{field: :state, value: {:literal, "archived"}}],
        predicate:
          {:and,
           [
             {:in, {:field, :id}, [{:literal, 41}, {:literal, 42}, {:literal, 43}]},
             {:eq, {:field, :state}, {:literal, "done"}}
           ]},
        expected_cardinality: {:exactly, 3}
      })

    assert {:ok, %{statements: [%{text: sql, params: params}]}} =
             Adapter.preview_write(:unused, command)

    assert sql ==
             "UPDATE \"items\" SET \"state\" = $1 WHERE (\"id\" IN ($2, $3, $4) AND \"state\" = $5)"

    assert params == ["archived", 41, 42, 43, "done"]
  end

  test "compiles the portable system-now token as an adapter-owned timestamp expression" do
    {:ok, command} =
      Command.new(%{
        operation: :update,
        relation: :items,
        assignments: [
          %{field: :state, value: {:literal, "archived"}},
          %{field: :archived_at, value: {:literal, {:system, :now}}}
        ],
        predicate: {:eq, {:field, :id}, {:literal, 41}}
      })

    assert {:ok, %{statements: [%{text: sql, params: params}]}} =
             Adapter.preview_write(:unused, command)

    assert sql ==
             "UPDATE \"items\" SET \"state\" = $1, \"archived_at\" = CURRENT_TIMESTAMP WHERE \"id\" = $2"

    assert params == ["archived", 41]
  end

  test "does not leave parameter gaps when expressions appear between bound assignments" do
    {:ok, command} =
      Command.new(%{
        operation: :insert,
        relation: :items,
        assignments: [
          %{field: :name, value: {:literal, "new item"}},
          %{field: :inserted_at, value: {:literal, {:system, :now}}},
          %{field: :quantity, value: {:literal, 2}}
        ]
      })

    assert {:ok, %{statements: [%{text: sql, params: ["new item", 2]}]}} =
             Adapter.preview_write(:unused, command)

    assert sql ==
             "INSERT INTO \"items\" (\"name\", \"inserted_at\", \"quantity\") VALUES ($1, CURRENT_TIMESTAMP, $2)"
  end

  test "fails closed for missing tenant context and raw SQL" do
    assert {:error, %Error{type: :missing_context}} =
             Adapter.preview_write(:unused, command!(:update))

    assert {:error, %Error{type: :invalid_command}} =
             Command.new(%{
               operation: :delete,
               relation: :items,
               predicate: {:unsafe_sql, "tenant_id = 7"}
             })
  end

  test "public write entrypoints validate malformed command structs before dispatch" do
    malformed = %Command{
      operation: :insert,
      relation: :items,
      assignments: [:not_an_assignment]
    }

    assert {:error, %Error{type: :invalid_command}} =
             Adapter.preview_write(:unused, malformed)

    assert {:error, %Error{type: :invalid_command}} =
             Adapter.execute_write(:unregistered_connection, malformed)

    assert {:error, %Error{type: :invalid_command}} =
             Adapter.preview_write(:unused, %{operation: :insert})

    assert {:error, %Error{type: :invalid_command}} =
             Adapter.execute_write(:unregistered_connection, %{operation: :insert})
  end

  test "rejects assignment fields that collide after identifier normalization" do
    duplicate = %Command{
      operation: :insert,
      relation: :items,
      assignments: [
        %{field: :name, value: {:literal, "first"}},
        %{field: "name", value: {:literal, "second"}}
      ]
    }

    assert {:error,
            %Error{
              type: :invalid_command,
              details: %{code: :duplicate_assignment_identifier, fields: ["name"]}
            }} =
             Adapter.preview_write(:unused, duplicate)

    batch = %Batch{commands: [duplicate]}

    assert {:error,
            %Error{type: :invalid_command, details: %{code: :duplicate_assignment_identifier}}} =
             Adapter.preview_write(:unused, batch)

    assert {:error,
            %Error{type: :invalid_command, details: %{code: :duplicate_assignment_identifier}}} =
             Adapter.execute_write(:unregistered_connection, batch)
  end

  test "rejects returning fields that collide after identifier normalization" do
    duplicate = %Command{
      operation: :insert,
      relation: :items,
      assignments: [%{field: :name, value: {:literal, "item"}}],
      returning: [:id, "id"]
    }

    assert {:error,
            %Error{
              type: :invalid_command,
              details: %{code: :duplicate_returning_identifier, fields: ["id"]}
            }} =
             Adapter.preview_write(:unused, duplicate)
  end

  test "advertises native PostgreSQL write capability without Ecto" do
    capabilities = Adapter.write_capabilities(:unused)

    assert capabilities.dialect == :postgresql
    assert capabilities.atomic_batch
    assert capabilities.returning
  end

  test "routes Ecto Repo writes through the Repo transaction boundary" do
    assert {:error, %Error{}} =
             Adapter.execute_write(EctoTransactionProbeRepo, command!(:update),
               context: %{tenant_id: 7}
             )

    assert_receive {:ecto_transaction, []}
  end

  test "previews a portable upsert with domain-governed conflict and update fields" do
    {:ok, command} =
      Command.new(%{
        operation: :upsert,
        relation: :items,
        assignments: [
          %{field: :external_id, value: {:literal, "external-1"}},
          %{field: :name, value: {:literal, "new item"}}
        ],
        metadata: %{conflict_target: [:external_id], upsert_update_fields: [:name]}
      })

    assert {:ok, %{statements: [%{text: sql, params: ["external-1", "new item"]}]}} =
             Adapter.preview_write(:unused, command)

    assert sql ==
             "INSERT INTO \"items\" (\"external_id\", \"name\") VALUES ($1, $2) ON CONFLICT (\"external_id\") DO UPDATE SET \"name\" = EXCLUDED.\"name\""
  end

  test "fails closed when an upsert lacks a domain-governed update field list" do
    {:ok, command} =
      Command.new(%{
        operation: :upsert,
        relation: :items,
        assignments: [%{field: :external_id, value: {:literal, "external-1"}}],
        metadata: %{conflict_target: [:external_id]}
      })

    assert {:error, %Error{type: :invalid_command, details: %{required: required}}} =
             Adapter.preview_write(:unused, command)

    assert required == :upsert_update_fields
  end

  test "compiles an empty governed upsert update set as DO NOTHING" do
    {:ok, command} =
      Command.new(%{
        operation: :upsert,
        relation: :items,
        assignments: [%{field: :external_id, value: {:literal, "external-1"}}],
        metadata: %{conflict_target: [:external_id], upsert_update_fields: []}
      })

    assert {:ok, %{statements: [%{text: sql}]}} = Adapter.preview_write(:unused, command)

    assert sql ==
             "INSERT INTO \"items\" (\"external_id\") VALUES ($1) ON CONFLICT (\"external_id\") DO NOTHING"
  end

  test "rejects governed upsert update fields that were not assigned" do
    {:ok, command} =
      Command.new(%{
        operation: :upsert,
        relation: :items,
        assignments: [%{field: :external_id, value: {:literal, "external-1"}}],
        metadata: %{
          conflict_target: [:external_id],
          upsert_update_fields: [:name]
        }
      })

    assert {:error, %Error{type: :invalid_command, details: %{reason: :field_not_assigned}}} =
             Adapter.preview_write(:unused, command)
  end

  test "compiles domain-provided foreign-key guards as parameterized reference checks" do
    {:ok, command} =
      Command.new(%{
        operation: :insert,
        relation: :items,
        assignments: [
          %{field: :name, value: {:literal, "new item"}},
          %{field: :account_id, value: {:literal, 42}}
        ],
        metadata: %{
          foreign_key_guards: [
            %{field: :account_id, relation: "accounts", target_field: :id}
          ]
        }
      })

    assert {:ok, %{statements: [%{text: sql, params: ["new item", 42, 42]}]}} =
             Adapter.preview_write(:unused, command)

    assert sql ==
             "INSERT INTO \"items\" (\"name\", \"account_id\") SELECT $1, $2 WHERE EXISTS (SELECT 1 FROM \"accounts\" WHERE \"id\" = $3)"
  end

  defp command!(:insert, opts) do
    {:ok, command} =
      Command.new(%{
        operation: :insert,
        relation: :items,
        assignments: [
          %{field: :name, value: {:literal, "new item"}},
          %{field: :tenant_id, value: {:context, :tenant_id}}
        ],
        returning: Keyword.get(opts, :returning, :none)
      })

    command
  end

  defp command!(:update) do
    {:ok, command} =
      Command.new(%{
        operation: :update,
        relation: :items,
        assignments: [%{field: :name, value: {:literal, "renamed"}}],
        predicate: {:eq, {:field, :tenant_id}, {:context, :tenant_id}}
      })

    command
  end
end
