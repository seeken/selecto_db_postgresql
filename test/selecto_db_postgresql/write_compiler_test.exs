defmodule SelectoDBPostgreSQL.WriteCompilerTest do
  use ExUnit.Case, async: true

  alias Selecto.Write.{Command, Error}
  alias SelectoDBPostgreSQL.Adapter

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

  test "advertises native PostgreSQL write capability without Ecto" do
    capabilities = Adapter.write_capabilities(:unused)

    assert capabilities.dialect == :postgresql
    assert capabilities.atomic_batch
    assert capabilities.returning
  end

  test "previews a portable upsert with an explicit conflict target" do
    {:ok, command} =
      Command.new(%{
        operation: :upsert,
        relation: :items,
        assignments: [
          %{field: :external_id, value: {:literal, "external-1"}},
          %{field: :name, value: {:literal, "new item"}}
        ],
        metadata: %{conflict_target: [:external_id]}
      })

    assert {:ok, %{statements: [%{text: sql, params: ["external-1", "new item"]}]}} =
             Adapter.preview_write(:unused, command)

    assert sql ==
             "INSERT INTO \"items\" (\"external_id\", \"name\") VALUES ($1, $2) ON CONFLICT (\"external_id\") DO UPDATE SET \"external_id\" = EXCLUDED.\"external_id\", \"name\" = EXCLUDED.\"name\""
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
