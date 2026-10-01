defmodule SelectoDBPostgreSQL.AdversarialWriteIntegrationTest do
  @moduledoc """
  Real-PostgreSQL regressions for the adversarial write scenarios. Every
  attempt is followed by a read of every tenant's rows.
  """

  use ExUnit.Case, async: false

  alias Selecto.Write.{Command, Error, Result}
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
    CREATE TEMP TABLE adv_projects (
      id integer PRIMARY KEY,
      tenant_id integer NOT NULL,
      name text NOT NULL
    )
    """)

    execute!(connection, """
    CREATE TEMP TABLE adv_tasks (
      id integer PRIMARY KEY,
      tenant_id integer NOT NULL,
      project_id integer REFERENCES adv_projects (id),
      title text NOT NULL
    )
    """)

    execute!(connection, """
    CREATE TEMP TABLE adv_products (
      id integer PRIMARY KEY,
      tenant_id integer NOT NULL,
      sku text NOT NULL UNIQUE,
      name text NOT NULL,
      UNIQUE (tenant_id, sku)
    )
    """)

    execute!(connection, "INSERT INTO adv_projects VALUES (70, 7, 'p7'), (80, 8, 'p8')")
    execute!(connection, "INSERT INTO adv_tasks VALUES (1, 7, 70, 't7'), (2, 8, 80, 't8')")

    execute!(
      connection,
      "INSERT INTO adv_products VALUES (1, 7, 'A', 'seven'), (2, 8, 'tenant-8-secret', 'eight')"
    )

    %{connection: connection}
  end

  describe "S3 tenant-scoped foreign-key guards" do
    test "a tenant 7 insert cannot reference tenant 8's parent", %{connection: connection} do
      attack = task_insert(3, 80)

      assert {:error, %Error{type: :cardinality_mismatch}} =
               Adapter.execute_write_unsafe(connection, attack)

      assert task_rows(connection) == [[1, 7, 70, "t7"], [2, 8, 80, "t8"]]

      assert {:ok, %Result{affected_rows: 1}} =
               Adapter.execute_write_unsafe(connection, task_insert(3, 70))

      assert task_rows(connection) == [[1, 7, 70, "t7"], [2, 8, 80, "t8"], [3, 7, 70, "new"]]
    end

    test "a tenant 7 update cannot re-point its row at tenant 8's parent", %{
      connection: connection
    } do
      assert {:error, %Error{type: :cardinality_mismatch}} =
               Adapter.execute_write_unsafe(connection, task_update(1, 80))

      assert task_rows(connection) == [[1, 7, 70, "t7"], [2, 8, 80, "t8"]]

      execute!(connection, "INSERT INTO adv_projects VALUES (71, 7, 'p7b')")

      assert {:ok, %Result{affected_rows: 1}} =
               Adapter.execute_write_unsafe(connection, task_update(1, 71))

      assert task_rows(connection) == [[1, 7, 71, "t7"], [2, 8, 80, "t8"]]
    end

    test "a tenant column missing from the referenced relation fails instead of binding outward",
         %{connection: connection} do
      execute!(connection, "CREATE TEMP TABLE adv_untenanted (id integer PRIMARY KEY)")
      execute!(connection, "INSERT INTO adv_untenanted VALUES (80)")

      command =
        task_update(1, 80)
        |> Map.update!(:metadata, fn metadata ->
          Map.update!(metadata, :foreign_key_guards, fn [guard] ->
            [%{guard | relation: "adv_untenanted"}]
          end)
        end)

      assert {:error, %Error{}} = Adapter.execute_write_unsafe(connection, command)
      assert task_rows(connection) == [[1, 7, 70, "t7"], [2, 8, 80, "t8"]]
    end
  end

  describe "S9 upsert scope" do
    test "an undeclared conflict target is refused before any statement runs", %{
      connection: connection
    } do
      {:ok, command} =
        Command.new(%{
          operation: :upsert,
          relation: "adv_products",
          assignments: [
            %{field: :id, value: {:literal, 2}},
            %{field: :tenant_id, value: {:literal, 7}},
            %{field: :sku, value: {:literal, "Z"}},
            %{field: :name, value: {:literal, "hijack"}}
          ],
          metadata: %{
            conflict_target: [:id],
            declared_conflict_targets: [[:tenant_id, :sku]],
            upsert_update_fields: [:name]
          }
        })

      assert {:error, %Error{details: %{code: :undeclared_conflict_target}}} =
               Adapter.execute_write_unsafe(connection, command)

      assert product_rows(connection) == [
               [1, 7, "A", "seven"],
               [2, 8, "tenant-8-secret", "eight"]
             ]
    end

    test "a tenant-bearing conflict target never updates another tenant's row", %{
      connection: connection
    } do
      assert {:error, %Error{type: :native_constraint_violation, details: details}} =
               Adapter.execute_write_unsafe(connection, product_upsert("tenant-8-secret"))

      assert details.category == :unique_violation
      assert details.constraint == "adv_products_sku_key"
      refute inspect(details) =~ "tenant-8-secret"

      assert product_rows(connection) == [
               [1, 7, "A", "seven"],
               [2, 8, "tenant-8-secret", "eight"]
             ]

      assert {:ok, %Result{affected_rows: 1}} =
               Adapter.execute_write_unsafe(connection, product_upsert("A"))

      assert product_rows(connection) == [
               [1, 7, "A", "upserted"],
               [2, 8, "tenant-8-secret", "eight"]
             ]
    end
  end

  describe "S19 sanitized read errors" do
    test "a direct-connection query error exposes no server detail, SQL or parameters", %{
      connection: connection
    } do
      query = "INSERT INTO adv_products (id, tenant_id, sku, name) VALUES ($1, $2, $3, $4)"

      assert {:error, reason} =
               Adapter.execute(connection, query, [9, 7, "tenant-8-secret", "probe"], [])

      error = Selecto.AdapterSupport.normalize_error(Adapter, reason)

      assert %Selecto.Error{
               type: :query_error,
               query: nil,
               params: [],
               details: %{category: :unique_violation, sqlstate: "23505"}
             } = error

      refute inspect(error) =~ "tenant-8-secret"
      refute inspect(error) =~ "INSERT INTO"
    end

    test "a pool query exception exposes no SQL or parameters" do
      connection_options = SelectoDBPostgreSQL.Verification.ConnectionOptions.options()

      assert {:ok, pool_ref} =
               Selecto.ConnectionPool.start_pool(connection_options,
                 adapter: Adapter,
                 pool_size: 1,
                 max_overflow: 0
               )

      Process.unlink(pool_ref.pool)
      on_exit(fn -> Selecto.ConnectionPool.stop_pool(pool_ref) end)

      assert {:error, reason} =
               Adapter.execute(
                 {:pool, pool_ref},
                 "SELECT $1::integer AS secret_probe_column",
                 ["secret-param-value"],
                 []
               )

      error = Selecto.AdapterSupport.normalize_error(Adapter, reason)

      assert %Selecto.Error{query: nil, params: params} = error
      assert params in [nil, []]
      refute inspect(error) =~ "secret-param-value"
      refute inspect(error) =~ "secret_probe_column"
    end
  end

  defp task_insert(id, project_id) do
    {:ok, command} =
      Command.new(%{
        operation: :insert,
        relation: "adv_tasks",
        assignments: [
          %{field: "id", value: {:literal, id}},
          %{field: "title", value: {:literal, "new"}},
          %{field: "project_id", value: {:literal, project_id}},
          %{field: "tenant_id", value: {:literal, 7}}
        ],
        metadata: %{foreign_key_guards: [project_guard()]}
      })

    command
  end

  defp task_update(id, project_id) do
    {:ok, command} =
      Command.new(%{
        operation: :update,
        relation: "adv_tasks",
        assignments: [%{field: "project_id", value: {:literal, project_id}}],
        predicate:
          {:and,
           [
             {:eq, {:field, "id"}, {:literal, id}},
             {:eq, {:field, "tenant_id"}, {:literal, 7}}
           ]},
        metadata: %{foreign_key_guards: [project_guard()]}
      })

    command
  end

  defp project_guard do
    %{
      field: "project_id",
      relation: "adv_projects",
      target_field: "id",
      tenant_field: "tenant_id",
      tenant_value: 7
    }
  end

  defp product_upsert(sku) do
    {:ok, command} =
      Command.new(%{
        operation: :upsert,
        relation: "adv_products",
        assignments: [
          %{field: :id, value: {:literal, 3}},
          %{field: :tenant_id, value: {:literal, 7}},
          %{field: :sku, value: {:literal, sku}},
          %{field: :name, value: {:literal, "upserted"}}
        ],
        metadata: %{
          conflict_target: [:tenant_id, :sku],
          declared_conflict_targets: [[:tenant_id, :sku]],
          upsert_update_fields: [:name]
        }
      })

    command
  end

  defp task_rows(connection),
    do: rows(connection, "SELECT id, tenant_id, project_id, title FROM adv_tasks ORDER BY id")

  defp product_rows(connection),
    do: rows(connection, "SELECT id, tenant_id, sku, name FROM adv_products ORDER BY id")

  defp rows(connection, query) do
    %Postgrex.Result{rows: rows} = Postgrex.query!(connection, query, [])
    rows
  end

  defp execute!(connection, query), do: Postgrex.query!(connection, query, [])
end
