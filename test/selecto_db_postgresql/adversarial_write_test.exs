defmodule SelectoDBPostgreSQL.AdversarialWriteTest do
  @moduledoc """
  Compiler-level regressions for the adversarial write scenarios (S3, S9,
  S19). The database-backed counterparts live in
  `SelectoDBPostgreSQL.AdversarialWriteIntegrationTest`.
  """

  use ExUnit.Case, async: true

  alias Selecto.Write.{Command, Error}
  alias SelectoDBPostgreSQL.Adapter

  describe "S3 tenant-scoped foreign-key guards" do
    test "a guard with a tenant field also binds the referenced row's tenant" do
      command =
        command!(:insert, [%{field: :project_id, value: {:literal, 80}}], %{
          foreign_key_guards: [
            %{
              field: "project_id",
              relation: "projects",
              target_field: "id",
              tenant_field: "tenant_id",
              tenant_value: 7
            }
          ]
        })

      assert {:ok, %{statements: [%{text: sql, params: [80, 80, 7]}]}} =
               Adapter.preview_write(:unused, command)

      assert sql ==
               "INSERT INTO \"tasks\" (\"project_id\") SELECT $1 WHERE EXISTS " <>
                 "(SELECT 1 FROM \"projects\" AS \"selecto_fk_parent\" " <>
                 "WHERE \"selecto_fk_parent\".\"id\" = $2 " <>
                 "AND \"selecto_fk_parent\".\"tenant_id\" = $3)"
    end

    test "an update guard carries its tenant after the predicate parameters" do
      command =
        command!(:update, [%{field: :project_id, value: {:literal, 80}}], %{
          foreign_key_guards: [
            %{
              field: "project_id",
              relation: "projects",
              target_field: "id",
              tenant_field: "tenant_id",
              tenant_value: 7
            }
          ]
        })

      assert {:ok, %{statements: [%{text: sql, params: [80, 1, 7, 80, 7]}]}} =
               Adapter.preview_write(:unused, command)

      assert sql =~
               ~s|WHERE ("id" = $2 AND "tenant_id" = $3) AND EXISTS (SELECT 1 FROM "projects" AS "selecto_fk_parent" WHERE "selecto_fk_parent"."id" = $4 AND "selecto_fk_parent"."tenant_id" = $5)|
    end

    test "a tenant guard without a tenant value fails closed" do
      for guard <- [
            %{field: "project_id", relation: "projects", target_field: "id", tenant_field: "t"},
            %{
              field: "project_id",
              relation: "projects",
              target_field: "id",
              tenant_field: "t",
              tenant_value: nil
            },
            %{
              field: "project_id",
              relation: "projects",
              target_field: "id",
              tenant_field: 7,
              tenant_value: 7
            }
          ] do
        command =
          command!(:insert, [%{field: :project_id, value: {:literal, 80}}], %{
            foreign_key_guards: [guard]
          })

        assert {:error, %Error{type: :invalid_foreign_key_guard}} =
                 Adapter.preview_write(:unused, command)
      end
    end
  end

  describe "S9 upsert scope" do
    test "the conflict target must be one of the published declared targets" do
      undeclared =
        upsert!(%{
          conflict_target: [:id],
          declared_conflict_targets: [[:tenant_id, :sku]],
          upsert_update_fields: [:name]
        })

      assert {:error,
              %Error{
                type: :invalid_command,
                details: %{code: :undeclared_conflict_target, conflict_target: [:id]}
              }} = Adapter.preview_write(:unused, undeclared)

      reordered =
        upsert!(%{
          conflict_target: [:sku, :tenant_id],
          declared_conflict_targets: [[:tenant_id, :sku]],
          upsert_update_fields: [:name]
        })

      assert {:ok, %{statements: [%{text: sql}]}} = Adapter.preview_write(:unused, reordered)
      assert sql =~ ~s|ON CONFLICT ("sku", "tenant_id") DO UPDATE SET "name" = EXCLUDED."name"|

      unpublished = upsert!(%{conflict_target: [:id], upsert_update_fields: [:name]})
      assert {:ok, _preview} = Adapter.preview_write(:unused, unpublished)
    end

    test "an upsert command predicate is refused rather than silently dropped" do
      {:ok, command} =
        Command.new(%{
          operation: :upsert,
          relation: :products,
          assignments: upsert_assignments(),
          predicate: {:eq, {:field, :tenant_id}, {:literal, 7}},
          metadata: %{conflict_target: [:tenant_id, :sku], upsert_update_fields: [:name]}
        })

      assert {:error,
              %Error{type: :invalid_command, details: %{code: :upsert_predicate_unsupported}}} =
               Adapter.preview_write(:unused, command)
    end
  end

  describe "S19 sanitized read errors" do
    test "a PostgreSQL error keeps its category and code but not its message, detail or query" do
      error = %Postgrex.Error{
        message: nil,
        postgres: %{
          code: :unique_violation,
          pg_code: "23505",
          severity: "ERROR",
          message: "duplicate key value violates unique constraint \"products_sku_key\"",
          detail: "Key (sku)=(tenant-8-secret) already exists.",
          constraint: "products_sku_key",
          table: "products"
        },
        query: "INSERT INTO products (sku) VALUES ($1)"
      }

      normalized = Adapter.normalize_error(error)

      assert %Selecto.Error{
               type: :query_error,
               message: "PostgreSQL rejected the query: unique constraint violated",
               query: nil,
               params: [],
               details: %{
                 adapter: :postgresql,
                 category: :unique_violation,
                 code: :unique_violation,
                 sqlstate: "23505",
                 constraint: "products_sku_key"
               }
             } = normalized

      refute inspect(normalized) =~ "tenant-8-secret"
      refute inspect(normalized) =~ "INSERT INTO"
    end

    test "query exceptions and connection exits are reduced to a category" do
      assert %Selecto.Error{
               message: "PostgreSQL query failed",
               details: %{category: :query_exception, exception: "DBConnection.EncodeError"}
             } =
               error =
               Adapter.normalize_error(
                 {:query_exception, DBConnection.EncodeError,
                  "Postgrex expected an integer, got \"secret-param\""}
               )

      refute inspect(error) =~ "secret-param"

      assert %Selecto.Error{type: :connection_error, details: details} =
               Adapter.normalize_error({:connection_exit, {:timeout, {:call, ["SELECT secret"]}}})

      refute inspect(details) =~ "secret"
    end
  end

  defp command!(operation, assignments, metadata) do
    attrs = %{
      operation: operation,
      relation: :tasks,
      assignments: assignments,
      metadata: metadata
    }

    attrs =
      if operation == :update,
        do:
          Map.put(
            attrs,
            :predicate,
            {:and,
             [{:eq, {:field, :id}, {:literal, 1}}, {:eq, {:field, :tenant_id}, {:literal, 7}}]}
          ),
        else: attrs

    {:ok, command} = Command.new(attrs)
    command
  end

  defp upsert!(metadata) do
    {:ok, command} =
      Command.new(%{
        operation: :upsert,
        relation: :products,
        assignments: upsert_assignments(),
        metadata: metadata
      })

    command
  end

  defp upsert_assignments do
    [
      %{field: :id, value: {:literal, 3}},
      %{field: :tenant_id, value: {:literal, 7}},
      %{field: :sku, value: {:literal, "X"}},
      %{field: :name, value: {:literal, "n"}}
    ]
  end
end
