defmodule SelectoDBPostgreSQL.FunctionVerificationIntegrationTest do
  use ExUnit.Case, async: false

  alias Selecto.FunctionVerification.Request
  alias Selecto.FunctionVerification.Suite
  alias SelectoDBPostgreSQL.Adapter

  @moduletag :postgres

  @scalar_name "selecto_verify_must_not_execute"
  @predicate_name "selecto_verify_positive"
  @table_name "selecto_verify_rows"
  @restricted_role "selecto_function_verification_no_execute"

  setup do
    {:ok, connection} =
      Adapter.connect(SelectoDBPostgreSQL.Verification.ConnectionOptions.options())

    Process.unlink(connection)

    on_exit(fn ->
      if Process.alive?(connection), do: GenServer.stop(connection)
    end)

    create_functions(connection)
    create_restricted_role(connection)

    on_exit(fn ->
      if Process.alive?(connection) do
        execute!(connection, "RESET ROLE")
        drop_functions(connection)
        execute!(connection, "DROP ROLE IF EXISTS #{@restricted_role}")
      end
    end)

    %{connection: connection}
  end

  test "resolves built-in overloads and reports live missing requirements and signatures", %{
    connection: connection
  } do
    lower = scalar_request("lower", "pg_catalog.lower", :string, :string)
    abs_integer = scalar_request("abs_integer", "pg_catalog.abs", :integer, :integer)
    abs_float = scalar_request("abs_float", "pg_catalog.abs", :float, :float)

    assert {:ok, lower_report} = Adapter.verify_function(connection, lower, [])
    assert lower_report.status == :database_resolved
    assert lower_report.resolved_identity =~ "pg_catalog.lower(text)"

    assert {:ok, integer_report} = Adapter.verify_function(connection, abs_integer, [])
    assert integer_report.status == :database_resolved
    assert integer_report.resolved_identity =~ "pg_catalog.abs(integer)"

    assert {:ok, float_report} = Adapter.verify_function(connection, abs_float, [])
    assert float_report.status == :database_resolved
    assert float_report.resolved_identity =~ "pg_catalog.abs(double precision)"

    refute integer_report.resolved_identity == float_report.resolved_identity

    missing_extension =
      scalar_request("lower_missing_extension", "pg_catalog.lower", :string, :string,
        database: %{requires: [extension: "selecto_extension_that_does_not_exist"]}
      )

    assert {:ok, extension_report} =
             Adapter.verify_function(connection, missing_extension, [])

    assert extension_report.status == :missing_requirement
    assert extension_report.diagnostics == [%{code: :required_extension_missing}]

    absent =
      scalar_request("absent", "public.selecto_function_that_does_not_exist", :integer, :integer)

    assert {:ok, absent_report} = Adapter.verify_function(connection, absent, [])
    assert absent_report.status == :missing_function

    wrong_signature = scalar_request("lower_integer", "pg_catalog.lower", :integer, :string)
    assert {:ok, wrong_report} = Adapter.verify_function(connection, wrong_signature, [])
    assert wrong_report.status == :signature_mismatch
  end

  test "reports current-role execute denial before parse-describe", %{connection: connection} do
    execute!(connection, "SET ROLE #{@restricted_role}")

    request =
      scalar_request(
        "restricted_scalar",
        "public.#{@scalar_name}",
        :integer,
        :integer
      )

    assert {:ok, report} = Adapter.verify_function(connection, request, [])
    assert report.status == :permission_denied
    assert report.diagnostics == [%{code: :function_execute_privilege_missing}]
    assert report.evidence == %{}

    execute!(connection, "RESET ROLE")
  end

  test "builds deterministic registry-wide connected evidence", %{connection: connection} do
    selecto =
      Selecto.configure(verification_domain(), connection,
        adapter: Adapter,
        validate: false
      )

    first = Suite.verify(selecto)
    second = Suite.verify(selecto)

    assert first == second
    assert first.strict_passed?
    assert first.summary.function_count == 3
    assert first.summary.signature_count == 3
    assert first.summary.status_counts == %{database_resolved: 3}

    assert Enum.map(first.results, & &1.function_id) == ["predicate", "scalar", "table"]
    assert Enum.all?(first.results, &(&1.status == :database_resolved))
    assert first.proof_boundary.runtime_argument_values_transmitted == false
    assert first.proof_boundary.function_execution_requested == false
  end

  test "verifies scalar, predicate, and table signatures without executing them", %{
    connection: connection
  } do
    scalar =
      request!(
        "scalar",
        %{
          kind: :scalar,
          sql_name: "public.#{@scalar_name}",
          args: [%{name: :value, type: :integer, source: :value, null?: false}],
          returns: :integer,
          database: %{adapters: [:postgresql], volatility: :immutable}
        },
        :select
      )

    predicate =
      request!(
        "predicate",
        %{
          kind: :predicate,
          sql_name: "public.#{@predicate_name}",
          args: [%{name: :value, type: :integer, source: :value, null?: false}],
          returns: :boolean,
          database: %{adapters: [:postgresql], volatility: :stable}
        },
        :filter
      )

    table =
      request!(
        "table",
        %{
          kind: :table,
          sql_name: "public.#{@table_name}",
          args: [%{name: :value, type: :integer, source: :value, null?: false}],
          returns: %{
            columns: %{
              id: %{type: :integer},
              label: %{type: :string}
            }
          },
          database: %{adapters: [:postgresql], volatility: :stable}
        },
        :lateral
      )

    for request <- [scalar, predicate, table] do
      assert {:ok, report} = Adapter.verify_function(connection, request, [])
      assert report.status == :database_resolved, inspect(report)
      assert report.evidence.function_executed == false
      assert report.evidence.argument_values_transmitted == false
      assert report.evidence.strategy == [:pg_catalog, :parse_describe]
    end
  end

  defp create_functions(connection) do
    execute!(connection, """
    CREATE OR REPLACE FUNCTION public.#{@scalar_name}(integer)
    RETURNS integer
    LANGUAGE plpgsql
    IMMUTABLE
    AS $function$
    BEGIN
      RAISE EXCEPTION 'verification executed the scalar function';
      RETURN $1;
    END;
    $function$
    """)

    execute!(connection, """
    CREATE OR REPLACE FUNCTION public.#{@predicate_name}(integer)
    RETURNS boolean
    LANGUAGE sql
    STABLE
    AS $function$
      SELECT $1 > 0
    $function$
    """)

    execute!(connection, """
    CREATE OR REPLACE FUNCTION public.#{@table_name}(integer)
    RETURNS TABLE(id integer, label text)
    LANGUAGE sql
    STABLE
    AS $function$
      SELECT $1, 'verified'::text
    $function$
    """)

    execute!(
      connection,
      "REVOKE EXECUTE ON FUNCTION public.#{@scalar_name}(integer) FROM PUBLIC"
    )
  end

  defp create_restricted_role(connection) do
    execute!(connection, "DROP ROLE IF EXISTS #{@restricted_role}")
    execute!(connection, "CREATE ROLE #{@restricted_role} NOLOGIN")
  end

  defp drop_functions(connection) do
    execute!(connection, "DROP FUNCTION IF EXISTS public.#{@table_name}(integer)")
    execute!(connection, "DROP FUNCTION IF EXISTS public.#{@predicate_name}(integer)")
    execute!(connection, "DROP FUNCTION IF EXISTS public.#{@scalar_name}(integer)")
  end

  defp execute!(connection, sql) do
    assert {:ok, _result} = Adapter.execute(connection, sql, [], prepared: false)
  end

  defp request!(function_id, signature, call_site) do
    assert {:ok, request} = Request.new(function_id, signature, call_site)
    request
  end

  defp scalar_request(function_id, sql_name, argument_type, return_type, opts \\ []) do
    request!(
      function_id,
      %{
        kind: :scalar,
        sql_name: sql_name,
        args: [%{name: :value, type: argument_type, source: :value, null?: false}],
        returns: return_type,
        database: Keyword.get(opts, :database, %{})
      },
      :select
    )
  end

  defp verification_domain do
    %{
      name: "PostgreSQL function verification",
      source: %{
        source_table: "pg_catalog.pg_type",
        primary_key: :oid,
        fields: [:oid],
        redact_fields: [],
        columns: %{oid: %{type: :integer}},
        associations: %{}
      },
      schemas: %{},
      joins: %{},
      functions: %{
        "scalar" => %{
          kind: :scalar,
          sql_name: "public.#{@scalar_name}",
          args: [%{name: :value, type: :integer, source: :value, null?: false}],
          returns: :integer,
          allowed_in: [:select],
          database: %{adapters: [:postgresql], volatility: :immutable}
        },
        "predicate" => %{
          kind: :predicate,
          sql_name: "public.#{@predicate_name}",
          args: [%{name: :value, type: :integer, source: :value, null?: false}],
          returns: :boolean,
          allowed_in: [:filter],
          database: %{adapters: [:postgresql], volatility: :stable}
        },
        "table" => %{
          kind: :table,
          sql_name: "public.#{@table_name}",
          args: [%{name: :value, type: :integer, source: :value, null?: false}],
          returns: %{
            columns: %{id: %{type: :integer}, label: %{type: :string}}
          },
          allowed_in: [:lateral],
          database: %{adapters: [:postgresql], volatility: :stable}
        }
      }
    }
  end
end
