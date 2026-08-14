defmodule SelectoDBPostgreSQL.FunctionVerificationTest do
  use ExUnit.Case, async: true

  alias Selecto.FunctionVerification.Request
  alias SelectoDBPostgreSQL.Adapter

  test "advertises connected function verification" do
    assert Adapter.supports?(:function_verification)
    assert function_exported?(Adapter, :verify_function, 3)
  end

  test "resolves an exact signature through the catalog and a parse-only typed probe" do
    parent = self()
    request = scalar_request()

    connection =
      mock_connection(
        catalog_row(),
        query_assertion: fn query, params, opts ->
          assert query =~ "to_regprocedure($1)"
          assert query =~ "has_function_privilege"
          assert query =~ "pg_extension"

          assert params == [
                   "public.similarity(text,text)",
                   "similarity",
                   "public",
                   ["pg_trgm"]
                 ]

          assert opts == [prepared: false]
          refute inspect({query, params}) =~ "TOP SECRET"
        end,
        probe_assertion: fn statement, opts ->
          assert statement ==
                   ~s|SELECT "public"."similarity"($1::text, $2::text) AS selecto_verified_value|

          assert opts == [timeout: 2_000]
          refute statement =~ "TOP SECRET"
          send(parent, :parse_describe_only)
        end
      )

    assert {:ok, report} = Adapter.verify_function(connection, request, timeout: 2_000)
    assert_receive :parse_describe_only

    assert report.status == :database_resolved
    assert report.resolved_identity == "public.similarity(text, text)"

    assert report.server == %{
             database: "selecto_test",
             search_path: ~s|"$user", public|,
             server_version_num: 180_001
           }

    assert report.evidence.strategy == [:pg_catalog, :parse_describe]
    assert report.evidence.catalog_match
    assert report.evidence.parse_describe
    assert report.evidence.function_executed == false
    assert report.evidence.argument_values_transmitted == false
  end

  test "distinguishes a missing function from an existing name with the wrong signature" do
    missing = catalog_row(oid: nil, resolved_identity: nil, candidate_count: 0)

    assert {:ok, missing_report} =
             Adapter.verify_function(mock_connection(missing), scalar_request(), [])

    assert missing_report.status == :missing_function
    assert missing_report.diagnostics == [%{code: :function_not_found}]

    mismatched = catalog_row(oid: nil, resolved_identity: nil, candidate_count: 2)

    assert {:ok, mismatch_report} =
             Adapter.verify_function(mock_connection(mismatched), scalar_request(), [])

    assert mismatch_report.status == :signature_mismatch
    assert mismatch_report.diagnostics == [%{code: :exact_signature_not_found}]
    assert mismatch_report.evidence.same_name_candidate_count == 2
  end

  test "fails closed on return type and execute privilege mismatches" do
    assert {:ok, return_report} =
             Adapter.verify_function(
               mock_connection(catalog_row(return_type: "integer")),
               scalar_request(),
               []
             )

    assert return_report.status == :return_mismatch
    assert return_report.diagnostics == [%{code: :scalar_return_type_mismatch}]
    assert return_report.evidence.actual_return == "integer"

    assert {:ok, privilege_report} =
             Adapter.verify_function(
               mock_connection(catalog_row(executable: false)),
               scalar_request(),
               []
             )

    assert privilege_report.status == :permission_denied
    assert privilege_report.diagnostics == [%{code: :function_execute_privilege_missing}]
  end

  test "checks extension, minimum-version, volatility, and adapter requirements" do
    assert {:ok, extension_report} =
             Adapter.verify_function(
               mock_connection(catalog_row(satisfied_extensions: [])),
               scalar_request(),
               []
             )

    assert extension_report.status == :missing_requirement
    assert extension_report.diagnostics == [%{code: :required_extension_missing}]
    assert extension_report.evidence.missing_extensions == ["pg_trgm"]

    assert {:ok, version_report} =
             Adapter.verify_function(
               mock_connection(catalog_row(server_version_num: "130009")),
               scalar_request(),
               []
             )

    assert version_report.status == :missing_requirement
    assert version_report.diagnostics == [%{code: :minimum_postgresql_version_not_met}]

    assert {:ok, volatility_report} =
             Adapter.verify_function(
               mock_connection(catalog_row(volatility: "v")),
               scalar_request(),
               []
             )

    assert volatility_report.status == :missing_requirement
    assert volatility_report.diagnostics == [%{code: :function_volatility_mismatch}]

    parent = self()

    connection = %{
      query_fun: fn _query, _params, _opts ->
        send(parent, :unexpected_catalog_dispatch)
        {:ok, %{rows: [catalog_row()]}}
      end,
      prepare_fun: fn _statement, _opts ->
        send(parent, :unexpected_probe_dispatch)
        {:ok, %{columns: [], result_oids: []}}
      end
    }

    request = scalar_request(database: %{adapters: [:mysql]})
    assert {:ok, adapter_report} = Adapter.verify_function(connection, request, [])
    assert adapter_report.status == :missing_requirement
    assert adapter_report.diagnostics == [%{code: :adapter_requirement_not_met}]
    refute_receive :unexpected_catalog_dispatch
    refute_receive :unexpected_probe_dispatch
  end

  test "verifies declared table columns by name and type" do
    request =
      request!(
        "nearby_points",
        %{
          kind: :table,
          sql_name: "gis.nearby_points",
          args: [%{name: :radius, type: :integer, source: :value, null?: false}],
          returns: %{
            columns: %{
              id: %{type: :integer},
              distance_m: %{type: :float}
            }
          }
        },
        :lateral
      )

    row =
      catalog_row(
        resolved_identity: "gis.nearby_points(integer)",
        return_type: "record",
        returns_set: true,
        volatility: "v",
        output_names: ["id", "distance_m"],
        output_types: ["integer", "double precision"],
        satisfied_extensions: []
      )

    connection =
      mock_connection(row,
        probe_assertion: fn statement, _opts ->
          assert statement == ~s|SELECT * FROM "gis"."nearby_points"($1::integer)|
        end,
        probe: %{columns: ["id", "distance_m"], result_oids: [23, 701]}
      )

    assert {:ok, report} = Adapter.verify_function(connection, request, [])
    assert report.status == :database_resolved
    assert report.evidence.catalog_returns_set

    wrong_shape = Keyword.put(row_options(row), :output_types, ["integer", "text"])

    assert {:ok, mismatch} =
             Adapter.verify_function(
               mock_connection(catalog_row(wrong_shape)),
               request,
               []
             )

    assert mismatch.status == :return_mismatch
    assert mismatch.diagnostics == [%{code: :table_return_shape_mismatch}]
  end

  test "sanitizes catalog and parse-describe failures into finite statuses" do
    catalog_failure = %{
      query_fun: fn _query, _params, _opts ->
        {:error, %{postgres: %{code: :insufficient_privilege}, secret: "TOP SECRET"}}
      end,
      prepare_fun: fn _statement, _opts -> flunk("probe must not run") end
    }

    assert {:ok, permission_report} =
             Adapter.verify_function(catalog_failure, scalar_request(), [])

    assert permission_report.status == :permission_denied
    refute inspect(permission_report) =~ "TOP SECRET"

    probe_failure =
      mock_connection(catalog_row(),
        probe: {:error, %{postgres: %{code: :undefined_function}, secret: "TOP SECRET"}}
      )

    assert {:ok, signature_report} =
             Adapter.verify_function(probe_failure, scalar_request(), [])

    assert signature_report.status == :signature_mismatch
    assert signature_report.diagnostics == [%{code: :database_signature_resolution_failed}]
    refute inspect(signature_report) =~ "TOP SECRET"

    invalid_probe = mock_connection(catalog_row(), probe: :invalid)
    assert {:ok, indeterminate} = Adapter.verify_function(invalid_probe, scalar_request(), [])
    assert indeterminate.status == :indeterminate
    assert indeterminate.diagnostics == [%{code: :invalid_parse_describe_result}]
  end

  test "unsupported Selecto argument types do not reach the database" do
    parent = self()
    request = scalar_request(args: [%{name: :left, type: :unknown, source: :value}])

    connection = %{
      query_fun: fn _query, _params, _opts ->
        send(parent, :unexpected_catalog_dispatch)
        {:ok, %{rows: [catalog_row()]}}
      end,
      prepare_fun: fn _statement, _opts ->
        send(parent, :unexpected_probe_dispatch)
        {:ok, %{columns: [], result_oids: []}}
      end
    }

    assert {:ok, report} = Adapter.verify_function(connection, request, [])
    assert report.status == :indeterminate
    assert report.diagnostics == [%{code: :unsupported_selecto_argument_type}]
    refute_receive :unexpected_catalog_dispatch
    refute_receive :unexpected_probe_dispatch
  end

  defp scalar_request(overrides \\ []) do
    args =
      Keyword.get(overrides, :args, [
        %{name: :left, type: :string, source: :selector, null?: false},
        %{name: :right, type: :string, source: :value, null?: false}
      ])

    database =
      Keyword.get(overrides, :database, %{
        adapters: [:postgresql],
        requires: [extension: "pg_trgm"],
        volatility: :stable,
        minimum_version: 14
      })

    request!(
      "similarity",
      %{
        kind: :scalar,
        sql_name: "public.similarity",
        args: args,
        returns: :float,
        database: database
      },
      :select
    )
  end

  defp request!(function_id, signature, call_site) do
    assert {:ok, request} = Request.new(function_id, signature, call_site)
    request
  end

  defp mock_connection(row, opts \\ []) do
    query_assertion =
      Keyword.get(opts, :query_assertion, fn _query, _params, _query_opts -> :ok end)

    probe_assertion = Keyword.get(opts, :probe_assertion, fn _statement, _probe_opts -> :ok end)
    probe = Keyword.get(opts, :probe, %{columns: ["selecto_verified_value"], result_oids: [701]})

    %{
      query_fun: fn query, params, query_opts ->
        query_assertion.(query, params, query_opts)
        {:ok, %{rows: [row], columns: []}}
      end,
      prepare_fun: fn statement, probe_opts ->
        probe_assertion.(statement, probe_opts)

        case probe do
          {:error, reason} -> {:error, reason}
          :invalid -> :invalid
          result -> {:ok, result}
        end
      end
    }
  end

  defp catalog_row(opts \\ []) do
    values =
      Keyword.merge(
        [
          oid: "16384",
          resolved_identity: "public.similarity(text, text)",
          return_type: "double precision",
          returns_set: false,
          volatility: "s",
          executable: true,
          server_version_num: "180001",
          search_path: ~s|"$user", public|,
          database_name: "selecto_test",
          output_names: [],
          output_types: [],
          composite_output_names: [],
          composite_output_types: [],
          candidate_count: 1,
          satisfied_extensions: ["pg_trgm"]
        ],
        opts
      )

    Enum.map(row_keys(), &Keyword.fetch!(values, &1))
  end

  defp row_options(row), do: Enum.zip(row_keys(), row)

  defp row_keys do
    [
      :oid,
      :resolved_identity,
      :return_type,
      :returns_set,
      :volatility,
      :executable,
      :server_version_num,
      :search_path,
      :database_name,
      :output_names,
      :output_types,
      :composite_output_names,
      :composite_output_types,
      :candidate_count,
      :satisfied_extensions
    ]
  end
end
