defmodule SelectoDBPostgreSQL.FunctionVerification do
  @moduledoc false

  alias Selecto.FunctionVerification.Request

  @catalog_query """
  WITH requested AS (
    SELECT to_regprocedure($1)::oid AS oid
  ),
  candidates AS (
    SELECT count(*)::integer AS candidate_count
    FROM pg_proc AS candidate
    JOIN pg_namespace AS candidate_namespace ON candidate_namespace.oid = candidate.pronamespace
    WHERE candidate.prokind = 'f'
      AND candidate.proname = $2
      AND CASE
        WHEN $3::text IS NULL THEN pg_function_is_visible(candidate.oid)
        ELSE candidate_namespace.nspname = $3
      END
  )
  SELECT
    resolved_function.oid::text,
    CASE
      WHEN resolved_function.oid IS NULL THEN NULL
      ELSE format(
        '%I.%I(%s)',
        namespace.nspname,
        resolved_function.proname,
        pg_get_function_identity_arguments(resolved_function.oid)
      )
    END AS resolved_identity,
    format_type(resolved_function.prorettype, NULL) AS return_type,
    resolved_function.proretset,
    resolved_function.provolatile::text,
    has_function_privilege(resolved_function.oid, 'EXECUTE') AS executable,
    current_setting('server_version_num') AS server_version_num,
    current_setting('search_path') AS search_path,
    current_database() AS database_name,
    ARRAY(
      SELECT resolved_function.proargnames[index]
      FROM generate_subscripts(resolved_function.proargmodes, 1) AS index
      WHERE resolved_function.proargmodes[index]::text IN ('o', 'b', 't')
      ORDER BY index
    ) AS output_names,
    ARRAY(
      SELECT format_type(resolved_function.proallargtypes[index], NULL)
      FROM generate_subscripts(resolved_function.proargmodes, 1) AS index
      WHERE resolved_function.proargmodes[index]::text IN ('o', 'b', 't')
      ORDER BY index
    ) AS output_types,
    ARRAY(
      SELECT attribute.attname
      FROM pg_attribute AS attribute
      WHERE attribute.attrelid = resolved_function.prorettype
        AND attribute.attnum > 0
        AND NOT attribute.attisdropped
      ORDER BY attribute.attnum
    ) AS composite_output_names,
    ARRAY(
      SELECT format_type(attribute.atttypid, attribute.atttypmod)
      FROM pg_attribute AS attribute
      WHERE attribute.attrelid = resolved_function.prorettype
        AND attribute.attnum > 0
        AND NOT attribute.attisdropped
      ORDER BY attribute.attnum
    ) AS composite_output_types,
    candidates.candidate_count,
    ARRAY(
      SELECT required_extension
      FROM unnest($4::text[]) AS required_extension
      WHERE EXISTS (
        SELECT 1
        FROM pg_extension
        WHERE pg_extension.extname = required_extension
      )
      ORDER BY required_extension
    ) AS satisfied_extensions
  FROM requested
  CROSS JOIN candidates
  LEFT JOIN pg_proc AS resolved_function
    ON resolved_function.oid = requested.oid
   AND resolved_function.prokind = 'f'
  LEFT JOIN pg_namespace AS namespace ON namespace.oid = resolved_function.pronamespace
  """

  @type query_fun ::
          (String.t(), [term()] -> {:ok, map()} | {:error, term()})
  @type probe_fun ::
          (String.t(), keyword() -> {:ok, map()} | {:error, term()})

  @spec verify(Request.t(), keyword(), query_fun(), probe_fun()) :: {:ok, map()}
  def verify(%Request{} = request, opts, query_fun, probe_fun)
      when is_list(opts) and is_function(query_fun, 2) and is_function(probe_fun, 2) do
    with {:ok, identity} <- requested_identity(request),
         :ok <- adapter_requirement(request),
         {:ok, catalog} <- fetch_catalog(request, identity, query_fun),
         :ok <- catalog_resolution(request, identity, catalog),
         :ok <- database_requirements(request, catalog),
         :ok <- return_shape(request, catalog),
         {:ok, probe} <- run_probe(request, identity, opts, probe_fun) do
      {:ok,
       report(:database_resolved, request, %{
         resolved_identity: catalog.resolved_identity,
         server: server(catalog),
         evidence: evidence(request, identity, catalog, probe)
       })}
    else
      {:report, status, attrs} -> {:ok, report(status, request, attrs)}
    end
  end

  defp fetch_catalog(request, identity, query_fun) do
    params = [
      identity.regprocedure,
      identity.function_name,
      identity.schema,
      required_extensions(request)
    ]

    case query_fun.(@catalog_query, params) do
      {:ok, %{rows: [row]}} when is_list(row) -> decode_catalog(row)
      {:ok, %{rows: []}} -> indeterminate(:catalog_returned_no_evidence)
      {:ok, _result} -> indeterminate(:invalid_catalog_result)
      {:error, reason} -> database_error(reason, :catalog_query_failed)
      _other -> indeterminate(:invalid_catalog_result)
    end
  rescue
    _exception -> indeterminate(:catalog_query_failed)
  catch
    _kind, _reason -> indeterminate(:catalog_query_failed)
  end

  defp decode_catalog([
         oid,
         resolved_identity,
         return_type,
         returns_set,
         volatility,
         executable,
         server_version_num,
         search_path,
         database_name,
         output_names,
         output_types,
         composite_output_names,
         composite_output_types,
         candidate_count,
         satisfied_extensions
       ]) do
    with {:ok, parsed_version} <- parse_integer(server_version_num),
         {:ok, parsed_candidates} <- parse_integer(candidate_count) do
      {:ok,
       %{
         oid: oid,
         resolved_identity: resolved_identity,
         return_type: return_type,
         returns_set: returns_set == true,
         volatility: normalize_volatility(volatility),
         executable: executable == true,
         server_version_num: parsed_version,
         search_path: search_path,
         database_name: database_name,
         output_names: list_or_empty(output_names),
         output_types: list_or_empty(output_types),
         composite_output_names: list_or_empty(composite_output_names),
         composite_output_types: list_or_empty(composite_output_types),
         candidate_count: parsed_candidates,
         satisfied_extensions: list_or_empty(satisfied_extensions)
       }}
    else
      _error -> indeterminate(:invalid_catalog_result)
    end
  end

  defp decode_catalog(_row), do: indeterminate(:invalid_catalog_result)

  defp catalog_resolution(_request, identity, %{oid: nil, candidate_count: 0}) do
    failure(:missing_function, :function_not_found, %{
      evidence: base_evidence(identity, false)
    })
  end

  defp catalog_resolution(_request, identity, %{oid: nil, candidate_count: count})
       when count > 0 do
    failure(:signature_mismatch, :exact_signature_not_found, %{
      evidence: Map.put(base_evidence(identity, false), :same_name_candidate_count, count)
    })
  end

  defp catalog_resolution(_request, _identity, %{oid: oid, executable: false})
       when is_binary(oid) do
    failure(:permission_denied, :function_execute_privilege_missing)
  end

  defp catalog_resolution(_request, _identity, %{oid: oid}) when is_binary(oid), do: :ok

  defp catalog_resolution(_request, _identity, _catalog),
    do: indeterminate(:invalid_catalog_result)

  defp database_requirements(request, catalog) do
    missing_extensions =
      request
      |> required_extensions()
      |> Kernel.--(catalog.satisfied_extensions)

    cond do
      missing_extensions != [] ->
        failure(:missing_requirement, :required_extension_missing, %{
          evidence: %{missing_extensions: missing_extensions}
        })

      not minimum_version_met?(request.requirements[:minimum_version], catalog.server_version_num) ->
        failure(:missing_requirement, :minimum_postgresql_version_not_met, %{
          evidence: %{
            required_version: request.requirements[:minimum_version],
            server_version_num: catalog.server_version_num
          }
        })

      not volatility_met?(request.requirements[:volatility], catalog.volatility) ->
        failure(:missing_requirement, :function_volatility_mismatch, %{
          evidence: %{
            required_volatility: request.requirements[:volatility],
            actual_volatility: catalog.volatility
          }
        })

      true ->
        :ok
    end
  end

  defp return_shape(%Request{kind: :table} = request, catalog) do
    {actual_names, actual_types} = table_output(catalog)

    with {:ok, expected} <- expected_table_columns(request.returns),
         {:ok, actual} <- actual_table_columns(actual_names, actual_types) do
      if map_size(expected) == map_size(actual) and
           Enum.all?(expected, fn {name, type} ->
             Map.has_key?(actual, name) and return_type_matches?(type, Map.fetch!(actual, name))
           end) do
        :ok
      else
        failure(:return_mismatch, :table_return_shape_mismatch, %{
          evidence: %{
            expected_columns: safe_column_evidence(expected),
            actual_columns: safe_column_evidence(actual),
            returns_set: catalog.returns_set
          }
        })
      end
    else
      {:error, code} -> indeterminate(code)
    end
  end

  defp return_shape(%Request{returns: nil}, _catalog), do: :ok

  defp return_shape(%Request{returns: expected}, catalog) do
    if return_type_matches?(expected, catalog.return_type) do
      :ok
    else
      failure(:return_mismatch, :scalar_return_type_mismatch, %{
        evidence: %{expected_return: expected, actual_return: catalog.return_type}
      })
    end
  end

  defp run_probe(request, identity, opts, probe_fun) do
    statement = probe_statement(request, identity)
    timeout = verification_timeout(opts)

    case probe_fun.(statement, timeout: timeout) do
      {:ok, probe} when is_map(probe) -> {:ok, normalize_probe(probe)}
      {:error, reason} -> database_error(reason, :parse_describe_failed)
      _other -> indeterminate(:invalid_parse_describe_result)
    end
  rescue
    _exception -> indeterminate(:parse_describe_failed)
  catch
    _kind, _reason -> indeterminate(:parse_describe_failed)
  end

  defp requested_identity(request) do
    with {:ok, schema, function_name} <- split_sql_name(request.sql_name),
         {:ok, argument_types} <- map_types(Enum.map(request.arguments, & &1.type)) do
      qualified_name =
        [schema, function_name]
        |> Enum.reject(&is_nil/1)
        |> Enum.join(".")

      {:ok,
       %{
         schema: schema,
         function_name: function_name,
         qualified_name: qualified_name,
         quoted_name: quote_qualified_name(schema, function_name),
         argument_types: argument_types,
         regprocedure: "#{qualified_name}(#{Enum.join(argument_types, ",")})"
       }}
    else
      {:error, code} -> indeterminate(code)
    end
  end

  defp split_sql_name(sql_name) do
    case String.split(sql_name, ".") do
      [function_name] -> {:ok, nil, function_name}
      [schema, function_name] -> {:ok, schema, function_name}
      _parts -> {:error, :unsupported_postgresql_function_name}
    end
  end

  defp map_types(types) do
    Enum.reduce_while(types, {:ok, []}, fn type, {:ok, mapped} ->
      case postgres_type(type) do
        {:ok, postgres_type} -> {:cont, {:ok, [postgres_type | mapped]}}
        :error -> {:halt, {:error, :unsupported_selecto_argument_type}}
      end
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      error -> error
    end
  end

  defp postgres_type({:array, inner}) do
    case postgres_type(inner) do
      {:ok, mapped} -> {:ok, mapped <> "[]"}
      :error -> :error
    end
  end

  defp postgres_type({:native, :postgresql, :jsonb}), do: {:ok, "jsonb"}
  defp postgres_type({:native, :postgresql, "jsonb"}), do: {:ok, "jsonb"}
  defp postgres_type({:native, :postgresql, :tsvector}), do: {:ok, "tsvector"}
  defp postgres_type({:native, :postgresql, "tsvector"}), do: {:ok, "tsvector"}
  defp postgres_type({:native, :postgresql, :bytea}), do: {:ok, "bytea"}
  defp postgres_type({:native, :postgresql, "bytea"}), do: {:ok, "bytea"}

  defp postgres_type(type) do
    case type do
      :integer ->
        {:ok, "integer"}

      :bigint ->
        {:ok, "bigint"}

      :smallint ->
        {:ok, "smallint"}

      :decimal ->
        {:ok, "numeric"}

      :numeric ->
        {:ok, "numeric"}

      :float ->
        {:ok, "double precision"}

      :string ->
        {:ok, "text"}

      :text ->
        {:ok, "text"}

      :varchar ->
        {:ok, "character varying"}

      :char ->
        {:ok, "character"}

      :boolean ->
        {:ok, "boolean"}

      :date ->
        {:ok, "date"}

      :time ->
        {:ok, "time without time zone"}

      :datetime ->
        {:ok, "timestamp without time zone"}

      :naive_datetime ->
        {:ok, "timestamp without time zone"}

      :timestamp ->
        {:ok, "timestamp without time zone"}

      :utc_datetime ->
        {:ok, "timestamp with time zone"}

      :json ->
        {:ok, "json"}

      :jsonb ->
        {:ok, "jsonb"}

      :map ->
        {:ok, "jsonb"}

      :binary ->
        {:ok, "bytea"}

      :bytea ->
        {:ok, "bytea"}

      :uuid ->
        {:ok, "uuid"}

      :interval ->
        {:ok, "interval"}

      :record ->
        {:ok, "record"}

      spatial when spatial in [:geometry, :geography, :point, :linestring, :polygon] ->
        {:ok, Atom.to_string(spatial)}

      spatial
      when spatial in [
             :multipoint,
             :multilinestring,
             :multipolygon,
             :geometrycollection
           ] ->
        {:ok, Atom.to_string(spatial)}

      _other ->
        :error
    end
  end

  defp probe_statement(%Request{kind: :table}, identity) do
    "SELECT * FROM #{identity.quoted_name}(#{probe_arguments(identity.argument_types)})"
  end

  defp probe_statement(_request, identity) do
    "SELECT #{identity.quoted_name}(#{probe_arguments(identity.argument_types)}) " <>
      "AS selecto_verified_value"
  end

  defp probe_arguments(types) do
    types
    |> Enum.with_index(1)
    |> Enum.map_join(", ", fn {type, index} -> "$#{index}::#{type}" end)
  end

  defp quote_qualified_name(nil, function_name), do: quote_identifier(function_name)

  defp quote_qualified_name(schema, function_name) do
    quote_identifier(schema) <> "." <> quote_identifier(function_name)
  end

  defp quote_identifier(identifier), do: ~s("#{String.replace(identifier, "\"", "\"\"")}")

  defp adapter_requirement(request) do
    case Map.get(request.requirements, :adapters) do
      nil ->
        :ok

      adapters ->
        if adapters |> List.wrap() |> Enum.any?(&postgresql_adapter?/1) do
          :ok
        else
          failure(:missing_requirement, :adapter_requirement_not_met, %{
            evidence: %{required_adapters: List.wrap(adapters), actual_adapter: :postgresql}
          })
        end
    end
  end

  defp postgresql_adapter?(:postgresql), do: true
  defp postgresql_adapter?("postgresql"), do: true
  defp postgresql_adapter?(_adapter), do: false

  defp required_extensions(request) do
    request.requirements
    |> Map.get(:requires)
    |> extension_values()
    |> Enum.filter(&(is_binary(&1) and &1 != ""))
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp extension_values(nil), do: []

  defp extension_values(requirements) when is_map(requirements) do
    requirements
    |> map_value(:extension)
    |> List.wrap()
  end

  defp extension_values(requirements) when is_list(requirements) do
    requirements
    |> Enum.flat_map(fn
      {:extension, value} -> List.wrap(value)
      {"extension", value} -> List.wrap(value)
      _requirement -> []
    end)
  end

  defp extension_values(_requirements), do: []

  defp minimum_version_met?(nil, _server_version_num), do: true

  defp minimum_version_met?(minimum, server_version_num) do
    case normalize_minimum_version(minimum) do
      {:major, major} -> div(server_version_num, 10_000) >= major
      {:version_num, version_num} -> server_version_num >= version_num
      :invalid -> false
    end
  end

  defp normalize_minimum_version(version) when is_integer(version) and version > 0 do
    if version < 10_000, do: {:major, version}, else: {:version_num, version}
  end

  defp normalize_minimum_version(version) when is_binary(version) do
    case version |> String.split(".", parts: 2) |> hd() |> Integer.parse() do
      {major, ""} when major > 0 -> {:major, major}
      _parse -> :invalid
    end
  end

  defp normalize_minimum_version(_version), do: :invalid

  defp volatility_met?(nil, _actual), do: true

  defp volatility_met?(required, actual) do
    normalize_volatility(required) == actual and actual in [:immutable, :stable, :volatile]
  end

  defp normalize_volatility(value) when value in [:immutable, "immutable", "i"], do: :immutable
  defp normalize_volatility(value) when value in [:stable, "stable", "s"], do: :stable
  defp normalize_volatility(value) when value in [:volatile, "volatile", "v"], do: :volatile
  defp normalize_volatility(_value), do: :unknown

  defp expected_table_columns(returns) do
    case map_value(returns, :columns) do
      columns when is_map(columns) and map_size(columns) > 0 ->
        Enum.reduce_while(columns, {:ok, %{}}, fn {name, declaration}, {:ok, acc} ->
          type = if is_map(declaration), do: map_value(declaration, :type), else: declaration

          case postgres_type(type) do
            {:ok, _mapped} -> {:cont, {:ok, Map.put(acc, to_string(name), type)}}
            :error -> {:halt, {:error, :unsupported_table_return_declaration}}
          end
        end)

      _columns ->
        {:error, :unsupported_table_return_declaration}
    end
  end

  defp actual_table_columns(names, types)
       when is_list(names) and is_list(types) and length(names) == length(types) do
    valid_names? = Enum.all?(names, &(is_binary(&1) and &1 != ""))

    if valid_names? and length(Enum.uniq(names)) == length(names) do
      {:ok, Map.new(Enum.zip(names, types))}
    else
      {:error, :invalid_catalog_table_shape}
    end
  end

  defp actual_table_columns(_names, _types), do: {:error, :invalid_catalog_table_shape}

  defp table_output(%{output_names: names, output_types: types}) when names != [] do
    {names, types}
  end

  defp table_output(catalog) do
    {catalog.composite_output_names, catalog.composite_output_types}
  end

  defp return_type_matches?({:array, inner}, actual) when is_binary(actual) do
    case String.trim(actual) do
      value when byte_size(value) > 2 ->
        if String.ends_with?(value, "[]") do
          return_type_matches?(inner, String.slice(value, 0, byte_size(value) - 2))
        else
          false
        end

      _value ->
        false
    end
  end

  defp return_type_matches?(expected, actual) when is_binary(actual) do
    normalized_actual = normalize_postgresql_type(actual)

    case expected do
      :string -> normalized_actual in ["text", "character varying", "character"]
      :float -> normalized_actual in ["double precision", "real"]
      _type -> match_postgres_type?(expected, normalized_actual)
    end
  end

  defp return_type_matches?(_expected, _actual), do: false

  defp match_postgres_type?(expected, actual) do
    case postgres_type(expected) do
      {:ok, mapped} -> normalize_postgresql_type(mapped) == actual
      :error -> false
    end
  end

  defp normalize_postgresql_type(type) do
    type
    |> String.downcase()
    |> String.trim()
    |> String.replace(~r/\s+/, " ")
  end

  defp evidence(request, identity, catalog, probe) do
    %{
      strategy: [:pg_catalog, :parse_describe],
      catalog_match: true,
      parse_describe: true,
      function_executed: false,
      argument_values_transmitted: false,
      requested_identity: identity.regprocedure,
      declared_kind: request.kind,
      catalog_return_type: catalog.return_type,
      catalog_returns_set: catalog.returns_set,
      catalog_volatility: catalog.volatility,
      required_extensions: required_extensions(request),
      parse_describe_columns: probe.columns,
      parse_describe_result_oids: probe.result_oids
    }
  end

  defp base_evidence(identity, catalog_match) do
    %{
      strategy: [:pg_catalog, :parse_describe],
      catalog_match: catalog_match,
      parse_describe: false,
      function_executed: false,
      argument_values_transmitted: false,
      requested_identity: identity.regprocedure
    }
  end

  defp normalize_probe(probe) do
    %{
      columns: list_or_empty(map_value(probe, :columns)),
      result_oids: list_or_empty(map_value(probe, :result_oids))
    }
  end

  defp safe_column_evidence(columns) do
    Map.new(columns, fn {name, type} -> {name, inspect(type)} end)
  end

  defp server(catalog) do
    %{
      database: catalog.database_name,
      server_version_num: catalog.server_version_num,
      search_path: catalog.search_path
    }
  end

  defp report(status, _request, attrs) do
    %{
      status: status,
      resolved_identity: Map.get(attrs, :resolved_identity),
      server: Map.get(attrs, :server, %{}),
      evidence: Map.get(attrs, :evidence, %{}),
      diagnostics: Map.get(attrs, :diagnostics, [])
    }
  end

  defp failure(status, code, attrs \\ %{}) do
    {:report, status, Map.put(attrs, :diagnostics, [%{code: code}])}
  end

  defp indeterminate(code), do: failure(:indeterminate, code)

  defp database_error(reason, fallback_code) do
    case database_error_code(reason) do
      code when code in [:insufficient_privilege, "42501"] ->
        failure(:permission_denied, :database_permission_denied)

      code when code in [:undefined_function, "42883"] ->
        failure(:signature_mismatch, :database_signature_resolution_failed)

      _code ->
        indeterminate(fallback_code)
    end
  end

  defp database_error_code(%Postgrex.Error{postgres: postgres}) when is_map(postgres) do
    map_value(postgres, :code)
  end

  defp database_error_code(%{postgres: postgres}) when is_map(postgres) do
    map_value(postgres, :code)
  end

  defp database_error_code(_reason), do: nil

  defp parse_integer(value) when is_integer(value) and value >= 0, do: {:ok, value}

  defp parse_integer(value) when is_binary(value) do
    case Integer.parse(value) do
      {integer, ""} when integer >= 0 -> {:ok, integer}
      _parse -> :error
    end
  end

  defp parse_integer(_value), do: :error

  defp verification_timeout(opts) do
    case Keyword.get(opts, :timeout, 15_000) do
      timeout when is_integer(timeout) and timeout > 0 -> timeout
      _timeout -> 15_000
    end
  end

  defp list_or_empty(value) when is_list(value), do: value
  defp list_or_empty(_value), do: []

  defp map_value(map, key) when is_map(map) do
    Map.get(map, key) || Map.get(map, Atom.to_string(key))
  end

  defp map_value(_value, _key), do: nil
end
