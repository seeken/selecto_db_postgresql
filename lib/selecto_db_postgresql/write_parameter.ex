defmodule SelectoDBPostgreSQL.WriteParameter do
  @moduledoc false
  alias Selecto.Write.Error

  # Only internal execution uses these markers; previews keep ordinary values.
  # The fresh unnamed description supplies the actual parameter codec, including
  # domains over uuid. Nothing is cached across statements or connections.
  @enforce_keys [:field, :value, :tenant?]
  defstruct [:field, :value, :tenant?]

  @uuid ~r/\A[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\z/

  def marked?(params), do: Enum.any?(params, &is_struct(&1, __MODULE__))

  def query(connection, statement, params, opts) do
    # The certification contract observes actual Postgrex.query/4 returns.
    # Keep that public execution path. prepare/4 closes the unnamed description;
    # query/4 then parses the business text afresh, so no statement can be
    # replaced under a server-side name. This costs one additional round trip
    # for commands with textual literals, without a catalog SQL statement.
    with {:ok, query} <- Postgrex.prepare(connection, "", statement, opts),
         {:ok, values} <- bind(params, query.param_oids, query.types) do
      Postgrex.query(connection, statement, values, opts)
    end
  end

  defp bind(params, oids, types) do
    Enum.zip(params, oids)
    |> Enum.reduce_while({:ok, []}, fn {param, oid}, {:ok, values} ->
      case value(param, oid, types) do
        {:ok, value} -> {:cont, {:ok, [value | values]}}
        error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, values} -> {:ok, Enum.reverse(values)}
      error -> error
    end
  end

  defp value(%__MODULE__{} = param, 2950, _types), do: uuid(param)

  # Postgrex 0.22's already-loaded TypeInfo is keyed by the OID supplied by
  # this fresh description. A UUID domain has uuid_send even when the host
  # replaces the UUID extension (or shares one extension across several types).
  # Read the server's type identity, not an extension module-name heuristic.
  # No adapter cache is created; missing metadata refuses before execution.
  defp value(%__MODULE__{} = param, oid, {_module, table}) do
    case :ets.lookup(table, oid) do
      [{^oid, %Postgrex.TypeInfo{send: "uuid_send"}, _codec}] -> uuid(param)
      [{^oid, %Postgrex.TypeInfo{}, _codec}] -> {:ok, param.value}
      _missing -> missing_type_info()
    end
  end

  defp value(%__MODULE__{}, _oid, _types), do: missing_type_info()
  defp value(value, _oid, _types), do: {:ok, value}

  defp missing_type_info do
    {:error,
     Error.new(:query_enforcement, "PostgreSQL cannot read the parameter's type identity",
       details: %{
         code: :query_rule_not_evaluable,
         reason: :parameter_type_info,
         adapter: :postgresql
       }
     )}
  end

  defp uuid(%{value: value}) when byte_size(value) == 16, do: {:ok, value}

  defp uuid(%{value: value, field: field, tenant?: tenant?}) do
    if Regex.match?(@uuid, value) and (not tenant? or value == String.downcase(value)) do
      {:ok, value |> String.replace("-", "") |> Base.decode16!(case: :mixed)}
    else
      {:error,
       Error.new(:query_enforcement, "PostgreSQL refuses a value that is no canonical uuid",
         details: %{
           code: :query_rule_not_evaluable,
           reason: :invalid_uuid,
           field: to_string(field),
           adapter: :postgresql
         }
       )}
    end
  end
end
