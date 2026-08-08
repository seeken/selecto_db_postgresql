defmodule SelectoDBPostgreSQL.WriteCompiler do
  @moduledoc false

  alias Selecto.Write.{Command, Error, Preview}

  @spec preview(Command.t() | Selecto.Write.Batch.t(), keyword()) ::
          {:ok, Preview.t()} | {:error, Error.t()}
  def preview(%Selecto.Write.Batch{commands: commands}, opts) do
    with {:ok, statements} <- map_commands(commands, &compile(&1, opts)) do
      {:ok, %Preview{statements: statements, metadata: %{dialect: :postgresql, atomic?: true}}}
    end
  end

  def preview(%Command{} = command, opts) do
    with {:ok, statement} <- compile(command, opts) do
      {:ok, %Preview{statements: [statement], metadata: %{dialect: :postgresql}}}
    end
  end

  @spec compile(Command.t(), keyword()) ::
          {:ok, %{text: String.t(), params: [term()]}} | {:error, Error.t()}
  def compile(%Command{operation: :insert} = command, opts), do: compile_insert(command, opts)
  def compile(%Command{operation: :upsert} = command, opts), do: compile_upsert(command, opts)
  def compile(%Command{operation: :update} = command, opts), do: compile_update(command, opts)
  def compile(%Command{operation: :delete} = command, opts), do: compile_delete(command, opts)

  def compile(%Command{operation: operation}, _opts) do
    {:error,
     Error.new(
       :unsupported_operation,
       "PostgreSQL adapter does not yet compile this write operation",
       details: %{operation: operation}
     )}
  end

  defp compile_insert(%Command{} = command, opts) do
    with {:ok, assignments} <- compile_assignments(command.assignments, opts),
         true <-
           assignments != [] or
             {:error, Error.new(:invalid_command, "insert requires at least one assignment")},
         {:ok, returning} <- compile_returning(command.returning) do
      {columns, values, params} = assignment_parts(assignments)

      with {:ok, guards} <-
             compile_foreign_key_guards(command.metadata, assignments, length(params)) do
        values_clause =
          case guards.text do
            nil -> "VALUES (#{Enum.join(values, ", ")})"
            text -> "SELECT #{Enum.join(values, ", ")} WHERE #{text}"
          end

        {:ok,
         %{
           text:
             "INSERT INTO #{quote_relation(command.relation)} (#{Enum.join(columns, ", ")}) #{values_clause}#{returning}",
           params: params ++ guards.params
         }}
      end
    else
      {:error, _} = error -> error
    end
  end

  defp compile_upsert(%Command{} = command, opts) do
    with {:ok, assignments} <- compile_assignments(command.assignments, opts),
         true <-
           assignments != [] or
             {:error, Error.new(:invalid_command, "upsert requires at least one assignment")},
         {:ok, conflict_target} <- compile_conflict_target(command.metadata),
         {:ok, returning} <- compile_returning(command.returning) do
      {columns, values, params} = assignment_parts(assignments)

      with {:ok, guards} <-
             compile_foreign_key_guards(command.metadata, assignments, length(params)) do
        update_set = Enum.map_join(columns, ", ", &"#{&1} = EXCLUDED.#{&1}")

        values_clause =
          case guards.text do
            nil -> "VALUES (#{Enum.join(values, ", ")})"
            text -> "SELECT #{Enum.join(values, ", ")} WHERE #{text}"
          end

        {:ok,
         %{
           text:
             "INSERT INTO #{quote_relation(command.relation)} (#{Enum.join(columns, ", ")}) #{values_clause} ON CONFLICT (#{conflict_target}) DO UPDATE SET #{update_set}#{returning}",
           params: params ++ guards.params
         }}
      end
    else
      {:error, _} = error -> error
    end
  end

  defp compile_update(%Command{} = command, opts) do
    with true <-
           not is_nil(command.predicate) or
             {:error, Error.new(:missing_predicate, "update requires a portable predicate")},
         {:ok, assignments} <- compile_assignments(command.assignments, opts),
         true <-
           assignments != [] or
             {:error, Error.new(:invalid_command, "update requires at least one assignment")},
         {:ok, predicate} <-
           compile_predicate(command.predicate, opts, assignment_parameter_count(assignments)),
         {:ok, guards} <-
           compile_foreign_key_guards(
             command.metadata,
             assignments,
             assignment_parameter_count(assignments) + length(predicate.params)
           ),
         {:ok, returning} <- compile_returning(command.returning) do
      {columns, values, assignment_params} = assignment_parts(assignments)

      set = Enum.zip_with(columns, values, &"#{&1} = #{&2}") |> Enum.join(", ")

      {:ok,
       %{
         text:
           "UPDATE #{quote_relation(command.relation)} SET #{set} WHERE #{predicate.text}#{guard_suffix(guards.text)}#{returning}",
         params: assignment_params ++ predicate.params ++ guards.params
       }}
    end
  end

  defp compile_delete(%Command{predicate: nil}, _opts) do
    {:error, Error.new(:missing_predicate, "delete requires a portable predicate")}
  end

  defp compile_delete(%Command{} = command, opts) do
    with {:ok, predicate} <- compile_predicate(command.predicate, opts, 0),
         {:ok, returning} <- compile_returning(command.returning) do
      {:ok,
       %{
         text:
           "DELETE FROM #{quote_relation(command.relation)} WHERE #{predicate.text}#{returning}",
         params: predicate.params
       }}
    end
  end

  defp compile_assignments(assignments, opts) do
    assignments
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, []}, fn {%{field: field, value: value}, index}, {:ok, acc} ->
      case compile_value(value, opts, index) do
        {:ok, %{text: text, params: params}} ->
          {:cont,
           {:ok,
            [%{field: field, column: quote_identifier(field), text: text, params: params} | acc]}}

        {:error, _} = error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, assignments} -> {:ok, Enum.reverse(assignments)}
      error -> error
    end
  end

  defp assignment_parts(assignments) do
    {Enum.map(assignments, & &1.column), Enum.map(assignments, & &1.text),
     Enum.flat_map(assignments, & &1.params)}
  end

  defp assignment_parameter_count(assignments),
    do: assignments |> Enum.flat_map(& &1.params) |> length()

  defp compile_foreign_key_guards(metadata, assignments, offset) do
    metadata
    |> Map.get(:foreign_key_guards, [])
    |> Enum.reduce_while({:ok, [], [], offset}, fn guard, {:ok, texts, params, next_offset} ->
      with %{field: field, relation: relation, target_field: target_field} <- guard,
           %{params: [value]} <-
             Enum.find(assignments, &(to_string(&1.field) == to_string(field))),
           true <- relation_ref?(relation) and field_ref?(target_field) do
        text =
          "EXISTS (SELECT 1 FROM #{quote_relation(relation)} WHERE #{quote_identifier(target_field)} = $#{next_offset + 1})"

        {:cont, {:ok, [text | texts], params ++ [value], next_offset + 1}}
      else
        _ ->
          {:halt,
           {:error,
            Error.new(
              :invalid_foreign_key_guard,
              "foreign-key guard must reference an assigned scalar value",
              details: %{guard: guard}
            )}}
      end
    end)
    |> case do
      {:ok, [], [], _offset} ->
        {:ok, %{text: nil, params: []}}

      {:ok, texts, params, _offset} ->
        {:ok, %{text: Enum.reverse(texts) |> Enum.join(" AND "), params: params}}

      error ->
        error
    end
  end

  defp guard_suffix(nil), do: ""
  defp guard_suffix(text), do: " AND " <> text

  defp relation_ref?(value) when is_atom(value), do: true
  defp relation_ref?(value) when is_binary(value), do: String.trim(value) != ""
  defp relation_ref?(_), do: false

  defp field_ref?(value) when is_atom(value), do: true
  defp field_ref?(value) when is_binary(value), do: String.trim(value) != ""
  defp field_ref?(_), do: false

  defp compile_predicate({:and, predicates}, opts, offset) when is_list(predicates),
    do: compile_predicate_list(predicates, " AND ", opts, offset)

  defp compile_predicate({:or, predicates}, opts, offset) when is_list(predicates),
    do: compile_predicate_list(predicates, " OR ", opts, offset)

  defp compile_predicate({:not, predicate}, opts, offset) do
    with {:ok, compiled} <- compile_predicate(predicate, opts, offset) do
      {:ok, %{text: "NOT (#{compiled.text})", params: compiled.params}}
    end
  end

  defp compile_predicate({:in, {:field, field}, values}, opts, offset)
       when is_list(values) and values != [] do
    values
    |> Enum.reduce_while({:ok, [], [], offset}, fn value, {:ok, texts, params, next_offset} ->
      case compile_value(value, opts, next_offset) do
        {:ok, compiled} ->
          {:cont,
           {:ok, [compiled.text | texts], params ++ compiled.params,
            next_offset + length(compiled.params)}}

        {:error, _} = error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, texts, params, _next_offset} ->
        {:ok,
         %{
           text: "#{quote_identifier(field)} IN (#{texts |> Enum.reverse() |> Enum.join(", ")})",
           params: params
         }}

      error ->
        error
    end
  end

  defp compile_predicate({operator, {:field, field}, value}, opts, offset)
       when operator in [:eq, :neq, :gt, :gte, :lt, :lte] do
    with {:ok, compiled_value} <- compile_value(value, opts, offset) do
      operator_text = %{eq: "=", neq: "!=", gt: ">", gte: ">=", lt: "<", lte: "<="}[operator]

      {:ok,
       %{
         text: "#{quote_identifier(field)} #{operator_text} #{compiled_value.text}",
         params: compiled_value.params
       }}
    end
  end

  defp compile_predicate({:is_null, {:field, field}}, _opts, _offset),
    do: {:ok, %{text: "#{quote_identifier(field)} IS NULL", params: []}}

  defp compile_predicate({:not_null, {:field, field}}, _opts, _offset),
    do: {:ok, %{text: "#{quote_identifier(field)} IS NOT NULL", params: []}}

  defp compile_predicate(predicate, _opts, _offset) do
    {:error,
     Error.new(:invalid_predicate, "unsupported portable PostgreSQL predicate",
       details: %{predicate: predicate}
     )}
  end

  defp compile_predicate_list([], _separator, _opts, _offset) do
    {:error, Error.new(:invalid_predicate, "boolean predicate groups must not be empty")}
  end

  defp compile_predicate_list(predicates, separator, opts, offset) do
    predicates
    |> Enum.reduce_while({:ok, [], [], offset}, fn predicate, {:ok, texts, params, next_offset} ->
      case compile_predicate(predicate, opts, next_offset) do
        {:ok, %{text: text, params: predicate_params}} ->
          {:cont,
           {:ok, [text | texts], params ++ predicate_params,
            next_offset + length(predicate_params)}}

        {:error, _} = error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, texts, params, _offset} ->
        {:ok,
         %{text: "(" <> (texts |> Enum.reverse() |> Enum.join(separator)) <> ")", params: params}}

      error ->
        error
    end
  end

  defp compile_value({:literal, {:system, :now}}, _opts, _offset),
    do: {:ok, %{text: "CURRENT_TIMESTAMP", params: []}}

  defp compile_value({:literal, value}, _opts, offset), do: parameter(value, offset)

  defp compile_value({:context, key}, opts, offset) do
    context = Keyword.get(opts, :context, %{})

    case fetch_context(context, key) do
      {:ok, value} ->
        parameter(value, offset)

      :error ->
        {:error,
         Error.new(:missing_context, "required write context value is missing",
           details: %{key: key}
         )}
    end
  end

  defp compile_value({:field, field}, _opts, _offset),
    do: {:ok, %{text: quote_identifier(field), params: []}}

  defp compile_value({:unsafe_sql, _}, _opts, _offset),
    do: {:error, Error.new(:invalid_command, "raw SQL is not allowed in portable writes")}

  defp compile_value({:unsafe_fragment, _}, _opts, _offset),
    do: {:error, Error.new(:invalid_command, "raw SQL is not allowed in portable writes")}

  defp compile_value(value, _opts, offset), do: parameter(value, offset)

  defp parameter(value, offset), do: {:ok, %{text: "$#{offset + 1}", params: [value]}}

  defp compile_returning(:none), do: {:ok, ""}
  defp compile_returning(:all), do: {:ok, " RETURNING *"}

  defp compile_returning(fields) when is_list(fields) do
    {:ok, " RETURNING " <> Enum.map_join(fields, ", ", &quote_identifier/1)}
  end

  defp compile_returning(value) do
    {:error,
     Error.new(:invalid_command, "invalid returning specification", details: %{returning: value})}
  end

  defp compile_conflict_target(metadata) do
    case Map.get(metadata, :conflict_target) do
      fields when is_list(fields) and fields != [] ->
        {:ok, Enum.map_join(fields, ", ", &quote_identifier/1)}

      _ ->
        {:error,
         Error.new(:invalid_command, "upsert requires a non-empty conflict target",
           details: %{required: :conflict_target}
         )}
    end
  end

  defp map_commands(commands, fun) do
    commands
    |> Enum.reduce_while({:ok, []}, fn command, {:ok, acc} ->
      case fun.(command) do
        {:ok, statement} -> {:cont, {:ok, [statement | acc]}}
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, statements} -> {:ok, Enum.reverse(statements)}
      error -> error
    end
  end

  defp quote_relation(relation) do
    relation
    |> to_string()
    |> String.split(".")
    |> Enum.map_join(".", &quote_identifier/1)
  end

  defp quote_identifier(identifier) do
    identifier
    |> to_string()
    |> String.replace("\"", "\"\"")
    |> then(&"\"#{&1}\"")
  end

  defp fetch_context(context, key) when is_map(context) do
    cond do
      Map.has_key?(context, key) ->
        {:ok, Map.fetch!(context, key)}

      is_atom(key) and Map.has_key?(context, Atom.to_string(key)) ->
        {:ok, Map.fetch!(context, Atom.to_string(key))}

      true ->
        case Enum.find(context, fn {context_key, _value} ->
               is_atom(context_key) and Atom.to_string(context_key) == to_string(key)
             end) do
          {_, value} -> {:ok, value}
          nil -> :error
        end
    end
  end

  defp fetch_context(_context, _key), do: :error
end
