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
    with :ok <- reject_upsert_predicate(command),
         {:ok, assignments} <- compile_assignments(command.assignments, opts),
         true <-
           assignments != [] or
             {:error, Error.new(:invalid_command, "upsert requires at least one assignment")},
         {:ok, conflict_target} <- compile_conflict_target(command.metadata),
         {:ok, update_assignments} <-
           compile_upsert_update_fields(command.metadata, assignments),
         {:ok, returning} <- compile_returning(command.returning) do
      {columns, values, params} = assignment_parts(assignments)

      with {:ok, guards} <-
             compile_foreign_key_guards(command.metadata, assignments, length(params)) do
        conflict_action = compile_conflict_action(update_assignments)

        values_clause =
          case guards.text do
            nil -> "VALUES (#{Enum.join(values, ", ")})"
            text -> "SELECT #{Enum.join(values, ", ")} WHERE #{text}"
          end

        {:ok,
         %{
           text:
             "INSERT INTO #{quote_relation(command.relation)} (#{Enum.join(columns, ", ")}) #{values_clause} ON CONFLICT (#{conflict_target}) #{conflict_action}#{returning}",
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
    |> Enum.reduce_while({:ok, [], 0}, fn %{field: field, value: value},
                                          {:ok, acc, next_offset} ->
      case compile_value(value, opts, next_offset) do
        {:ok, %{text: text, params: params}} ->
          {:cont,
           {:ok,
            [%{field: field, column: quote_identifier(field), text: text, params: params} | acc],
            next_offset + length(params)}}

        {:error, _} = error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, assignments, _offset} -> {:ok, Enum.reverse(assignments)}
      error -> error
    end
  end

  defp assignment_parts(assignments) do
    {Enum.map(assignments, & &1.column), Enum.map(assignments, & &1.text),
     Enum.flat_map(assignments, & &1.params)}
  end

  defp assignment_parameter_count(assignments),
    do: assignments |> Enum.flat_map(& &1.params) |> length()

  # A guard proves the referenced row exists. A guard that names
  # `tenant_field` must also carry `tenant_value`; the referenced row must then
  # belong to that tenant. Its columns are qualified by a subquery alias so a
  # column missing from the referenced relation fails instead of resolving to
  # the outer write target.
  defp compile_foreign_key_guards(metadata, assignments, offset) do
    metadata
    |> Map.get(:foreign_key_guards, [])
    |> Enum.reduce_while({:ok, [], [], offset}, fn guard, {:ok, texts, params, next_offset} ->
      with %{field: field, relation: relation, target_field: target_field} <- guard,
           %{params: [value]} <-
             Enum.find(assignments, &(to_string(&1.field) == to_string(field))),
           true <- relation_ref?(relation) and field_ref?(target_field),
           {:ok, tenant} <- foreign_key_guard_tenant(guard) do
        {text, guard_params} =
          foreign_key_guard_text(relation, target_field, value, tenant, next_offset)

        {:cont, {:ok, [text | texts], params ++ guard_params, next_offset + length(guard_params)}}
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

  defp foreign_key_guard_tenant(guard) do
    case {Map.fetch(guard, :tenant_field), Map.get(guard, :tenant_value)} do
      {:error, _value} ->
        {:ok, nil}

      {{:ok, tenant_field}, tenant_value} when not is_nil(tenant_value) ->
        if field_ref?(tenant_field), do: {:ok, {tenant_field, tenant_value}}, else: :error

      _invalid ->
        :error
    end
  end

  defp foreign_key_guard_text(relation, target_field, value, nil, offset) do
    {"EXISTS (SELECT 1 FROM #{quote_relation(relation)} WHERE #{quote_identifier(target_field)} = $#{offset + 1})",
     [value]}
  end

  defp foreign_key_guard_text(relation, target_field, value, {tenant_field, tenant}, offset) do
    alias_name = quote_identifier("selecto_fk_parent")

    {"EXISTS (SELECT 1 FROM #{quote_relation(relation)} AS #{alias_name} " <>
       "WHERE #{alias_name}.#{quote_identifier(target_field)} = $#{offset + 1} " <>
       "AND #{alias_name}.#{quote_identifier(tenant_field)} = $#{offset + 2})", [value, tenant]}
  end

  defp guard_suffix(nil), do: ""
  defp guard_suffix(text), do: " AND " <> text

  defp relation_ref?(value) when is_atom(value), do: true
  defp relation_ref?(value) when is_binary(value), do: String.trim(value) != ""
  defp relation_ref?(_), do: false

  defp field_ref?(value) when is_atom(value), do: true
  defp field_ref?(value) when is_binary(value), do: String.trim(value) != ""
  defp field_ref?(_), do: false

  @doc false
  def compile_predicate(predicate, opts \\ [], offset \\ 0)

  def compile_predicate({:and, predicates}, opts, offset) when is_list(predicates),
    do: compile_predicate_list(predicates, " AND ", opts, offset)

  def compile_predicate({:or, predicates}, opts, offset) when is_list(predicates),
    do: compile_predicate_list(predicates, " OR ", opts, offset)

  def compile_predicate({:not, predicate}, opts, offset) do
    with {:ok, compiled} <- compile_predicate(predicate, opts, offset) do
      {:ok, %{text: "NOT (#{compiled.text})", params: compiled.params}}
    end
  end

  def compile_predicate({:in, {:field, field}, values}, opts, offset)
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
           text: "#{quote_field(field, opts)} IN (#{texts |> Enum.reverse() |> Enum.join(", ")})",
           params: params
         }}

      error ->
        error
    end
  end

  def compile_predicate({operator, {:field, field}, value}, opts, offset)
      when operator in [:eq, :neq, :gt, :gte, :lt, :lte] do
    with {:ok, compiled_value} <- compile_value(value, opts, offset) do
      operator_text = %{eq: "=", neq: "!=", gt: ">", gte: ">=", lt: "<", lte: "<="}[operator]

      {:ok,
       %{
         text: "#{quote_field(field, opts)} #{operator_text} #{compiled_value.text}",
         params: compiled_value.params
       }}
    end
  end

  def compile_predicate({:is_null, {:field, field}}, opts, _offset),
    do: {:ok, %{text: "#{quote_field(field, opts)} IS NULL", params: []}}

  def compile_predicate({:not_null, {:field, field}}, opts, _offset),
    do: {:ok, %{text: "#{quote_field(field, opts)} IS NOT NULL", params: []}}

  def compile_predicate(predicate, _opts, _offset) do
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

  @doc false
  def compile_value(value, opts \\ [], offset \\ 0)

  def compile_value({:literal, {:system, :now}}, _opts, _offset),
    do: {:ok, %{text: "CURRENT_TIMESTAMP", params: []}}

  def compile_value({:literal, value}, _opts, offset), do: parameter(value, offset)

  def compile_value({:context, key}, opts, offset) do
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

  def compile_value({:field, field}, opts, _offset),
    do: {:ok, %{text: quote_field(field, opts), params: []}}

  def compile_value({:unsafe_sql, _}, _opts, _offset),
    do: {:error, Error.new(:invalid_command, "raw SQL is not allowed in portable writes")}

  def compile_value({:unsafe_fragment, _}, _opts, _offset),
    do: {:error, Error.new(:invalid_command, "raw SQL is not allowed in portable writes")}

  def compile_value(value, _opts, offset), do: parameter(value, offset)

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

  # ON CONFLICT ... DO UPDATE has no mutation predicate here; a scope predicate
  # on an upsert command would be silently dropped, so it is refused.
  defp reject_upsert_predicate(%Command{predicate: nil}), do: :ok

  defp reject_upsert_predicate(%Command{}) do
    {:error,
     Error.new(:invalid_command, "PostgreSQL upsert cannot enforce a command predicate",
       details: %{code: :upsert_predicate_unsupported}
     )}
  end

  defp compile_conflict_target(metadata) do
    case Map.get(metadata, :conflict_target) do
      fields when is_list(fields) and fields != [] ->
        with :ok <- ensure_declared_conflict_target(fields, metadata) do
          {:ok, Enum.map_join(fields, ", ", &quote_identifier/1)}
        end

      _ ->
        {:error,
         Error.new(:invalid_command, "upsert requires a non-empty conflict target",
           details: %{required: :conflict_target}
         )}
    end
  end

  # When the producer publishes the domain's declared targets, the selected
  # target must be one of them (column order is not significant).
  defp ensure_declared_conflict_target(fields, metadata) do
    case Map.fetch(metadata, :declared_conflict_targets) do
      :error ->
        :ok

      {:ok, declared} when is_list(declared) ->
        target = field_set(fields)

        if not is_nil(target) and Enum.any?(declared, &(field_set(&1) == target)) do
          :ok
        else
          undeclared_conflict_target(fields)
        end

      {:ok, _declared} ->
        undeclared_conflict_target(fields)
    end
  end

  defp field_set(fields) when is_list(fields) do
    if Enum.all?(fields, &field_ref?/1),
      do: fields |> Enum.map(&to_string/1) |> MapSet.new(),
      else: nil
  end

  defp field_set(_fields), do: nil

  defp undeclared_conflict_target(fields) do
    {:error,
     Error.new(:invalid_command, "upsert conflict target is not a declared conflict target",
       details: %{code: :undeclared_conflict_target, conflict_target: fields}
     )}
  end

  defp compile_upsert_update_fields(metadata, assignments) do
    case Map.fetch(metadata, :upsert_update_fields) do
      {:ok, fields} when is_list(fields) ->
        normalized_fields = Enum.map(fields, &to_string/1)
        assigned_fields = MapSet.new(assignments, &to_string(&1.field))

        cond do
          Enum.any?(fields, &(not field_ref?(&1))) ->
            invalid_upsert_update_fields(fields, :invalid_field)

          length(normalized_fields) != MapSet.size(MapSet.new(normalized_fields)) ->
            invalid_upsert_update_fields(fields, :duplicate_field)

          Enum.any?(normalized_fields, &(not MapSet.member?(assigned_fields, &1))) ->
            invalid_upsert_update_fields(fields, :field_not_assigned)

          true ->
            update_fields = MapSet.new(normalized_fields)

            {:ok, Enum.filter(assignments, &MapSet.member?(update_fields, to_string(&1.field)))}
        end

      _ ->
        {:error,
         Error.new(
           :invalid_command,
           "upsert requires a domain-governed update field list",
           details: %{required: :upsert_update_fields}
         )}
    end
  end

  defp invalid_upsert_update_fields(fields, reason) do
    {:error,
     Error.new(:invalid_command, "invalid domain-governed upsert update field list",
       details: %{upsert_update_fields: fields, reason: reason}
     )}
  end

  defp compile_conflict_action([]), do: "DO NOTHING"

  defp compile_conflict_action(assignments) do
    update_set =
      Enum.map_join(assignments, ", ", fn assignment ->
        "#{assignment.column} = EXCLUDED.#{assignment.column}"
      end)

    "DO UPDATE SET " <> update_set
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

  defp quote_field(field, opts) do
    case Keyword.get(opts, :predicate_relation_alias) do
      nil -> quote_identifier(field)
      alias_name -> quote_identifier(alias_name) <> "." <> quote_identifier(field)
    end
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
