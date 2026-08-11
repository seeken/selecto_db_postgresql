defmodule SelectoDBPostgreSQL.GraphCompiler do
  @moduledoc false

  alias Selecto.Write.{Command, Error, Graph, Preview, Result}
  alias Selecto.Write.Graph.{Binding, Node, Row}
  alias SelectoDBPostgreSQL.WriteCompiler

  @row_id_column "__selecto_row_id"
  @existing_column "__selecto_existing"
  @action_column "__selecto_action"

  @spec preview(Graph.t(), pos_integer(), keyword()) ::
          {:ok, Preview.t()} | {:error, Error.t()}
  def preview(%Graph{} = graph, server_major, opts) do
    graph.nodes
    |> Enum.reduce_while({:ok, [], %{}, []}, fn node, {:ok, statements, results, merge_nodes} ->
      with {:ok, materialized} <- materialize_node(node, results),
           {:ok, node_statements, used_merge?} <-
             preview_node(materialized, server_major, results, opts) do
        next_results = Map.merge(results, symbolic_results(materialized))
        merge_nodes = if used_merge?, do: merge_nodes ++ [node.id], else: merge_nodes
        {:cont, {:ok, statements ++ node_statements, next_results, merge_nodes}}
      else
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, statements, _results, merge_nodes} ->
        {:ok,
         %Preview{
           statements: statements,
           metadata: %{
             dialect: :postgresql,
             atomic?: true,
             graph?: true,
             server_major: server_major,
             merge_nodes: merge_nodes
           }
         }}

      error ->
        error
    end
  end

  @spec materialize_node(Node.t(), map()) :: {:ok, Node.t()} | {:error, Error.t()}
  def materialize_node(%Node{} = node, results) when is_map(results) do
    with {:ok, sync_predicate} <- resolve_generated(node.sync_predicate, results),
         {:ok, rows} <- materialize_rows(node.rows, results) do
      {:ok, %{node | sync_predicate: sync_predicate, rows: rows}}
    end
  end

  @spec merge_eligible?(Node.t(), pos_integer()) :: boolean()
  def merge_eligible?(%Node{strategy: :sync, rows: rows} = node, server_major)
      when server_major >= 17 and rows != [] do
    Enum.all?(rows, &merge_row_eligible?(&1, node)) and uniform_assignments?(rows) and
      merge_types_available?(node)
  end

  def merge_eligible?(_node, _server_major), do: false

  @spec compile_merge(Node.t(), keyword()) ::
          {:ok, %{text: String.t(), params: [term()], metadata: map()}}
          | {:error, Error.t()}
  def compile_merge(%Node{} = node, opts) do
    identity_fields = Enum.map(node.identity_fields, &to_string/1)
    update_rows = Enum.filter(node.rows, &match?(%Row{command: %{operation: :update}}, &1))
    insert_rows = Enum.filter(node.rows, &match?(%Row{command: %{operation: :insert}}, &1))
    update_fields = assignment_fields(update_rows)
    insert_fields = assignment_fields(insert_rows)
    source_fields = Enum.uniq(identity_fields ++ update_fields ++ insert_fields)

    with true <-
           node.strategy == :sync or
             {:error, Error.new(:invalid_graph, "MERGE requires a sync graph node")},
         true <-
           merge_types_available?(node) or
             {:error,
              Error.new(:invalid_graph, "MERGE requires portable types for every source field")},
         true <-
           node.rows != [] or
             {:error, Error.new(:invalid_graph, "MERGE requires at least one source row")},
         {:ok, source} <-
           compile_source_rows(node.rows, source_fields, node.field_types, opts),
         {:ok, ownership} <-
           WriteCompiler.compile_predicate(
             node.sync_predicate,
             Keyword.put(opts, :predicate_relation_alias, "target"),
             length(source.params)
           ),
         {:ok, returning_fields} <- returning_fields(node.rows) do
      on =
        Enum.map_join(identity_fields, " AND ", fn field ->
          "target.#{quote_identifier(field)} = source.#{quote_identifier(field)}"
        end)

      clauses =
        []
        |> add_update_clause(update_rows, update_fields, ownership.text)
        |> add_insert_clause(insert_rows, insert_fields)
        |> add_delete_clause(node.delete_missing?, ownership.text)

      returning =
        [
          "source.#{quote_identifier(@row_id_column)} AS #{quote_identifier(@row_id_column)}",
          "merge_action() AS #{quote_identifier(@action_column)}"
        ] ++
          Enum.map(returning_fields, fn field ->
            "target.#{quote_identifier(field)} AS #{quote_identifier(field)}"
          end)

      columns = [@row_id_column, @existing_column | source_fields]

      {:ok,
       %{
         text:
           "MERGE INTO #{quote_relation(node.relation)} AS target " <>
             "USING (VALUES #{source.text}) AS source (#{Enum.map_join(columns, ", ", &quote_identifier/1)}) " <>
             "ON #{on} #{Enum.join(clauses, " ")} RETURNING #{Enum.join(returning, ", ")}",
         params: source.params ++ ownership.params,
         metadata: %{
           strategy: :merge,
           node_id: node.id,
           source_rows: length(node.rows),
           returning_fields: returning_fields
         }
       }}
    else
      {:error, _} = error -> error
    end
  end

  @spec delete_missing_command(Node.t(), map()) ::
          {:ok, Command.t() | nil} | {:error, Error.t()}
  def delete_missing_command(%Node{delete_missing?: false}, _row_results), do: {:ok, nil}

  def delete_missing_command(%Node{} = node, row_results) do
    identities =
      Enum.reduce_while(node.rows, {:ok, []}, fn row, {:ok, identities} ->
        case identity_from_result(row, node.identity_fields, row_results) do
          {:ok, identity} -> {:cont, {:ok, [identity | identities]}}
          {:error, _} = error -> {:halt, error}
        end
      end)

    with {:ok, identities} <- identities do
      identity_exclusion =
        identities
        |> Enum.reverse()
        |> Enum.map(&identity_predicate/1)
        |> case do
          [] -> nil
          [predicate] -> {:not, predicate}
          predicates -> {:not, {:or, predicates}}
        end

      predicate = conjunction([node.sync_predicate, identity_exclusion])

      Command.new(%{
        operation: :delete,
        relation: node.relation,
        predicate: predicate,
        expected_cardinality: :many,
        returning: :none,
        required_capabilities: [:delete],
        metadata: %{graph_cleanup: true, graph_node: node.id}
      })
    end
  end

  @spec merge_results(Node.t(), map()) ::
          {:ok, {map(), non_neg_integer()}} | {:error, Error.t()}
  def merge_results(%Node{} = node, %{rows: rows, columns: columns} = query_result) do
    mapped_rows = Enum.map(rows || [], &Map.new(Enum.zip(columns || [], &1)))

    source_rows =
      Enum.reject(mapped_rows, fn row -> is_nil(fetch_value(row, @row_id_column)) end)

    returned_ids = Enum.map(source_rows, &to_string(fetch_value(&1, @row_id_column)))
    expected_ids = Enum.map(node.rows, & &1.id)

    if Enum.sort(returned_ids) == Enum.sort(expected_ids) do
      results =
        Map.new(node.rows, fn row ->
          returned =
            Enum.find(source_rows, &(to_string(fetch_value(&1, @row_id_column)) == row.id))

          action = fetch_value(returned, @action_column)

          data =
            returned
            |> Map.drop([@row_id_column, @action_column])

          {row.id,
           %Result{
             operation: normalize_merge_action(action, row.command.operation),
             affected_rows: 1,
             rows: [data],
             metadata: %{dialect: :postgresql, strategy: :merge}
           }}
        end)

      affected = Map.get(query_result, :num_rows, length(mapped_rows))
      {:ok, {results, affected}}
    else
      {:error,
       Error.new(:cardinality_mismatch, "MERGE did not apply every submitted graph row",
         details: %{expected_row_ids: expected_ids, actual_row_ids: returned_ids, node: node.id}
       )}
    end
  end

  defp preview_node(node, server_major, results, opts) do
    if merge_eligible?(node, server_major) do
      with {:ok, statement} <- compile_merge(node, opts) do
        {:ok, [statement], true}
      end
    else
      with {:ok, commands} <- fallback_commands(node, results),
           {:ok, statements} <- compile_commands(commands, opts) do
        {:ok, statements, false}
      end
    end
  end

  defp fallback_commands(%Node{strategy: :ordered} = node, _results),
    do: {:ok, Enum.map(node.rows, & &1.command)}

  defp fallback_commands(%Node{strategy: :sync} = node, _results) do
    row_results =
      node
      |> symbolic_results()
      |> Map.new(fn {{_node_id, row_id}, result} -> {row_id, result} end)

    with {:ok, cleanup} <- delete_missing_command(node, row_results) do
      commands = Enum.map(node.rows, & &1.command)
      {:ok, if(is_nil(cleanup), do: commands, else: commands ++ [cleanup])}
    end
  end

  defp compile_commands(commands, opts) do
    Enum.reduce_while(commands, {:ok, []}, fn command, {:ok, statements} ->
      case WriteCompiler.compile(command, opts) do
        {:ok, statement} -> {:cont, {:ok, statements ++ [statement]}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp materialize_rows(rows, results) do
    Enum.reduce_while(rows, {:ok, []}, fn row, {:ok, materialized} ->
      case materialize_row(row, results) do
        {:ok, row} -> {:cont, {:ok, materialized ++ [row]}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp materialize_row(%Row{} = row, results) do
    Enum.reduce_while(row.bindings, {:ok, row.command}, fn binding, {:ok, command} ->
      with {:ok, value} <- binding_value(binding, results),
           {:ok, command} <- apply_binding(command, binding.field, value) do
        {:cont, {:ok, command}}
      else
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, command} -> {:ok, %{row | command: command}}
      error -> error
    end
  end

  defp binding_value(%Binding{} = binding, results) do
    case Map.get(results, {binding.from_node, binding.from_row}) do
      %Result{rows: [row | _], metadata: metadata} ->
        case fetch_result_value(row, binding.from_field) do
          {:ok, value} ->
            {:ok, value}

          :error ->
            case metadata do
              %{symbolic_source: {node_id, row_id}} ->
                {:ok, {:generated, node_id, row_id, binding.from_field}}

              _ ->
                missing_binding(binding)
            end
        end

      _ ->
        missing_binding(binding)
    end
  end

  defp missing_binding(binding) do
    {:error,
     Error.new(:generated_value_missing, "generated graph binding value is unavailable",
       details: %{
         source: {binding.from_node, binding.from_row},
         field: binding.from_field
       }
     )}
  end

  defp apply_binding(%Command{operation: operation} = command, field, value)
       when operation in [:insert, :upsert] do
    {:ok,
     %{command | assignments: command.assignments ++ [%{field: field, value: {:literal, value}}]}}
  end

  defp apply_binding(%Command{operation: operation} = command, field, value)
       when operation in [:update, :delete] do
    predicate = conjunction([command.predicate, {:eq, {:field, field}, {:literal, value}}])
    {:ok, %{command | predicate: predicate}}
  end

  defp resolve_generated(nil, _results), do: {:ok, nil}

  defp resolve_generated({:generated, node_id, row_id, field}, results) do
    binding_value(
      %Binding{field: field, from_node: node_id, from_row: row_id, from_field: field},
      results
    )
    |> case do
      {:ok, value} -> {:ok, {:literal, value}}
      error -> error
    end
  end

  defp resolve_generated(tuple, results) when is_tuple(tuple) do
    tuple
    |> Tuple.to_list()
    |> resolve_generated_list(results)
    |> case do
      {:ok, values} -> {:ok, List.to_tuple(values)}
      error -> error
    end
  end

  defp resolve_generated(list, results) when is_list(list),
    do: resolve_generated_list(list, results)

  defp resolve_generated(value, _results), do: {:ok, value}

  defp resolve_generated_list(values, results) do
    Enum.reduce_while(values, {:ok, []}, fn value, {:ok, resolved} ->
      case resolve_generated(value, results) do
        {:ok, value} -> {:cont, {:ok, resolved ++ [value]}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp symbolic_results(%Node{} = node) do
    Map.new(node.rows, fn row ->
      fields = command_returning_fields(row.command)

      data =
        Map.new(fields, fn field ->
          {to_string(field), {:generated, node.id, row.id, field}}
        end)

      {{node.id, row.id},
       %Result{
         operation: row.command.operation,
         affected_rows: 1,
         rows: [data],
         metadata: %{symbolic_source: {node.id, row.id}}
       }}
    end)
  end

  defp merge_row_eligible?(%Row{command: %Command{} = command} = row, node) do
    no_guards? = Map.get(command.metadata, :foreign_key_guards, []) == []

    command.operation in [:insert, :update] and no_guards? and
      command.returning != :all and
      (command.operation == :insert or governed_update_predicate?(row, node))
  end

  defp governed_update_predicate?(row, node) do
    identity = Map.get(row.metadata, :identity, %{})

    identity_predicates =
      Enum.map(node.identity_fields, fn field ->
        {:eq, {:field, field}, {:literal, fetch_value(identity, field)}}
      end)

    actual = row.command.predicate |> flatten_and() |> canonical_predicates()

    expected =
      [node.sync_predicate | identity_predicates]
      |> Enum.flat_map(&flatten_and/1)
      |> canonical_predicates()

    actual == expected
  end

  defp uniform_assignments?(rows) do
    rows
    |> Enum.group_by(& &1.command.operation)
    |> Enum.all?(fn {_operation, operation_rows} ->
      operation_rows
      |> Enum.map(fn row ->
        Enum.map(row.command.assignments, &to_string(&1.field)) |> Enum.sort()
      end)
      |> Enum.uniq()
      |> length() == 1
    end)
  end

  defp merge_types_available?(node) do
    fields =
      Enum.uniq(Enum.map(node.identity_fields, &to_string/1) ++ all_assignment_fields(node.rows))

    Enum.all?(fields, fn field ->
      not is_nil(postgres_cast(fetch_value(node.field_types, field)))
    end)
  end

  defp canonical_predicates(predicates), do: predicates |> Enum.map(&inspect/1) |> Enum.sort()
  defp flatten_and({:and, predicates}), do: Enum.flat_map(predicates, &flatten_and/1)
  defp flatten_and(nil), do: []
  defp flatten_and(predicate), do: [predicate]

  defp compile_source_rows(rows, fields, field_types, opts) do
    types = [:string, :boolean] ++ Enum.map(fields, &fetch_value(field_types, &1))

    rows
    |> Enum.reduce_while({:ok, [], [], 0}, fn row, {:ok, sql_rows, params, offset} ->
      values =
        [row.id, row.command.operation == :update] ++ Enum.map(fields, &source_value(row, &1))

      case compile_source_values(Enum.zip(values, types), opts, offset) do
        {:ok, texts, row_params} ->
          {:cont,
           {:ok, sql_rows ++ ["(" <> Enum.join(texts, ", ") <> ")"], params ++ row_params,
            offset + length(row_params)}}

        {:error, _} = error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, sql_rows, params, _offset} ->
        {:ok, %{text: Enum.join(sql_rows, ", "), params: params}}

      error ->
        error
    end
  end

  defp compile_source_values(values_and_types, opts, offset) do
    Enum.reduce_while(values_and_types, {:ok, [], [], offset}, fn {value, type},
                                                                  {:ok, texts, params, next} ->
      case WriteCompiler.compile_value(normalize_source_value(value), opts, next) do
        {:ok, compiled} ->
          text =
            if compiled.params == [],
              do: compiled.text,
              else: compiled.text <> postgres_cast(type)

          {:cont,
           {:ok, texts ++ [text], params ++ compiled.params, next + length(compiled.params)}}

        {:error, _} = error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, texts, params, _next} -> {:ok, texts, params}
      error -> error
    end
  end

  defp normalize_source_value({kind, _} = value) when kind in [:literal, :context, :field],
    do: value

  defp normalize_source_value(value), do: {:literal, value}

  defp source_value(row, field) do
    identity = Map.get(row.metadata, :identity, %{})

    case fetch_result_value(identity, field) do
      {:ok, value} ->
        value

      :error ->
        case Enum.find(row.command.assignments, &(to_string(&1.field) == to_string(field))) do
          %{value: value} -> value
          nil -> nil
        end
    end
  end

  defp assignment_fields([]), do: []

  defp assignment_fields([row | _]),
    do: Enum.map(row.command.assignments, &to_string(&1.field))

  defp all_assignment_fields(rows) do
    rows
    |> Enum.flat_map(fn row ->
      Enum.map(row.command.assignments, fn assignment -> to_string(assignment.field) end)
    end)
    |> Enum.uniq()
  end

  defp postgres_cast(type) when type in [:integer, "integer"], do: "::bigint"
  defp postgres_cast(type) when type in [:float, "float"], do: "::double precision"
  defp postgres_cast(type) when type in [:decimal, "decimal"], do: "::numeric"
  defp postgres_cast(type) when type in [:string, "string", :text, "text"], do: "::text"
  defp postgres_cast(type) when type in [:boolean, "boolean"], do: "::boolean"
  defp postgres_cast(type) when type in [:date, "date"], do: "::date"

  defp postgres_cast(type)
       when type in [:datetime, "datetime", :naive_datetime, "naive_datetime"],
       do: "::timestamp"

  defp postgres_cast(type) when type in [:utc_datetime, "utc_datetime"],
    do: "::timestamptz"

  defp postgres_cast(type) when type in [:uuid, "uuid"], do: "::uuid"
  defp postgres_cast(type) when type in [:binary, "binary"], do: "::bytea"
  defp postgres_cast(_type), do: nil

  defp add_update_clause(clauses, [], _fields, _ownership), do: clauses

  defp add_update_clause(clauses, _rows, fields, ownership) do
    set =
      Enum.map_join(fields, ", ", fn field ->
        "#{quote_identifier(field)} = source.#{quote_identifier(field)}"
      end)

    clauses ++
      [
        "WHEN MATCHED AND #{ownership} THEN UPDATE SET #{set}",
        "WHEN MATCHED THEN DO NOTHING"
      ]
  end

  defp add_insert_clause(clauses, [], _fields), do: clauses

  defp add_insert_clause(clauses, _rows, fields) do
    columns = Enum.map_join(fields, ", ", &quote_identifier/1)
    values = Enum.map_join(fields, ", ", &("source." <> quote_identifier(&1)))

    clauses ++
      [
        "WHEN NOT MATCHED AND source.#{quote_identifier(@existing_column)} = FALSE " <>
          "THEN INSERT (#{columns}) VALUES (#{values})"
      ]
  end

  defp add_delete_clause(clauses, true, ownership),
    do: clauses ++ ["WHEN NOT MATCHED BY SOURCE AND #{ownership} THEN DELETE"]

  defp add_delete_clause(clauses, false, _ownership), do: clauses

  defp returning_fields(rows) do
    fields =
      rows |> Enum.flat_map(&command_returning_fields(&1.command)) |> Enum.uniq_by(&to_string/1)

    {:ok, fields}
  end

  defp command_returning_fields(%Command{returning: :all}), do: []
  defp command_returning_fields(%Command{returning: :none}), do: []
  defp command_returning_fields(%Command{returning: fields}) when is_list(fields), do: fields

  defp identity_from_result(row, fields, results) do
    with %Result{rows: [data | _]} <- Map.get(results, row.id) do
      identity_from_map(data, fields, row)
    else
      _ -> missing_identity(row, fields)
    end
  end

  defp identity_from_map(data, fields, row) do
    identity =
      Enum.reduce_while(fields, {:ok, %{}}, fn field, {:ok, values} ->
        case fetch_result_value(data, field) do
          {:ok, value} when not is_nil(value) -> {:cont, {:ok, Map.put(values, field, value)}}
          _ -> {:halt, missing_identity(row, fields)}
        end
      end)

    identity
  end

  defp missing_identity(row, fields) do
    {:error,
     Error.new(:generated_value_missing, "sync row did not return its complete identity",
       details: %{row: row.id, fields: fields}
     )}
  end

  defp identity_predicate(identity) do
    identity
    |> Enum.map(fn {field, value} -> {:eq, {:field, field}, {:literal, value}} end)
    |> conjunction()
  end

  defp conjunction(values) do
    case Enum.reject(values, &is_nil/1) do
      [value] -> value
      values -> {:and, values}
    end
  end

  defp fetch_result_value(map, field) when is_map(map) do
    case Enum.find(map, fn {key, _value} -> to_string(key) == to_string(field) end) do
      {_key, value} -> {:ok, value}
      nil -> :error
    end
  end

  defp fetch_value(nil, _field), do: nil

  defp fetch_value(map, field) do
    case fetch_result_value(map, field) do
      {:ok, value} -> value
      :error -> nil
    end
  end

  defp normalize_merge_action(action, _fallback) when action in ["INSERT", "insert"], do: :insert
  defp normalize_merge_action(action, _fallback) when action in ["UPDATE", "update"], do: :update
  defp normalize_merge_action(_action, fallback), do: fallback

  defp quote_relation(relation) do
    relation |> to_string() |> String.split(".") |> Enum.map_join(".", &quote_identifier/1)
  end

  defp quote_identifier(identifier) do
    identifier
    |> to_string()
    |> String.replace("\"", "\"\"")
    |> then(&("\"" <> &1 <> "\""))
  end
end
