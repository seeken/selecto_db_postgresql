defmodule SelectoDBPostgreSQL.Dialect do
  @moduledoc false

  @behaviour Selecto.DB.Dialect

  alias Selecto.Dialect.TextSearch.{Predicate, Rank}
  alias Selecto.Dialect.Text.Normalization, as: TextNormalization
  alias Selecto.Dialect.Predicate.Comparison
  alias Selecto.Dialect.Bucket.Expression, as: BucketExpression
  alias Selecto.Dialect.Collection.Operation, as: CollectionOperation
  alias Selecto.Dialect.DateTime.Operation, as: DateTimeOperation
  alias Selecto.Dialect.Hierarchy.{Adjacency, MaterializedPath}
  alias Selecto.Dialect.TableFunction.Join, as: TableFunctionJoin
  alias Selecto.Dialect.Window.FrameBoundary
  alias Selecto.Dialect.View.{Definition, Index, Refresh}

  alias Selecto.Dialect.Json.{
    ArrayContains,
    ArrayContainsAll,
    Contains,
    Extraction,
    KeyExists,
    Operation
  }

  @impl true
  def render_interval(%Selecto.Dialect.Interval{amount: amount, unit: unit}, _selecto) do
    {:ok, ["interval '", Integer.to_string(amount), " ", Atom.to_string(unit), "'"]}
  end

  @impl true
  def render_datetime_operation(%DateTimeOperation{} = operation, _selecto) do
    expression = datetime_expression(operation)

    case operation.operation do
      :current_timestamp ->
        {:ok, "NOW()"}

      :truncate ->
        {:ok, ["DATE_TRUNC('", Atom.to_string(operation.part), "', ", expression, ")"]}

      :age when is_nil(operation.second_expression) ->
        {:ok, ["AGE(", expression, ")"]}

      :age ->
        {:ok, ["AGE(", expression, ", ", operation.second_expression, ")"]}

      :extract_part when operation.part == :weekday_sunday_one ->
        {:ok, ["CAST(TO_CHAR(", expression, ", 'D') AS INTEGER)"]}

      :extract_part ->
        {:ok, ["DATE_PART('", Atom.to_string(operation.part), "', ", expression, ")"]}

      :format ->
        format = operation.options |> Map.fetch!(:format) |> to_string() |> escape_literal()
        {:ok, ["TO_CHAR(", expression, ", '", format, "')"]}

      :elapsed_days ->
        {:ok, ["CURRENT_DATE - DATE(", expression, ")"]}

      :temporal_cutoff ->
        amount = Map.fetch!(operation.options, :amount)

        clock =
          case Map.fetch!(operation.options, :clock) do
            :current_date -> "CURRENT_DATE"
            :current_timestamp -> "NOW()"
          end

        {:ok,
         {[
            operation.expression,
            " > (",
            clock,
            " - (",
            {:param, amount},
            " * INTERVAL '1 ",
            Atom.to_string(operation.part),
            "'))"
          ], [amount]}}

      unsupported ->
        {:error,
         Selecto.Error.validation_error("PostgreSQL does not support this datetime operation", %{
           operation: unsupported,
           unsupported_feature: :datetime_operation
         })}
    end
  end

  defp datetime_expression(%DateTimeOperation{} = operation) do
    expression =
      case Map.get(operation.options, :epoch_storage) do
        :unix_seconds -> ["TO_TIMESTAMP(", operation.expression, ")"]
        :unix_milliseconds -> ["TO_TIMESTAMP((", operation.expression, ") / 1000.0)"]
        _ -> operation.expression
      end

    case Map.get(operation.options, :timezone) do
      timezone when is_binary(timezone) and timezone != "" ->
        timezone = escape_literal(timezone)

        case Map.get(operation.options, :epoch_storage) do
          storage when storage in [:unix_seconds, :unix_milliseconds] ->
            [expression, " AT TIME ZONE '", timezone, "'"]

          _ ->
            storage_timezone =
              operation.options
              |> Map.get(:storage_timezone, "Etc/UTC")
              |> to_string()
              |> escape_literal()

            [
              "(",
              expression,
              " AT TIME ZONE '",
              storage_timezone,
              "') AT TIME ZONE '",
              timezone,
              "'"
            ]
        end

      _ ->
        expression
    end
  end

  @impl true
  def render_text_normalization(%TextNormalization{} = normalization, _selecto) do
    {:ok, text_normalization_sql(normalization)}
  end

  @impl true
  def render_comparison(%Comparison{} = comparison, _selecto) do
    operator =
      case comparison.operation do
        :case_insensitive_like -> "ILIKE"
        :case_insensitive_not_like -> "NOT ILIKE"
      end

    {:ok, [comparison.left, " ", operator, " ", comparison.right]}
  end

  @impl true
  def render_bucket(%BucketExpression{} = bucket, _selecto) do
    with :ok <- validate_bucket(bucket) do
      {:ok, bucket_sql(bucket)}
    end
  end

  defp bucket_sql(%BucketExpression{kind: :numeric_ranges} = bucket) do
    range_case_sql(bucket.expression, bucket.ranges)
  end

  defp bucket_sql(%BucketExpression{kind: :numeric_increment} = bucket) do
    increment_bucket_sql(bucket.expression, bucket.increment)
  end

  defp bucket_sql(%BucketExpression{kind: :year_increment} = bucket) do
    expression = bucket_datetime_expression(bucket)
    increment_bucket_sql(["DATE_PART('year', ", expression, ")"], bucket.increment)
  end

  defp bucket_sql(%BucketExpression{kind: :date_relative_ranges} = bucket) do
    expression = bucket_datetime_expression(bucket)
    date_range_case_sql(["DATE(", expression, ")"], bucket.ranges)
  end

  defp bucket_sql(%BucketExpression{kind: :elapsed_days_ranges} = bucket) do
    expression = bucket_datetime_expression(bucket)
    range_case_sql(["CURRENT_DATE - DATE(", expression, ")"], bucket.ranges)
  end

  defp bucket_sql(%BucketExpression{kind: :year_ranges} = bucket) do
    expression = bucket_datetime_expression(bucket)
    range_case_sql(["DATE_PART('year', ", expression, ")"], bucket.ranges)
  end

  defp bucket_sql(%BucketExpression{kind: :text_prefix} = bucket) do
    normalized =
      text_normalization_sql(%TextNormalization{
        expression: bucket.expression,
        exclude_articles: bucket.exclude_articles,
        ignore_case: bucket.ignore_case
      })

    [
      "CASE WHEN ",
      normalized,
      " = '' THEN 'Other' ELSE UPPER(LEFT(",
      normalized,
      ", ",
      Integer.to_string(bucket.prefix_length),
      ")) END"
    ]
  end

  defp increment_bucket_sql(expression, increment_value) do
    increment = Integer.to_string(increment_value)

    bucket_start = [
      "CAST(FLOOR(CAST(",
      expression,
      " AS NUMERIC) / ",
      increment,
      ") AS BIGINT) * ",
      increment
    ]

    [
      "CASE WHEN ",
      expression,
      " IS NULL THEN 'Other' ELSE CAST((",
      bucket_start,
      ") AS TEXT) || '-' || CAST(((",
      bucket_start,
      ") + ",
      Integer.to_string(increment_value - 1),
      ") AS TEXT) END"
    ]
  end

  defp range_case_sql(expression, ranges) do
    clauses =
      Enum.map(ranges, fn
        {min, max, label} when is_integer(min) and is_integer(max) and min == max ->
          [
            "WHEN ",
            expression,
            " = ",
            Integer.to_string(min),
            " THEN '",
            escape_literal(label),
            "'"
          ]

        {min, max, label} when is_integer(min) and is_integer(max) ->
          [
            "WHEN ",
            expression,
            " >= ",
            Integer.to_string(min),
            " AND ",
            expression,
            " <= ",
            Integer.to_string(max),
            " THEN '",
            escape_literal(label),
            "'"
          ]

        {min, :infinity, label} when is_integer(min) ->
          [
            "WHEN ",
            expression,
            " >= ",
            Integer.to_string(min),
            " THEN '",
            escape_literal(label),
            "'"
          ]

        {:negative_infinity, max, label} when is_integer(max) ->
          [
            "WHEN ",
            expression,
            " <= ",
            Integer.to_string(max),
            " THEN '",
            escape_literal(label),
            "'"
          ]
      end)

    ["CASE ", Enum.intersperse(clauses, " "), " ELSE 'Other' END"]
  end

  defp date_range_case_sql(expression, ranges) do
    clauses =
      Enum.map(ranges, fn
        {min, max, label} when is_integer(min) and is_integer(max) and min == max ->
          [
            "WHEN ",
            expression,
            " = CURRENT_DATE - INTERVAL '",
            Integer.to_string(min),
            " day' THEN '",
            escape_literal(label),
            "'"
          ]

        {min, max, label} when is_integer(min) and is_integer(max) ->
          [
            "WHEN ",
            expression,
            " BETWEEN CURRENT_DATE - INTERVAL '",
            Integer.to_string(max),
            " day' AND CURRENT_DATE - INTERVAL '",
            Integer.to_string(min),
            " day' THEN '",
            escape_literal(label),
            "'"
          ]

        {min, :infinity, label} when is_integer(min) ->
          [
            "WHEN ",
            expression,
            " <= CURRENT_DATE - INTERVAL '",
            Integer.to_string(min),
            " day' THEN '",
            escape_literal(label),
            "'"
          ]

        {"today", "today", _label} ->
          ["WHEN ", expression, " = CURRENT_DATE THEN 'Today'"]

        {"yesterday", "yesterday", _label} ->
          ["WHEN ", expression, " = CURRENT_DATE - INTERVAL '1 day' THEN 'Yesterday'"]

        {"tomorrow", "tomorrow", _label} ->
          ["WHEN ", expression, " = CURRENT_DATE + INTERVAL '1 day' THEN 'Tomorrow'"]
      end)

    ["CASE ", Enum.intersperse(clauses, " "), " ELSE 'Other' END"]
  end

  defp text_normalization_sql(%TextNormalization{} = normalization) do
    trimmed = ["BTRIM(COALESCE(CAST(", normalization.expression, " AS TEXT), ''))"]

    article_normalized =
      case normalization.exclude_articles do
        [] ->
          trimmed

        articles ->
          pattern =
            articles
            |> Enum.map(&Regex.escape/1)
            |> Enum.join("|")

          [
            "REGEXP_REPLACE(",
            trimmed,
            ", '^(",
            escape_literal(pattern),
            ")([[:space:]]+|$)', '', 'i')"
          ]
      end

    if normalization.ignore_case,
      do: ["LOWER(", article_normalized, ")"],
      else: article_normalized
  end

  defp bucket_datetime_expression(%BucketExpression{} = bucket) do
    datetime_expression(%DateTimeOperation{
      operation: :extract_part,
      clause: :select,
      expression: bucket.expression,
      options: bucket.temporal_options
    })
  end

  defp validate_bucket(%BucketExpression{kind: kind, increment: increment})
       when kind in [:numeric_increment, :year_increment] and is_integer(increment) and
              increment > 0,
       do: :ok

  defp validate_bucket(%BucketExpression{kind: :text_prefix, prefix_length: prefix_length})
       when is_integer(prefix_length) and prefix_length > 0,
       do: :ok

  defp validate_bucket(%BucketExpression{kind: kind, ranges: ranges})
       when kind in [:numeric_ranges, :date_relative_ranges, :elapsed_days_ranges, :year_ranges] and
              is_list(ranges) and ranges != [],
       do: :ok

  defp validate_bucket(%BucketExpression{} = bucket) do
    {:error,
     Selecto.Error.validation_error("Invalid PostgreSQL bucket expression", %{
       kind: bucket.kind,
       unsupported_feature: :bucket_expression
     })}
  end

  @impl true
  def render_json_extraction(%Extraction{} = fragment, _selecto) do
    column_ref = qualified_column(fragment.column, fragment.table_alias)

    extraction =
      case fragment.path do
        [key] ->
          operator = if fragment.as_text, do: "->>", else: "->"
          [column_ref, operator, "'", escape_literal(key), "'"]

        keys ->
          operator = if fragment.as_text, do: "#>>", else: "#>"
          path = keys |> Enum.map(&["'", escape_literal(&1), "'"]) |> Enum.intersperse(", ")
          [column_ref, operator, "ARRAY[", path, "]"]
      end

    {:ok, cast_json(extraction, fragment.cast)}
  end

  @impl true
  def render_json_contains(%Contains{} = fragment, _selecto) do
    encoded = fragment.value |> Jason.encode!() |> escape_literal()
    {:ok, [qualified_column(fragment.column, fragment.table_alias), " @> '", encoded, "'::jsonb"]}
  end

  @impl true
  def render_json_key_exists(%KeyExists{} = fragment, selecto) do
    column_ref = qualified_column(fragment.column, fragment.table_alias)

    case fragment.path do
      [key] ->
        {:ok, [column_ref, " ? '", escape_literal(key), "'"]}

      keys ->
        {parent, [key]} = Enum.split(keys, -1)

        with {:ok, extraction} <-
               render_json_extraction(
                 %Extraction{
                   column: fragment.column,
                   path: parent,
                   as_text: false,
                   table_alias: fragment.table_alias
                 },
                 selecto
               ) do
          {:ok, [extraction, " ? '", escape_literal(key), "'"]}
        end
    end
  end

  @impl true
  def render_json_array_contains(%ArrayContains{} = fragment, selecto) do
    with {:ok, array} <- json_array_expression(fragment, selecto) do
      case fragment.value do
        value when is_binary(value) ->
          {:ok, [array, " ? '", escape_literal(value), "'"]}

        values when is_list(values) ->
          encoded = values |> Enum.map(&["'", escape_literal(&1), "'"]) |> Enum.intersperse(",")
          {:ok, [array, " ?| array[", encoded, "]"]}

        value ->
          {:error,
           Selecto.Error.validation_error(
             "PostgreSQL JSON array membership requires a string or list",
             %{value: value}
           )}
      end
    end
  end

  @impl true
  def render_json_array_contains_all(%ArrayContainsAll{} = fragment, selecto) do
    with {:ok, array} <- json_array_expression(fragment, selecto) do
      encoded =
        fragment.values |> Enum.map(&["'", escape_literal(&1), "'"]) |> Enum.intersperse(",")

      {:ok, [array, " ?& array[", encoded, "]"]}
    end
  end

  @impl true
  def render_json_operation(%Operation{} = operation, selecto) do
    case operation.operation do
      kind when kind in [:json_extract, :json_extract_path] ->
        operation_extraction(operation, false, selecto)

      kind when kind in [:json_extract_text, :json_extract_path_text] ->
        operation_extraction(operation, true, selecto)

      :json_contains ->
        render_json_contains(operation_contains(operation), selecto)

      :json_contained ->
        encoded = operation.value |> Jason.encode!() |> escape_literal()
        {:ok, [operation_column(operation), " <@ '", encoded, "'::jsonb"]}

      kind when kind in [:json_exists, :json_path_exists] ->
        render_json_key_exists(operation_key_exists(operation), selecto)

      :json_agg ->
        {:ok, ["json_agg(", operation_column(operation), ")"]}

      :json_object_agg ->
        {:ok,
         [
           "json_object_agg(",
           operation_field(operation, :key_sql, operation.key_field),
           ", ",
           operation_field(operation, :value_sql, operation.value_field),
           ")"
         ]}

      :json_build_object ->
        {:ok, ["json_build_object(", operation_pairs(operation), ")"]}

      :json_build_array ->
        {:ok, ["json_build_array(", json_values(operation.value), ")"]}

      :json_empty_array ->
        {:ok, "'[]'::json"}

      :json_set ->
        {:ok,
         [
           "JSONB_SET(",
           operation_column(operation),
           ", ",
           postgres_path(operation.path),
           ", ",
           json_value(operation.value),
           ")"
         ]}

      :json_insert ->
        {:ok,
         [
           "JSONB_INSERT(",
           operation_column(operation),
           ", ",
           postgres_path(operation.path),
           ", ",
           json_value(operation.value),
           ")"
         ]}

      :json_remove ->
        {:ok, [operation_column(operation), " #- ", postgres_path(operation.path)]}

      :json_typeof ->
        {:ok, ["JSONB_TYPEOF(", operation_column(operation), ")"]}

      :json_array_length ->
        {:ok, ["JSONB_ARRAY_LENGTH(", operation_column(operation), ")"]}
    end
  end

  @impl true
  def render_collection_operation(%CollectionOperation{} = operation, _selecto) do
    case operation.operation do
      kind when kind in [:array_agg, :array_agg_distinct] ->
        distinct = if operation.distinct, do: "DISTINCT ", else: ""
        {:ok, ["ARRAY_AGG(", distinct, operation.column, order_by(operation.order_by), ")"]}

      :string_agg ->
        delimiter = Map.get(operation.options, :delimiter, ",")
        distinct = if operation.distinct, do: "DISTINCT ", else: ""

        {:ok,
         {[
            "STRING_AGG(",
            distinct,
            operation.column,
            ", ",
            {:param, delimiter},
            order_by(operation.order_by),
            ")"
          ], [delimiter]}}

      kind when kind in [:array_contains, :array_contained, :array_overlap, :array_eq] ->
        operator = collection_operator(kind)

        {:ok,
         {[operation.column, " ", operator, " ", {:param, operation.value}], [operation.value]}}

      :array_length ->
        {:ok,
         ["ARRAY_LENGTH(", operation.column, ", ", Integer.to_string(operation.dimension), ")"]}

      :cardinality ->
        {:ok, ["CARDINALITY(", operation.column, ")"]}

      kind when kind in [:array_ndims, :array_dims] ->
        {:ok, [kind |> Atom.to_string() |> String.upcase(), "(", operation.column, ")"]}

      :array ->
        elements = operation.value || []

        {:ok,
         {["ARRAY[", Enum.intersperse(Enum.map(elements, &{:param, &1}), ", "), "]"], elements}}

      :array_constructor ->
        {:ok, ["ARRAY[", operation.value || [], "]"]}

      :array_fill ->
        dimensions = Map.get(operation.options, :dimensions)

        {:ok,
         {["ARRAY_FILL(", {:param, operation.value}, ", ", {:param, dimensions}, ")"],
          [operation.value, dimensions]}}

      :array_append ->
        one_param_function("ARRAY_APPEND", operation.column, operation.value)

      :array_prepend ->
        {:ok,
         {["ARRAY_PREPEND(", {:param, operation.value}, ", ", operation.column, ")"],
          [operation.value]}}

      :array_cat ->
        case Map.fetch(operation.options, :value_expression) do
          {:ok, value_expression} ->
            {:ok, ["ARRAY_CAT(", operation.column, ", ", value_expression, ")"]}

          :error ->
            one_param_function("ARRAY_CAT", operation.column, operation.value)
        end

      :array_position ->
        case Map.fetch(operation.options, :start) do
          {:ok, start} ->
            {:ok,
             {[
                "ARRAY_POSITION(",
                operation.column,
                ", ",
                {:param, operation.value},
                ", ",
                {:param, start},
                ")"
              ], [operation.value, start]}}

          :error ->
            one_param_function("ARRAY_POSITION", operation.column, operation.value)
        end

      :array_positions ->
        one_param_function("ARRAY_POSITIONS", operation.column, operation.value)

      :array_remove ->
        one_param_function("ARRAY_REMOVE", operation.column, operation.value)

      :array_replace ->
        new_value = Map.get(operation.options, :new_value)

        {:ok,
         {[
            "ARRAY_REPLACE(",
            operation.column,
            ", ",
            {:param, operation.value},
            ", ",
            {:param, new_value},
            ")"
          ], [operation.value, new_value]}}

      :unnest ->
        suffix = if Map.get(operation.options, :with_ordinality), do: " WITH ORDINALITY", else: ""
        {:ok, ["UNNEST(", operation.column, ")", suffix]}

      :array_to_string ->
        collection_transform("ARRAY_TO_STRING", operation)

      :string_to_array ->
        collection_transform("STRING_TO_ARRAY", operation)

      kind when kind in [:array_union, :array_intersect, :array_except] ->
        function = kind |> Atom.to_string() |> String.upcase()
        one_param_function(function, operation.column, operation.value)

      operation_name ->
        {:error,
         Selecto.Error.validation_error(
           "PostgreSQL does not support this collection operation",
           %{
             operation: operation_name,
             unsupported_feature: :collection_operation
           }
         )}
    end
  end

  @impl true
  def render_hierarchy_adjacency(%Adjacency{} = hierarchy, _selecto) do
    {:ok,
     [
       hierarchy.cte_name,
       " AS (",
       "SELECT #{hierarchy.id_field}, #{hierarchy.name_field}, #{hierarchy.parent_field}, 0 as level, ",
       "CAST(#{hierarchy.id_field} AS TEXT) as path, ARRAY[#{hierarchy.id_field}] as path_array ",
       "FROM #{hierarchy.source_table} WHERE #{hierarchy.parent_field} IS NULL",
       " UNION ALL ",
       "SELECT c.#{hierarchy.id_field}, c.#{hierarchy.name_field}, c.#{hierarchy.parent_field}, h.level + 1, ",
       "h.path || '/' || CAST(c.#{hierarchy.id_field} AS TEXT), h.path_array || c.#{hierarchy.id_field} ",
       "FROM #{hierarchy.source_table} c JOIN #{hierarchy.cte_name} h ON c.#{hierarchy.parent_field} = h.#{hierarchy.id_field} ",
       "WHERE h.level < ",
       {:param, hierarchy.depth_limit},
       ")"
     ]}
  end

  @impl true
  def render_hierarchy_materialized_path(%MaterializedPath{} = hierarchy, _selecto) do
    separator = escape_literal(hierarchy.path_separator)

    {:ok,
     [
       hierarchy.query_name,
       " AS (",
       "SELECT *, ",
       "(length(#{hierarchy.path_field}) - length(replace(#{hierarchy.path_field}, '#{separator}', ''))) as depth, ",
       "string_to_array(#{hierarchy.path_field}, '#{separator}') as path_array ",
       "FROM #{hierarchy.source_table} ",
       "WHERE #{hierarchy.path_field} LIKE ",
       {:param, hierarchy.path_pattern},
       ")"
     ]}
  end

  @impl true
  def render_table_function_join(%TableFunctionJoin{} = join, _selecto) do
    alias_sql = portable_alias(join.alias)

    source =
      case join.ordinality_alias do
        nil ->
          [join.source_sql, " AS ", alias_sql]

        ordinality_alias ->
          [
            join.source_sql,
            " WITH ORDINALITY AS ",
            alias_sql,
            "(",
            portable_alias("value"),
            ", ",
            portable_alias(ordinality_alias),
            ")"
          ]
      end

    case join.join_type do
      :cross -> {:ok, ["CROSS JOIN LATERAL ", source]}
      :inner -> {:ok, ["INNER JOIN LATERAL ", source, " ON true"]}
      :left -> {:ok, ["LEFT JOIN LATERAL ", source, " ON true"]}
      :right -> {:ok, ["RIGHT JOIN LATERAL ", source, " ON true"]}
      :full -> {:ok, ["FULL JOIN LATERAL ", source, " ON true"]}
    end
  end

  @impl true
  def render_window_frame_boundary(%FrameBoundary{} = boundary, _selecto) do
    {:ok,
     [
       "INTERVAL '",
       boundary.amount,
       " ",
       Atom.to_string(boundary.unit),
       "' ",
       boundary.direction |> Atom.to_string() |> String.upcase()
     ]}
  end

  defp portable_alias(identifier) do
    identifier = to_string(identifier)

    if Regex.match?(~r/\A[A-Za-z_][A-Za-z0-9_]*\z/, identifier),
      do: identifier,
      else: SelectoDBPostgreSQL.Adapter.quote_identifier(identifier)
  end

  @impl true
  def render_view_definition(%Definition{} = definition, _context) do
    create =
      case definition.kind do
        :view -> "CREATE VIEW"
        :materialized_view -> "CREATE MATERIALIZED VIEW"
      end

    {:ok,
     [
       create,
       " ",
       Selecto.SQL.QualifiedIdentifier.quote!(
         definition.database_name,
         SelectoDBPostgreSQL.Adapter
       ),
       " AS\n",
       definition.query_sql,
       ";"
     ]}
  end

  @impl true
  def render_view_refresh(%Refresh{} = refresh, _context) do
    concurrently = if refresh.concurrently, do: " CONCURRENTLY", else: ""

    {:ok,
     [
       "REFRESH MATERIALIZED VIEW",
       concurrently,
       " ",
       Selecto.SQL.QualifiedIdentifier.quote!(refresh.database_name, SelectoDBPostgreSQL.Adapter),
       ";"
     ]}
  end

  @impl true
  def render_view_index(%Index{} = index, _context) do
    create = if index.unique, do: "CREATE UNIQUE INDEX", else: "CREATE INDEX"
    concurrently = if index.concurrently, do: " CONCURRENTLY", else: ""

    columns =
      index.columns
      |> Enum.map(&SelectoDBPostgreSQL.Adapter.quote_identifier/1)
      |> Enum.intersperse(", ")

    {:ok,
     [
       create,
       concurrently,
       " ",
       SelectoDBPostgreSQL.Adapter.quote_identifier(index.index_name),
       " ON ",
       Selecto.SQL.QualifiedIdentifier.quote!(index.database_name, SelectoDBPostgreSQL.Adapter),
       " (",
       columns,
       ");"
     ]}
  end

  defp collection_operator(:array_contains), do: "@>"
  defp collection_operator(:array_contained), do: "<@"
  defp collection_operator(:array_overlap), do: "&&"
  defp collection_operator(:array_eq), do: "="

  defp order_by([]), do: []

  defp order_by(order_by) do
    parts =
      order_by
      |> Enum.map(fn {column, direction} -> [column, " ", direction_sql(direction)] end)
      |> Enum.intersperse(", ")

    [" ORDER BY ", parts]
  end

  defp direction_sql(:asc), do: "ASC"
  defp direction_sql(:desc), do: "DESC"

  defp one_param_function(function, column, value),
    do: {:ok, {[function, "(", column, ", ", {:param, value}, ")"], [value]}}

  defp collection_transform(function, operation) do
    delimiter = operation.value || ","

    case Map.get(operation.options, :null_string) do
      nil ->
        {:ok, {[function, "(", operation.column, ", ", {:param, delimiter}, ")"], [delimiter]}}

      null_string ->
        {:ok,
         {[
            function,
            "(",
            operation.column,
            ", ",
            {:param, delimiter},
            ", ",
            {:param, null_string},
            ")"
          ], [delimiter, null_string]}}
    end
  end

  defp operation_extraction(operation, as_text, selecto) do
    render_json_extraction(
      %Extraction{
        column: operation.column,
        path: parse_operation_path(operation.path),
        as_text: as_text,
        table_alias: operation.table_alias
      },
      selecto
    )
  end

  defp operation_contains(operation) do
    %Contains{
      column: operation.column,
      value: operation.value,
      table_alias: operation.table_alias
    }
  end

  defp operation_key_exists(operation) do
    %KeyExists{
      column: operation.column,
      path: parse_operation_path(operation.path),
      table_alias: operation.table_alias
    }
  end

  defp operation_column(%{options: options} = operation) do
    Map.get(options || %{}, :column_sql) ||
      qualified_column(operation.column, operation.table_alias)
  end

  defp operation_field(operation, key, field) do
    Map.get(operation.options || %{}, key) || quote_field(field)
  end

  defp operation_pairs(operation),
    do: Map.get(operation.options || %{}, :pairs_sql) || object_pairs(operation.value)

  defp quote_field(field), do: SelectoDBPostgreSQL.Adapter.quote_identifier(field)

  defp object_pairs(pairs) when is_list(pairs) do
    pairs
    |> Enum.map(fn {key, value} -> [json_value(to_string(key)), ", ", json_value(value)] end)
    |> Enum.intersperse(", ")
  end

  defp json_values(values) when is_list(values),
    do: values |> Enum.map(&json_value/1) |> Enum.intersperse(", ")

  defp json_value(value) when is_binary(value), do: ["'", escape_literal(value), "'"]
  defp json_value(value) when is_integer(value), do: Integer.to_string(value)
  defp json_value(value) when is_float(value), do: Float.to_string(value)
  defp json_value(true), do: "true"
  defp json_value(false), do: "false"
  defp json_value(nil), do: "null"

  defp json_value(value) when is_map(value) or is_list(value),
    do: ["'", value |> Jason.encode!() |> escape_literal(), "'::jsonb"]

  defp json_value(value), do: ["'", value |> inspect() |> escape_literal(), "'"]

  defp postgres_path(path) do
    path = parse_operation_path(path)
    ["ARRAY[", path |> Enum.map(&["'", escape_literal(&1), "'"]) |> Enum.intersperse(", "), "]"]
  end

  defp parse_operation_path(nil), do: []

  defp parse_operation_path(path) do
    path
    |> String.replace_prefix("$.", "")
    |> String.split(~r/[\.\[\]]/, trim: true)
  end

  defp json_array_expression(fragment, selecto) do
    render_json_extraction(
      %Extraction{
        column: fragment.column,
        path: fragment.path,
        as_text: false,
        table_alias: fragment.table_alias
      },
      selecto
    )
  end

  defp qualified_column(column, nil), do: SelectoDBPostgreSQL.Adapter.quote_identifier(column)

  defp qualified_column(column, table_alias) do
    [
      SelectoDBPostgreSQL.Adapter.quote_identifier(table_alias),
      ".",
      SelectoDBPostgreSQL.Adapter.quote_identifier(column)
    ]
  end

  defp cast_json(extraction, nil), do: extraction
  defp cast_json(extraction, :integer), do: ["(", extraction, ")::integer"]
  defp cast_json(extraction, :decimal), do: ["(", extraction, ")::numeric"]
  defp cast_json(extraction, :float), do: ["(", extraction, ")::double precision"]
  defp cast_json(extraction, :boolean), do: ["(", extraction, ")::boolean"]
  defp cast_json(extraction, :date), do: ["(", extraction, ")::date"]
  defp cast_json(extraction, :datetime), do: ["(", extraction, ")::timestamp"]
  defp cast_json(extraction, :utc_datetime), do: ["(", extraction, ")::timestamptz"]

  defp cast_json(_extraction, cast) do
    raise ArgumentError, "unsupported PostgreSQL JSON cast: #{inspect(cast)}"
  end

  defp escape_literal(value), do: value |> to_string() |> String.replace("'", "''")

  @impl true
  def render_text_search_predicate(%Predicate{} = predicate, _selecto) do
    with :ok <- require_single_selector(predicate.selectors),
         {:ok, query_function} <- query_function(predicate.mode) do
      [selector] = predicate.selectors

      {:ok,
       [
         " ",
         selector,
         " @@ ",
         query_function,
         "(",
         {:param, predicate.query},
         ") "
       ]}
    end
  end

  @impl true
  def render_text_search_rank(%Rank{} = rank, selecto) do
    with :ok <- require_rank_query(rank.query),
         :ok <- reject_rank_weights(rank.weights),
         {:ok, field} <- require_single_field(rank.fields),
         {:ok, conf} <- text_search_field(selecto, field),
         {:ok, query_function} <- query_function(rank.mode) do
      field_ref = Map.get(conf, :field, field)

      {:ok,
       {:field,
        {:func, :ts_rank,
         [to_string(field_ref), {:func, query_function, [{:literal, rank.query}]}]}, rank.alias}}
    end
  end

  defp query_function(mode) when mode in [nil, :websearch], do: {:ok, :websearch_to_tsquery}
  defp query_function(mode) when mode in [:plain, :natural], do: {:ok, :plainto_tsquery}
  defp query_function(:phrase), do: {:ok, :phraseto_tsquery}
  defp query_function(:boolean), do: {:ok, :to_tsquery}
  defp query_function(mode), do: error("Unsupported PostgreSQL text-search mode", mode: mode)

  defp require_single_selector([_selector]), do: :ok

  defp require_single_selector(selectors) do
    error("PostgreSQL text search requires exactly one search-document selector",
      selector_count: length(selectors)
    )
  end

  defp require_single_field([field]), do: {:ok, field}

  defp require_single_field(fields) do
    error("PostgreSQL text_search_rank/3 requires exactly one search-document field",
      field_count: length(fields)
    )
  end

  defp require_rank_query(nil),
    do: error("PostgreSQL text_search_rank/3 requires a :query option")

  defp require_rank_query(_query), do: :ok

  defp reject_rank_weights([]), do: :ok

  defp reject_rank_weights(_weights),
    do: error("PostgreSQL text_search_rank/3 does not support :weights yet")

  defp text_search_field(selecto, field) do
    case root_column_conf(selecto, field) do
      nil ->
        error("PostgreSQL text_search_rank/3 field not found", field: field)

      conf ->
        native_type = Map.get(conf, :type)

        if Selecto.AdapterSupport.type_family(SelectoDBPostgreSQL.Adapter, native_type) ==
             :text_search or Map.get(conf, :text_search_backend) == :postgresql do
          {:ok, conf}
        else
          error("PostgreSQL text_search_rank/3 field is not a search document", field: field)
        end
    end
  end

  defp root_column_conf(selecto, field) do
    columns = selecto |> Map.get(:config, %{}) |> Map.get(:columns, %{})
    Map.get(columns, field) || Map.get(columns, safe_existing_atom(field))
  end

  defp safe_existing_atom(field) when is_binary(field) do
    try do
      String.to_existing_atom(field)
    rescue
      ArgumentError -> nil
    end
  end

  defp safe_existing_atom(_field), do: nil

  defp error(message, details \\ []) do
    {:error,
     Selecto.Error.validation_error(
       message,
       Map.new([unsupported_feature: :text_search] ++ details)
     )}
  end
end
