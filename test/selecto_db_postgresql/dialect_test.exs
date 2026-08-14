defmodule SelectoDBPostgreSQL.DialectTest do
  use ExUnit.Case, async: true

  alias Selecto.Dialect.Collection.Operation, as: CollectionOperation
  alias Selecto.Dialect.Bucket.Expression, as: BucketExpression
  alias Selecto.Dialect.DateTime.Operation, as: DateTimeOperation
  alias Selecto.Dialect.Hierarchy.{Adjacency, MaterializedPath}
  alias Selecto.Dialect.Json.Operation, as: JsonOperation
  alias Selecto.Dialect.Predicate.Comparison
  alias Selecto.Dialect.Text.Normalization, as: TextNormalization
  alias Selecto.Dialect.View.{Definition, Index, Refresh}
  alias SelectoDBPostgreSQL.Dialect

  defp sql(iodata), do: IO.iodata_to_binary(iodata)

  test "collection aggregates are rendered entirely by the PostgreSQL dialect" do
    fragment = %CollectionOperation{
      operation: :array_agg,
      clause: :select,
      column: ~s("selecto_root"."name"),
      distinct: true,
      order_by: [{~s("selecto_root"."created_at"), :desc}],
      options: %{}
    }

    assert {:ok, rendered} = Dialect.render_collection_operation(fragment, %{})

    assert sql(rendered) ==
             ~s|ARRAY_AGG(DISTINCT "selecto_root"."name" ORDER BY "selecto_root"."created_at" DESC)|
  end

  test "collection expressions and adapter-owned parameters retain their shape" do
    cat = %CollectionOperation{
      operation: :array_cat,
      clause: :select,
      column: ~s("left_tags"),
      order_by: [],
      options: %{value_expression: ~s("right_tags")}
    }

    assert {:ok, rendered_cat} = Dialect.render_collection_operation(cat, %{})
    assert sql(rendered_cat) == ~s|ARRAY_CAT("left_tags", "right_tags")|

    position = %CollectionOperation{
      operation: :array_position,
      clause: :select,
      column: ~s("tags"),
      value: "needle",
      order_by: [],
      options: %{start: 2}
    }

    assert {:ok, {rendered_position, ["needle", 2]}} =
             Dialect.render_collection_operation(position, %{})

    assert rendered_position == [
             "ARRAY_POSITION(",
             ~s("tags"),
             ", ",
             {:param, "needle"},
             ", ",
             {:param, 2},
             ")"
           ]
  end

  test "array constructors are rendered by the PostgreSQL collection dialect" do
    fragment = %CollectionOperation{
      operation: :array_constructor,
      clause: :select,
      value: [{:param, "one"}, ", ", {:param, "two"}],
      order_by: [],
      options: %{}
    }

    assert {:ok, rendered} = Dialect.render_collection_operation(fragment, %{})
    assert rendered == ["ARRAY[", fragment.value, "]"]
  end

  test "PostgreSQL hierarchy path SQL is adapter-owned" do
    adjacency = %Adjacency{
      cte_name: "category_hierarchy",
      source_table: "categories",
      id_field: "id",
      name_field: "name",
      parent_field: "parent_id",
      depth_limit: 5
    }

    materialized = %MaterializedPath{
      query_name: "category_path",
      source_table: "categories",
      path_field: "path",
      path_separator: "/",
      path_pattern: "root/%"
    }

    assert {:ok, adjacency_sql} = Dialect.render_hierarchy_adjacency(adjacency, %{})

    assert {finalized_adjacency, [5]} =
             Selecto.SQL.Params.finalize(adjacency_sql, adapter: SelectoDBPostgreSQL.Adapter)

    assert finalized_adjacency =~ "ARRAY[id] as path_array"

    assert {:ok, materialized_sql} =
             Dialect.render_hierarchy_materialized_path(materialized, %{})

    assert {finalized_materialized, ["root/%"]} =
             Selecto.SQL.Params.finalize(materialized_sql,
               adapter: SelectoDBPostgreSQL.Adapter
             )

    assert finalized_materialized =~ "string_to_array(path, '/') as path_array"
  end

  test "portable JSON aggregate nodes accept core-prepared expressions" do
    aggregate = %JsonOperation{
      operation: :json_agg,
      clause: :select,
      options: %{column_sql: ~s("selecto_root"."payload")}
    }

    object_aggregate = %JsonOperation{
      operation: :json_object_agg,
      clause: :select,
      options: %{
        key_sql: ~s("selecto_root"."key"),
        value_sql: ~s("selecto_root"."value")
      }
    }

    assert {:ok, rendered_aggregate} = Dialect.render_json_operation(aggregate, %{})
    assert sql(rendered_aggregate) == ~s|json_agg("selecto_root"."payload")|

    assert {:ok, rendered_object} = Dialect.render_json_operation(object_aggregate, %{})

    assert sql(rendered_object) ==
             ~s|json_object_agg("selecto_root"."key", "selecto_root"."value")|
  end

  test "PostgreSQL date functions are adapter-owned typed operations" do
    truncate = %DateTimeOperation{
      operation: :truncate,
      clause: :select,
      expression: ~s("created_at"),
      part: :month
    }

    part = %DateTimeOperation{
      operation: :extract_part,
      clause: :select,
      expression: ~s("created_at"),
      part: :epoch
    }

    age = %DateTimeOperation{
      operation: :age,
      clause: :select,
      expression: ~s("finished_at"),
      second_expression: ~s("started_at")
    }

    assert {:ok, rendered_truncate} = Dialect.render_datetime_operation(truncate, %{})
    assert sql(rendered_truncate) == ~s|DATE_TRUNC('month', "created_at")|

    assert {:ok, rendered_part} = Dialect.render_datetime_operation(part, %{})
    assert sql(rendered_part) == ~s|DATE_PART('epoch', "created_at")|

    assert {:ok, rendered_age} = Dialect.render_datetime_operation(age, %{})
    assert sql(rendered_age) == ~s|AGE("finished_at", "started_at")|
  end

  test "PostgreSQL owns formatted, timezone-aware, and epoch-backed datetime SQL" do
    fragment = %DateTimeOperation{
      operation: :format,
      clause: :select,
      expression: ~s("occurred_at_epoch"),
      options: %{
        format: "YYYY-MM",
        epoch_storage: :unix_seconds,
        timezone: "America/Denver"
      }
    }

    assert {:ok, rendered} = Dialect.render_datetime_operation(fragment, %{})

    assert sql(rendered) ==
             ~s|TO_CHAR(TO_TIMESTAMP("occurred_at_epoch") AT TIME ZONE 'America/Denver', 'YYYY-MM')|
  end

  test "PostgreSQL owns text normalization and case-insensitive comparison SQL" do
    normalization = %TextNormalization{
      expression: ~s("title"),
      exclude_articles: ["a", "an", "the"],
      ignore_case: true
    }

    comparison = %Comparison{
      operation: :case_insensitive_like,
      left: ~s("title"),
      right: {:param, "%office%"}
    }

    assert {:ok, normalized} = Dialect.render_text_normalization(normalization, %{})
    assert sql(normalized) =~ "REGEXP_REPLACE"
    assert sql(normalized) =~ "LOWER("

    assert {:ok, rendered_comparison} = Dialect.render_comparison(comparison, %{})
    assert rendered_comparison == [~s("title"), " ", "ILIKE", " ", {:param, "%office%"}]
  end

  test "PostgreSQL owns year and prefix bucket SQL" do
    year_bucket = %BucketExpression{
      kind: :year_increment,
      expression: ~s("created_at"),
      increment: 5,
      temporal_options: %{}
    }

    prefix_bucket = %BucketExpression{
      kind: :text_prefix,
      expression: ~s("title"),
      prefix_length: 2,
      exclude_articles: ["a", "an", "the"],
      ignore_case: true
    }

    assert {:ok, rendered_year} = Dialect.render_bucket(year_bucket, %{})
    assert sql(rendered_year) =~ "DATE_PART('year', \"created_at\")"
    assert sql(rendered_year) =~ "FLOOR"

    assert {:ok, rendered_prefix} = Dialect.render_bucket(prefix_bucket, %{})
    assert sql(rendered_prefix) =~ "REGEXP_REPLACE"
    assert sql(rendered_prefix) =~ "UPPER(LEFT("
  end

  test "view publication SQL is owned and safely quoted by the PostgreSQL dialect" do
    definition = %Definition{
      kind: :materialized_view,
      database_name: "reporting.daily_totals",
      query_sql: "SELECT 1"
    }

    refresh = %Refresh{database_name: "reporting.daily_totals", concurrently: true}

    index = %Index{
      database_name: "reporting.daily_totals",
      index_name: "daily totals unique",
      columns: ["account id", "day"],
      unique: true,
      concurrently: true
    }

    assert {:ok, rendered_definition} = Dialect.render_view_definition(definition, %{})

    assert sql(rendered_definition) ==
             ~s(CREATE MATERIALIZED VIEW "reporting"."daily_totals" AS\nSELECT 1;)

    assert {:ok, rendered_refresh} = Dialect.render_view_refresh(refresh, %{})

    assert sql(rendered_refresh) ==
             ~s(REFRESH MATERIALIZED VIEW CONCURRENTLY "reporting"."daily_totals";)

    assert {:ok, rendered_index} = Dialect.render_view_index(index, %{})

    assert sql(rendered_index) ==
             ~s|CREATE UNIQUE INDEX CONCURRENTLY "daily totals unique" ON "reporting"."daily_totals" ("account id", "day");|
  end

  test "unknown collection operations fail with structured capability evidence" do
    fragment = %CollectionOperation{
      operation: :not_a_postgresql_collection_operation,
      clause: :select,
      order_by: [],
      options: %{}
    }

    assert {:error,
            %Selecto.Error{
              type: :validation_error,
              details: %{
                operation: :not_a_postgresql_collection_operation,
                unsupported_feature: :collection_operation
              }
            }} = Dialect.render_collection_operation(fragment, %{})
  end
end
