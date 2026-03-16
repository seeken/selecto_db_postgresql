defmodule SelectoDBPostgreSQL.SelectoComponentsSQLTest do
  use ExUnit.Case, async: true

  alias SelectoComponents.Helpers.BucketParser
  alias SelectoComponents.Helpers.Filters
  alias SelectoComponents.Views.Aggregate.Process, as: AggregateProcess
  alias SelectoComponents.Views.Graph.Process, as: GraphProcess

  test "text prefix filters compile PostgreSQL raw SQL" do
    filters = %{
      "filters" => [
        %{
          "uuid" => "f1",
          "section" => "filters",
          "filter" => "title",
          "comp" => "TEXT_PREFIX",
          "value" => "OF",
          "prefix_length" => "2",
          "exclude_articles" => "true"
        }
      ]
    }

    [filter] = Filters.filter_recurse(selecto(), filters, "filters")

    assert {{:raw_sql, sql_expr}, {:like, "of%"}} = filter
    assert sql_expr =~ "REGEXP_REPLACE"
    assert sql_expr =~ "selecto_root.title"
  end

  test "starts-with article stripping compiles PostgreSQL raw SQL" do
    filters = %{
      "filters" => [
        %{
          "uuid" => "f1",
          "section" => "filters",
          "filter" => "title",
          "comp" => "STARTS",
          "value" => "of",
          "exclude_articles" => "true"
        }
      ]
    }

    [filter] = Filters.filter_recurse(selecto(), filters, "filters")

    assert {{:raw_sql, sql_expr}, {:like, "of%"}} = filter
    assert sql_expr =~ "REGEXP_REPLACE"
    assert sql_expr =~ "selecto_root.title"
  end

  test "case-insensitive article stripping compiles PostgreSQL raw SQL" do
    filters = %{
      "filters" => [
        %{
          "uuid" => "f1",
          "section" => "filters",
          "filter" => "title",
          "comp" => "=",
          "value" => "Office",
          "exclude_articles" => "true",
          "ignore_case" => "true"
        }
      ]
    }

    [filter] = Filters.filter_recurse(selecto(), filters, "filters")

    assert {{:raw_sql, sql_expr}, "office"} = filter
    assert sql_expr =~ "LOWER("
    assert sql_expr =~ "REGEXP_REPLACE"
  end

  test "weekday and shortcut filters compile PostgreSQL date SQL" do
    weekday_filters = %{
      "filters" => [
        %{
          "uuid" => "f1",
          "section" => "filters",
          "filter" => "created_at",
          "comp" => "WEEKDAY",
          "value" => "1"
        }
      ]
    }

    [weekday_filter] = Filters.filter_recurse(datetime_selecto(), weekday_filters, "filters")
    assert {:raw_sql_filter, weekday_sql} = weekday_filter
    assert IO.iodata_to_binary(weekday_sql) =~ "EXTRACT(ISODOW FROM selecto_root.created_at)"

    shortcut_filters = %{
      "filters" => [
        %{
          "uuid" => "f2",
          "section" => "filters",
          "filter" => "created_at",
          "comp" => "SHORTCUT",
          "value" => "weekdays"
        }
      ]
    }

    [shortcut_filter] = Filters.filter_recurse(datetime_selecto(), shortcut_filters, "filters")
    assert {:raw_sql_filter, shortcut_sql} = shortcut_filter
    rendered_shortcut_sql = IO.iodata_to_binary(shortcut_sql)
    assert rendered_shortcut_sql =~ "EXTRACT(ISODOW FROM selecto_root.created_at)"
    assert rendered_shortcut_sql =~ "IN (1,2,3,4,5)"
  end

  test "weekday_sun1 and week_of_year filters compile PostgreSQL date SQL" do
    sun1_filters = %{
      "filters" => [
        %{
          "uuid" => "f1",
          "section" => "filters",
          "filter" => "created_at",
          "comp" => "WEEKDAY_SUN1",
          "value" => "1"
        }
      ]
    }

    [sun1_filter] = Filters.filter_recurse(datetime_selecto(), sun1_filters, "filters")
    assert {:raw_sql_filter, sun1_sql} = sun1_filter
    assert IO.iodata_to_binary(sun1_sql) =~ "to_char(selecto_root.created_at, 'D')::int = 1"

    week_filters = %{
      "filters" => [
        %{
          "uuid" => "f2",
          "section" => "filters",
          "filter" => "created_at",
          "comp" => "WEEK_OF_YEAR",
          "value" => "2017-02"
        }
      ]
    }

    [week_filter] = Filters.filter_recurse(datetime_selecto(), week_filters, "filters")
    assert {:raw_sql_filter, week_sql} = week_filter

    assert IO.iodata_to_binary(week_sql) =~
             "to_char(selecto_root.created_at, 'YYYY-WW') = '2017-02'"
  end

  test "bucket parser emits PostgreSQL SQL for numeric and text buckets" do
    numeric_sql = BucketParser.generate_bucket_case_sql("selecto_root.price", "*/10", :integer)
    assert numeric_sql =~ "FLOOR((selecto_root.price)::numeric / 10)"
    assert numeric_sql =~ "+ 9"

    text_sql = BucketParser.generate_text_prefix_case_sql("selecto_root.title")
    assert text_sql =~ "REGEXP_REPLACE"
    assert text_sql =~ "UPPER(LEFT("
    assert text_sql =~ ", 2))"

    custom_text_sql =
      BucketParser.generate_text_prefix_case_sql("title", %{
        "prefix_length" => "3",
        "exclude_articles" => "false"
      })

    refute custom_text_sql =~ "REGEXP_REPLACE"
    assert custom_text_sql =~ ", 3))"
  end

  test "aggregate process emits PostgreSQL text prefix bucket SQL" do
    columns = %{"title" => %{colid: :title, type: :string, name: "Title"}}

    params = %{
      "g1" => %{
        "field" => "title",
        "index" => "0",
        "format" => "text_prefix",
        "prefix_length" => "2",
        "exclude_articles" => "true"
      }
    }

    [{_col, {:field, {:raw_sql, sql}, "title"}}] =
      AggregateProcess.group_by(params, columns, selecto())

    assert sql =~ "REGEXP_REPLACE"
    assert sql =~ "UPPER(LEFT("
    assert sql =~ ", 2))"
  end

  test "graph process emits PostgreSQL age bucket SQL" do
    columns = %{
      "category" => %{colid: :category, type: :string},
      "film_count" => %{colid: :film_id, type: :integer},
      "created_at" => %{colid: :created_at, type: :utc_datetime}
    }

    params = %{
      "x_axis" => %{
        "1" => %{
          "field" => "created_at",
          "index" => "0",
          "alias" => "Age",
          "format" => "age_buckets",
          "bucket_ranges" => "0,1-7,8+"
        }
      },
      "y_axis" => %{
        "1" => %{
          "field" => "film_count",
          "index" => "0",
          "function" => "count",
          "alias" => "Count"
        }
      }
    }

    {view_set, _} = GraphProcess.view(nil, params, columns, [], nil)
    [{_col, field_selector}] = view_set.x_axis_groups

    assert {:field, {:raw_sql, sql}, "Age"} = field_selector
    assert sql =~ "EXTRACT(DAY FROM AGE(CURRENT_DATE, selecto_root.created_at))"
  end

  defp selecto do
    domain = %{
      name: "SelectoComponentsSQLTest",
      source: %{
        source_table: "records",
        primary_key: :id,
        fields: [:id, :title],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          title: %{type: :string}
        },
        associations: %{}
      },
      schemas: %{},
      joins: %{}
    }

    Selecto.configure(domain, nil, validate: false)
  end

  defp datetime_selecto do
    domain = %{
      name: "SelectoComponentsDateTimeSQLTest",
      source: %{
        source_table: "records",
        primary_key: :id,
        fields: [:id, :created_at],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          created_at: %{type: :utc_datetime}
        },
        associations: %{}
      },
      schemas: %{},
      joins: %{}
    }

    Selecto.configure(domain, nil, validate: false)
  end
end
