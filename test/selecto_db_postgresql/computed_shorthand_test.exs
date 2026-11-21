defmodule SelectoDBPostgreSQL.ComputedShorthandTest do
  use ExUnit.Case, async: true

  alias Selecto.Dialect.Bucket.Expression, as: BucketExpression
  alias SelectoDBPostgreSQL.{Adapter, Dialect}

  defp days_between(expression, type \\ "integer") do
    %{
      __struct__: Selecto.Dialect.ComputedValue,
      operation: :days_between,
      expression: expression,
      type: type,
      path: []
    }
  end

  test "days_between subtracts the two dates" do
    assert {:ok, rendered} =
             Dialect.render_computed_value(
               days_between(["selecto_root.purchased_at", {:param, "2026-06-30"}]),
               %{}
             )

    assert Selecto.SQL.Params.finalize(rendered, adapter: Adapter) ==
             {"(CAST($1 AS DATE) - CAST(selecto_root.purchased_at AS DATE))", ["2026-06-30"]}
  end

  test "days_between takes exactly two compiled operands" do
    for fragment <- [
          days_between(["a"]),
          days_between(["a", "b", "c"]),
          days_between(["a", ""]),
          days_between(["a", %{sql: "b"}]),
          days_between(["a", "b"], nil)
        ] do
      assert {:error, %Selecto.Error{details: %{unsupported_feature: :computed_value}}} =
               Dialect.render_computed_value(fragment, %{})
    end
  end

  test "month_ranges counts calendar months back from the current month" do
    assert {:ok, sql} =
             Dialect.render_bucket(
               %BucketExpression{
                 kind: :month_ranges,
                 expression: "received",
                 ranges: [{0, 0, "this"}, {3, 5, "quarter"}, {13, :infinity, "older"}]
               },
               %{}
             )

    sql = IO.iodata_to_binary(sql)

    months =
      "((DATE_PART('year', CURRENT_DATE) * 12 + DATE_PART('month', CURRENT_DATE)) - " <>
        "(DATE_PART('year', received) * 12 + DATE_PART('month', received)))"

    assert sql ==
             "CASE WHEN #{months} = 0 THEN 'this' " <>
               "WHEN #{months} >= 3 AND #{months} <= 5 THEN 'quarter' " <>
               "WHEN #{months} >= 13 THEN 'older' ELSE 'Other' END"

    assert {:error, _} =
             Dialect.render_bucket(
               %BucketExpression{kind: :month_ranges, expression: "received", ranges: []},
               %{}
             )
  end

  describe "against PostgreSQL" do
    @describetag :postgres

    setup do
      {:ok, connection} =
        Adapter.connect(SelectoDBPostgreSQL.Verification.ConnectionOptions.options())

      Process.unlink(connection)
      on_exit(fn -> if Process.alive?(connection), do: GenServer.stop(connection) end)
      %{connection: connection}
    end

    test "temporal_delta and bucket columns run as days_between and case", %{
      connection: connection
    } do
      for sql <- [
            "CREATE TEMP TABLE selecto_cs_events (id integer PRIMARY KEY, starts_on date)",
            "CREATE TEMP TABLE selecto_cs_tickets (id integer PRIMARY KEY, event_id integer, purchased_at timestamp)",
            "INSERT INTO selecto_cs_events VALUES (1, '2026-06-30'), (2, NULL)",
            "INSERT INTO selecto_cs_tickets VALUES (1, 1, '2026-06-30 23:59:00'), (2, 1, '2026-06-24 00:00:01'), (5, 1, '2026-07-02 09:00:00'), (6, 2, '2026-03-01 09:00:00'), (10, NULL, '2026-06-01 00:00:00')"
          ],
          do: Postgrex.query!(connection, sql, [])

      query =
        connection
        |> tickets()
        |> Selecto.select(["id", "lead_days", "lead_bucket"])
        |> Selecto.order_by("id")

      assert {:ok, {rows, _columns, _aliases}} = Selecto.execute(query)

      assert rows == [
               [1, 0, "soon"],
               [2, 6, "soon"],
               [5, -2, "after"],
               [6, nil, "Unknown"],
               [10, nil, "Unknown"]
             ]
    end

    test "month_ranges labels dates by calendar months ago", %{connection: connection} do
      Postgrex.query!(
        connection,
        "CREATE TEMP TABLE selecto_cs_loads (id integer, received date)",
        []
      )

      Postgrex.query!(
        connection,
        "INSERT INTO selecto_cs_loads SELECT i, (date_trunc('month', CURRENT_DATE) - make_interval(months => ago) + interval '14 days')::date FROM unnest(ARRAY[0, 1, 2, 4, 6, 12, 13]) WITH ORDINALITY AS t(ago, i) UNION ALL SELECT 8, NULL",
        []
      )

      bucket =
        {:bucket, "received",
         %{
           kind: :month_ranges,
           ranges: [
             {6, 12, "half year"},
             {3, 5, "quarter"},
             {2, 2, "two"},
             {1, 1, "last"},
             {0, 0, "this"}
           ]
         }}

      query =
        %{
          name: "Loads",
          source: %{
            source_table: "selecto_cs_loads",
            source_schema: "pg_temp",
            primary_key: :id,
            fields: [:id, :received],
            columns: %{id: %{type: :integer}, received: %{type: :date}},
            associations: %{},
            redact_fields: []
          },
          schemas: %{},
          joins: %{}
        }
        |> Selecto.configure(connection, adapter: Adapter)
        |> Selecto.select(["id", Selecto.Expr.as(bucket, "band")])
        |> Selecto.order_by("id")

      assert {:ok, {rows, _columns, _aliases}} = Selecto.execute(query)

      assert Enum.map(rows, &List.last/1) ==
               ["this", "last", "two", "quarter", "half year", "half year", "Other", "Other"]
    end
  end

  defp tickets(connection) do
    %{
      name: "Tickets",
      source: %{
        source_table: "selecto_cs_tickets",
        source_schema: "pg_temp",
        primary_key: :id,
        fields: [:id, :event_id, :purchased_at, :lead_days, :lead_bucket],
        columns: %{
          id: %{type: :integer},
          event_id: %{type: :integer},
          purchased_at: %{type: :naive_datetime},
          lead_days: %{
            type: :integer,
            computed: %{kind: :temporal_delta, from: "purchased_at", to: "event.starts_on"}
          },
          lead_bucket: %{
            type: :string,
            computed: %{
              kind: :bucket,
              field: "lead_days",
              otherwise: "Unknown",
              buckets: [%{label: "after", max: -1}, %{label: "soon", min: 0, max: 6}]
            }
          }
        },
        associations: %{
          event: %{queryable: :event, field: :event, owner_key: :event_id, related_key: :id}
        },
        redact_fields: []
      },
      schemas: %{
        event: %{
          source_table: "selecto_cs_events",
          source_schema: "pg_temp",
          primary_key: :id,
          fields: [:id, :starts_on],
          columns: %{id: %{type: :integer}, starts_on: %{type: :date}},
          associations: %{},
          redact_fields: []
        }
      },
      joins: %{event: %{type: :left}}
    }
    |> Selecto.configure(connection, adapter: Adapter)
  end
end
