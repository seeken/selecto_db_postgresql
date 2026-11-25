defmodule SelectoDBPostgreSQL.CoreFixesTest do
  use ExUnit.Case, async: false
  alias SelectoDBPostgreSQL.Adapter

  defmodule ClockSpy do
    for {function, arity} <- SelectoDBPostgreSQL.Adapter.__info__(:functions),
        {function, arity} != {:execute, 4} do
      args = Macro.generate_arguments(arity, __MODULE__)

      def unquote(function)(unquote_splicing(args)),
        do: apply(SelectoDBPostgreSQL.Adapter, unquote(function), [unquote_splicing(args)])
    end

    def execute(conn, sql, params, opts) do
      send(self(), {:clock_build_sql, sql})
      SelectoDBPostgreSQL.Adapter.execute(conn, sql, params, opts)
    end
  end

  setup do
    {:ok, conn} =
      Postgrex.start_link(
        hostname: "127.0.0.1",
        port: 55481,
        username: "chris",
        database: "selecto_perf",
        pool_size: 1
      )

    Process.unlink(conn)
    on_exit(fn -> if Process.alive?(conn), do: GenServer.stop(conn) end)
    %{conn: conn}
  end

  defp sql!(conn, sql), do: Postgrex.query!(conn, sql, [])

  defp domain(type, extra \\ %{}) do
    %{
      name: "Clock fixes",
      source: %{
        source_table: "fix_events",
        primary_key: :id,
        fields: [:id, :at, :age],
        associations: %{},
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          at: Map.merge(%{type: type}, extra),
          age: %{
            type: :integer,
            computed: %{kind: :expression, expression: ["days_between", ["field", "at"], ["now"]]}
          }
        }
      },
      schemas: %{},
      joins: %{}
    }
  end

  defp base(conn, domain) do
    Selecto.configure(domain, Selecto.Runtime.Context.new(Adapter, conn), adapter: Adapter)
  end

  defp rows(query) do
    assert {:ok, {rows, _, _}} = Selecto.execute(query, analyze_complexity: false)
    rows
  end

  test "query date agrees across shortcut, computed age and relative buckets", %{conn: conn} do
    sql!(conn, "CREATE TEMP TABLE fix_events(id INTEGER, at DATE)")
    sql!(conn, "INSERT INTO fix_events VALUES(1,'2026-03-15'),(2,'2026-03-14')")

    for {clock, zone} <- [
          {"2026-03-15", "Asia/Tokyo"},
          {"2026-03-15T20:00:00-06:00", "America/Denver"}
        ] do
      q = base(conn, domain(:date)) |> Selecto.as_of(clock) |> Selecto.timezone(zone)

      assert rows(q |> Selecto.select(["id", "age"]) |> Selecto.order_by(["id"])) == [
               [1, 0],
               [2, 1]
             ]

      assert rows(
               q
               |> Selecto.select(["id"])
               |> Selecto.filter([Selecto.Expr.date_shortcut("at", "today")])
             ) == [[1]]

      for kind <- [:elapsed_days_ranges, :date_relative_ranges] do
        ranges =
          if kind == :date_relative_ranges,
            do: [{"today", "today", "x"}, {"yesterday", "yesterday", "y"}],
            else: [{0, 0, "Today"}, {1, 1, "Yesterday"}]

        assert rows(
                 q
                 |> Selecto.select(["id", {:bucket, "at", %{kind: kind, ranges: ranges}}])
                 |> Selecto.order_by(["id"])
               ) == [[1, "Today"], [2, "Yesterday"]]
      end

      assert rows(q |> Selecto.select([{:count_age_bucket, "at", 0, 0}])) == [[1]]
    end
  end

  test "naive UTC on and shortcut bounds ignore the session timezone", %{conn: conn} do
    sql!(conn, "SET TIME ZONE 'America/Denver'")
    sql!(conn, "CREATE TEMP TABLE fix_events(id INTEGER, at TIMESTAMP WITHOUT TIME ZONE)")

    sql!(
      conn,
      "INSERT INTO fix_events VALUES(1,'2026-03-15 20:00:00'),(2,'2026-03-16 03:00:00'),(3,'2026-03-15 01:00:00')"
    )

    q =
      base(conn, domain(:utc_datetime, %{storage: :naive_utc}))
      |> Selecto.timezone("Etc/UTC")
      |> Selecto.as_of("2026-03-15")
      |> Selecto.select(["id"])
      |> Selecto.order_by(["id"])

    for predicate <- [
          Selecto.Expr.on("at", "2026-03-15"),
          Selecto.Expr.date_shortcut("at", "today")
        ] do
      assert rows(Selecto.filter(q, [predicate])) == [[1], [3]]
    end
  end

  test "unknown session timezone refuses cleanly", %{conn: conn} do
    sql!(conn, "SET TIME ZONE '<+03>-03'")
    sql!(conn, "CREATE TEMP TABLE fix_events(id INTEGER, at TIMESTAMPTZ)")

    q =
      base(conn, domain(:timestamptz))
      |> Selecto.select(["id"])
      |> Selecto.filter([Selecto.Expr.on("at", "2026-03-15")])

    assert {:error, %{details: %{code: :invalid_query}}} =
             Selecto.execute(q, analyze_complexity: false)
  end

  test "unbound shortcuts use database clock without build-time queries", %{conn: conn} do
    # A compile-only connection must stay completely untouched.
    for zone <- [nil, "Asia/Tokyo"],
        shortcut <- ["today", "last_month", "this_week", "qtd_all_years"] do
      q = base(:compile_only, domain(:date)) |> Selecto.select(["id"])
      q = if zone, do: Selecto.timezone(q, zone), else: q

      {sql, _, params} =
        Selecto.gen_sql(Selecto.filter(q, [Selecto.Expr.date_shortcut("at", shortcut)]), [])

      assert sql =~ "CURRENT_"
      refute Enum.any?(params, &match?(<<_::binary-size(4), "-", _::binary>>, &1))
    end

    sql!(conn, "CREATE TEMP TABLE fix_events(id INTEGER, at DATE)")
    sql!(conn, "INSERT INTO fix_events VALUES(1,CURRENT_DATE),(2,CURRENT_DATE-1)")

    q =
      base(conn, domain(:date))
      |> Selecto.select(["id"])
      |> Selecto.filter([Selecto.Expr.date_shortcut("at", "today")])

    assert rows(q) == [[1]]
  end

  test "all dynamic calendar shortcuts agree with bound database date", %{conn: conn} do
    sql!(conn, "SET TIME ZONE 'America/Denver'")
    sql!(conn, "CREATE TEMP TABLE fix_events(id INTEGER, at DATE)")

    sql!(
      conn,
      "INSERT INTO fix_events SELECT n, CURRENT_DATE + n FROM generate_series(-400,400,5) n"
    )

    %{rows: [[today]]} =
      sql!(conn, "SELECT CAST(CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo' AS DATE)")

    today = to_string(today)

    base =
      base(conn, domain(:date))
      |> Selecto.timezone("Asia/Tokyo")
      |> Selecto.select(["id"])
      |> Selecto.order_by(["id"])

    for shortcut <-
          ~w(today yesterday tomorrow this_week last_week next_week this_month last_month next_month mtd this_quarter last_quarter next_quarter qtd this_year last_year next_year ytd last_7_days last_14_days last_30_days last_90_days next_7_days next_30_days last_3_months this_and_last_month this_and_last_year mtd_all_years qtd_all_years ytd_all_years) do
      query = Selecto.filter(base, [Selecto.Expr.date_shortcut("at", shortcut)])
      assert rows(query) == rows(Selecto.as_of(query, today)), shortcut
    end
  end

  test "month buckets and now use the query-zone day across UTC month boundaries", %{conn: conn} do
    sql!(conn, "CREATE TEMP TABLE fix_events(id INTEGER, at DATE)")
    sql!(conn, "INSERT INTO fix_events VALUES(1,'2026-03-01'),(2,'2026-02-28')")

    for {clock, zone, expected} <- [
          {"2026-03-01", "Asia/Tokyo", [[1, "This"], [2, "Previous"]]},
          {"2026-02-28T22:00:00-07:00", "America/Denver", [[1, "Other"], [2, "This"]]}
        ] do
      q = base(conn, domain(:date)) |> Selecto.as_of(clock) |> Selecto.timezone(zone)

      assert rows(
               q
               |> Selecto.select([
                 "id",
                 {:bucket, "at",
                  %{kind: :month_ranges, ranges: [{0, 0, "This"}, {1, 1, "Previous"}]}}
               ])
               |> Selecto.order_by(["id"])
             ) == expected

      # Exercise the standalone now node independently from days_between.
      {sql, _, params} =
        Selecto.gen_sql(q |> Selecto.select([{:computed_value, ["cast", ["now"], "date"]}]), [])

      assert sql != ""
      assert Enum.all?(params, &String.starts_with?(&1, String.slice(clock, 0, 10)))
    end
  end

  test "one compiled shortcut follows execution-time session dates without build I/O", %{
    conn: conn
  } do
    sql!(conn, "CREATE TEMP TABLE fix_events(id INTEGER, at DATE)")
    zones = ["Pacific/Pago_Pago", "Pacific/Kiritimati"]

    dates =
      Enum.map(zones, fn zone ->
        sql!(conn, "SET TIME ZONE '" <> zone <> "'")
        %{rows: [[date]]} = sql!(conn, "SELECT CURRENT_DATE")
        date
      end)

    assert Enum.uniq(dates) == dates
    Postgrex.query!(conn, "INSERT INTO fix_events VALUES(1,$1),(2,$2)", dates)

    q =
      base(conn, domain(:date))
      |> Selecto.select(["id"])
      |> Selecto.filter([Selecto.Expr.date_shortcut("at", "today")])

    q = %{q | adapter: ClockSpy}
    {sql, _, params} = Selecto.gen_sql(q, [])
    refute_receive {:clock_build_sql, _}

    for {zone, id} <- Enum.with_index(zones, 1) do
      sql!(conn, "SET TIME ZONE '" <> zone <> "'")
      assert {:ok, %{rows: [[^id]]}} = Adapter.execute(conn, sql, params, [])
    end
  end
end
