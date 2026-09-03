defmodule SelectoDBPostgreSQL.CoDomainTest do
  use ExUnit.Case, async: false
  alias SelectoDBPostgreSQL.Adapter

  defp target_domain do
    %{
      name: "Clients",
      source: %{
        source_table: "selecto_lookup_clients",
        primary_key: :id,
        fields: [:id, :name, :city, :tenant_id, :active],
        columns: %{
          id: %{type: :integer},
          name: %{type: :string},
          city: %{type: :string, label: "City"},
          tenant_id: %{type: :integer},
          active: %{type: :boolean}
        },
        associations: %{}
      },
      schemas: %{},
      joins: %{},
      required_filters: [{"tenant_id", 7}],
      query_library: %{
        segments: %{active: %{filters: [{:eq, :active, true}]}},
        projections: %{identity: %{fields: [:id, :name, :city]}},
        orderings: %{by_name: %{order_by: [{:asc, :name}]}},
        views: %{lookup: %{segments: [:active], projection: :identity, ordering: :by_name}}
      }
    }
  end

  defp source_domain do
    target_domain()
    |> Map.put(:co_domains, %{
      carriers: %{
        domain: :client,
        view: :lookup,
        search: %{fields: [:name, :city], mode: :prefix, rank: true},
        result: %{value_field: :id, label_field: :name, description_fields: [:city]}
      }
    })
  end

  defp target(connection) do
    Selecto.configure(target_domain(), Selecto.Runtime.Context.new(Adapter, connection))
  end

  test "lookup plans preserve required filters, bind prefix search, and rank before named ordering" do
    plan =
      Selecto.CoDomain.plan(source_domain(), target(:compile_only), :carriers, "Den'); --",
        scope: {"id", {:>=, 1}},
        limit: 5
      )

    {sql, _, params} = Selecto.gen_sql(plan.query, [])
    assert sql =~ "TO_TSVECTOR"
    assert String.downcase(sql) =~ "ts_rank"
    assert String.downcase(sql) =~ "order by ts_rank"
    assert Enum.member?(params, "den:*")
    assert Enum.member?(params, 7)
    assert Enum.member?(params, 1)
    refute sql =~ "Den'); --"
    assert plan.query.set.limit == 5

    assert {:ok, %{results: []}} =
             Selecto.CoDomain.lookup(source_domain(), target(:compile_only), :carriers, "---")
  end

  test "lookup rejects missing projection fields and excessive request bounds" do
    for text <- ["", String.duplicate("a", 201), <<0>>] do
      assert_raise ArgumentError, fn ->
        Selecto.CoDomain.plan(source_domain(), target(:compile_only), :carriers, text)
      end
    end

    assert_raise ArgumentError, fn ->
      Selecto.CoDomain.plan(source_domain(), target(:compile_only), :carriers, "den", limit: 101)
    end

    invalid = put_in(source_domain(), [:co_domains, :carriers, :result, :value_field], :tenant_id)

    assert_raise ArgumentError, fn ->
      Selecto.CoDomain.plan(invalid, target(:compile_only), :carriers, "den")
    end
  end

  @tag :postgres
  test "lookup executes scoped, bounded, ranked results through PostgreSQL" do
    {:ok, connection} =
      Adapter.connect(SelectoDBPostgreSQL.Verification.ConnectionOptions.options())

    Process.unlink(connection)
    on_exit(fn -> if Process.alive?(connection), do: GenServer.stop(connection) end)

    Postgrex.query!(
      connection,
      "CREATE TEMP TABLE selecto_lookup_clients(id integer, name text, city text, tenant_id integer, active boolean)",
      []
    )

    Postgrex.query!(
      connection,
      "INSERT INTO selecto_lookup_clients VALUES(1,'Denver Freight','Denver',7,true),(2,'Denver Other Tenant','Denver',8,true),(3,'Denver Inactive','Denver',7,false),(4,'Boston Freight','Boston',7,true)",
      []
    )

    assert {:ok, %{results: [%{value: "1", label: "Denver Freight", description: "City Denver"}]}} =
             Selecto.CoDomain.lookup(source_domain(), target(connection), :carriers, "Den",
               scope: {"id", {:>=, 1}},
               limit: 1
             )
  end
end
