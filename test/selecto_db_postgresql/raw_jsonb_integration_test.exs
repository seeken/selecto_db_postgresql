defmodule SelectoDBPostgreSQL.RawJSONBIntegrationTest do
  use ExUnit.Case, async: false

  alias SelectoDBPostgreSQL.Adapter
  alias SelectoDBPostgreSQL.Verification.ConnectionOptions

  test "invalid options and conflicting types refuse before connecting" do
    assert {:error, {:invalid_connection_options, :raw_jsonb}} = Adapter.connect(raw_jsonb: :yes)

    assert {:error, {:invalid_connection_options, :raw_jsonb_types_conflict}} =
             Adapter.connect(raw_jsonb: true, types: Postgrex.DefaultTypes)
  end

  @tag :postgres
  test "jsonb passthrough is opt-in; json, encoding, arrays and SQL NULL retain their contracts" do
    for raw? <- [false, true] do
      opts = Keyword.merge(ConnectionOptions.options(), raw_jsonb: raw?, pool_size: 1)
      {:ok, c} = Adapter.connect(opts)
      Process.unlink(c)
      on_exit(fn -> if Process.alive?(c), do: GenServer.stop(c) end)

      object = %{"b" => 1, "aa" => ["café", nil, true]}

      assert {:ok, %{rows: [[jsonb, json, array, nil]]}} =
               Adapter.execute(
                 c,
                 "SELECT $1::jsonb, $1::json, ARRAY[$1::jsonb, NULL::jsonb], NULL::jsonb",
                 [object],
                 selecto_read: true
               )

      assert json == object

      if raw? do
        assert %Jason.Fragment{} = jsonb
        assert Jason.decode!(Jason.encode!(jsonb)) == object
        assert [%Jason.Fragment{}, nil] = array
        assert Jason.decode!(Jason.encode!(array)) == [object, nil]
        assert Jason.decode!(Jason.encode!(%{payload: jsonb})) == %{"payload" => object}
      else
        assert jsonb == object
        assert array == [object, nil]
      end

      assert {:ok, %{rows: rows}} =
               Adapter.execute(
                 c,
                 "SELECT v FROM unnest(ARRAY['null', 'true', '42', '1.5', '\"text\"', '[1,2]']::jsonb[]) AS v",
                 [],
                 selecto_read: true
               )

      values = Enum.map(rows, fn [v] -> if raw?, do: Jason.decode!(Jason.encode!(v)), else: v end)
      assert values == [nil, true, 42, 1.5, "text", [1, 2]]
      assert DBConnection.run(c, &DBConnection.status/1) == :idle
    end

    # Omitting the option is still Postgrex's native map path.
    {:ok, c} = Adapter.connect(ConnectionOptions.options())
    Process.unlink(c)
    on_exit(fn -> if Process.alive?(c), do: GenServer.stop(c) end)

    assert {:ok, %{rows: [[%{"a" => 1}]]}} =
             Adapter.execute(c, "SELECT '{\"a\":1}'::jsonb", [], selecto_read: true)
  end
end
