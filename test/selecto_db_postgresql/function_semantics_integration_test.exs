defmodule SelectoDBPostgreSQL.FunctionSemanticsIntegrationTest do
  use ExUnit.Case, async: false

  alias Selecto.FunctionVerification.Request
  alias SelectoDBPostgreSQL.Adapter

  @moduletag :postgres

  @text_function "selecto_semantic_normalize_text"
  @bigint_function "selecto_semantic_identity_bigint"
  @predicate_function "selecto_semantic_is_even"
  @table_function "selecto_semantic_expand_text"
  @volatile_function "selecto_semantic_random_unit"

  setup do
    {:ok, connection} =
      Adapter.connect(SelectoDBPostgreSQL.Verification.ConnectionOptions.options())

    Process.unlink(connection)

    on_exit(fn ->
      if Process.alive?(connection), do: GenServer.stop(connection)
    end)

    create_functions(connection)

    on_exit(fn ->
      if Process.alive?(connection), do: drop_functions(connection)
    end)

    %{connection: connection}
  end

  test "controlled scalar fixtures cover null, empty, representative, and boundary values", %{
    connection: connection
  } do
    text_request =
      scalar_request(
        "normalize_text",
        @text_function,
        :string,
        :string,
        :immutable
      )

    text_verification = verify_fixture!(connection, text_request)

    text_cases = [
      {nil, ""},
      {"", ""},
      {"Selecto database", "SELECTO DATABASE"}
    ]

    text_results =
      Enum.map(text_cases, fn {input, expected} ->
        assert {:ok, %{rows: [[actual]], columns: ["value"]}} =
                 Adapter.execute(
                   connection,
                   "SELECT public.#{@text_function}($1::text) AS value",
                   [input],
                   []
                 )

        assert actual == expected
        %{case: semantic_case(input), matched?: true}
      end)

    bigint_request =
      scalar_request(
        "identity_bigint",
        @bigint_function,
        :bigint,
        :bigint,
        :immutable
      )

    bigint_verification = verify_fixture!(connection, bigint_request)
    bigint_cases = [-9_223_372_036_854_775_808, 0, 9_223_372_036_854_775_807]

    bigint_results =
      Enum.map(bigint_cases, fn input ->
        assert {:ok, %{rows: [[^input]], columns: ["value"]}} =
                 Adapter.execute(
                   connection,
                   "SELECT public.#{@bigint_function}($1::bigint) AS value",
                   [input],
                   []
                 )

        %{case: semantic_case(input), matched?: true}
      end)

    assert controlled_report("normalize_text", text_verification, text_results) == %{
             fixture: "normalize_text",
             proof_level: :controlled_live_fixture,
             resolution_status: :database_resolved,
             case_count: 3,
             cases: text_results
           }

    assert controlled_report("identity_bigint", bigint_verification, bigint_results).case_count ==
             3
  end

  test "controlled predicate and table fixtures preserve declared result behavior and shape", %{
    connection: connection
  } do
    predicate_request =
      request!(
        "is_even",
        %{
          kind: :predicate,
          sql_name: "public.#{@predicate_function}",
          args: [%{name: :value, type: :integer, source: :value, null?: false}],
          returns: :boolean,
          database: %{adapters: [:postgresql], volatility: :immutable}
        },
        :filter
      )

    predicate_verification = verify_fixture!(connection, predicate_request)

    for {input, expected} <- [{-1, false}, {0, true}, {2_147_483_647, false}] do
      assert {:ok, %{rows: [[^expected]], columns: ["matches"]}} =
               Adapter.execute(
                 connection,
                 "SELECT public.#{@predicate_function}($1::integer) AS matches",
                 [input],
                 []
               )
    end

    table_request =
      request!(
        "expand_text",
        %{
          kind: :table,
          sql_name: "public.#{@table_function}",
          args: [%{name: :values, type: {:array, :string}, source: :value, null?: false}],
          returns: %{
            columns: %{
              ordinal: %{type: :bigint},
              value: %{type: :string}
            }
          },
          database: %{adapters: [:postgresql], volatility: :stable}
        },
        :lateral
      )

    table_verification = verify_fixture!(connection, table_request)

    assert {:ok, %{rows: [], columns: ["ordinal", "value"]}} =
             Adapter.execute(
               connection,
               "SELECT * FROM public.#{@table_function}($1::text[])",
               [[]],
               []
             )

    assert {:ok,
            %{
              rows: [[1, "alpha"], [2, ""], [3, "omega"]],
              columns: ["ordinal", "value"]
            }} =
             Adapter.execute(
               connection,
               "SELECT * FROM public.#{@table_function}($1::text[])",
               [["alpha", "", "omega"]],
               []
             )

    assert controlled_report("is_even", predicate_verification, [
             %{case: :negative_odd, matched?: true},
             %{case: :zero, matched?: true},
             %{case: :maximum_integer, matched?: true}
           ]).proof_level == :controlled_live_fixture

    assert controlled_report("expand_text", table_verification, [
             %{case: :empty, matched?: true},
             %{case: :representative, matched?: true}
           ]).case_count == 2
  end

  test "volatile fixture asserts only type, range, and finite shape invariants", %{
    connection: connection
  } do
    request =
      request!(
        "random_unit",
        %{
          kind: :scalar,
          sql_name: "public.#{@volatile_function}",
          args: [],
          returns: :float,
          database: %{adapters: [:postgresql], volatility: :volatile}
        },
        :select
      )

    verification = verify_fixture!(connection, request)

    cases =
      Enum.map(1..10, fn _index ->
        assert {:ok, %{rows: [[value]], columns: ["value"]}} =
                 Adapter.execute(
                   connection,
                   "SELECT public.#{@volatile_function}() AS value",
                   [],
                   []
                 )

        assert is_float(value)
        assert value >= 0.0
        assert value < 1.0
        %{case: :volatile_sample, invariant_satisfied?: true}
      end)

    report = controlled_report("random_unit", verification, cases)
    assert report.proof_level == :controlled_live_fixture
    assert report.case_count == 10
    refute Map.has_key?(report, :expected_value)
  end

  defp create_functions(connection) do
    execute!(connection, """
    CREATE OR REPLACE FUNCTION public.#{@text_function}(text)
    RETURNS text
    LANGUAGE sql
    IMMUTABLE
    CALLED ON NULL INPUT
    AS $function$
      SELECT upper(COALESCE($1, ''))
    $function$
    """)

    execute!(connection, """
    CREATE OR REPLACE FUNCTION public.#{@bigint_function}(bigint)
    RETURNS bigint
    LANGUAGE sql
    IMMUTABLE
    STRICT
    AS $function$
      SELECT $1
    $function$
    """)

    execute!(connection, """
    CREATE OR REPLACE FUNCTION public.#{@predicate_function}(integer)
    RETURNS boolean
    LANGUAGE sql
    IMMUTABLE
    STRICT
    AS $function$
      SELECT mod($1, 2) = 0
    $function$
    """)

    execute!(connection, """
    CREATE OR REPLACE FUNCTION public.#{@table_function}(text[])
    RETURNS TABLE(ordinal bigint, value text)
    LANGUAGE sql
    STABLE
    STRICT
    AS $function$
      SELECT expanded.ordinality::bigint, expanded.value
      FROM unnest($1) WITH ORDINALITY AS expanded(value, ordinality)
    $function$
    """)

    execute!(connection, """
    CREATE OR REPLACE FUNCTION public.#{@volatile_function}()
    RETURNS double precision
    LANGUAGE sql
    VOLATILE
    AS $function$
      SELECT random()
    $function$
    """)
  end

  defp drop_functions(connection) do
    execute!(connection, "DROP FUNCTION IF EXISTS public.#{@volatile_function}()")
    execute!(connection, "DROP FUNCTION IF EXISTS public.#{@table_function}(text[])")
    execute!(connection, "DROP FUNCTION IF EXISTS public.#{@predicate_function}(integer)")
    execute!(connection, "DROP FUNCTION IF EXISTS public.#{@bigint_function}(bigint)")
    execute!(connection, "DROP FUNCTION IF EXISTS public.#{@text_function}(text)")
  end

  defp verify_fixture!(connection, request) do
    assert {:ok, verification} = Adapter.verify_function(connection, request, [])
    assert verification.status == :database_resolved, inspect(verification)
    assert verification.evidence.function_executed == false
    verification
  end

  defp scalar_request(function_id, function_name, argument_type, return_type, volatility) do
    request!(
      function_id,
      %{
        kind: :scalar,
        sql_name: "public.#{function_name}",
        args: [%{name: :value, type: argument_type, source: :value}],
        returns: return_type,
        database: %{adapters: [:postgresql], volatility: volatility}
      },
      :select
    )
  end

  defp request!(function_id, signature, call_site) do
    assert {:ok, request} = Request.new(function_id, signature, call_site)
    request
  end

  defp controlled_report(fixture, verification, cases) do
    %{
      fixture: fixture,
      proof_level: :controlled_live_fixture,
      resolution_status: verification.status,
      case_count: length(cases),
      cases: cases
    }
  end

  defp semantic_case(nil), do: :null
  defp semantic_case(""), do: :empty
  defp semantic_case(_value), do: :representative

  defp execute!(connection, sql) do
    assert {:ok, _result} = Adapter.execute(connection, sql, [], prepared: false)
  end
end
