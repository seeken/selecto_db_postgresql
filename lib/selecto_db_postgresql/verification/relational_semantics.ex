defmodule SelectoDBPostgreSQL.Verification.RelationalSemantics do
  @moduledoc """
  Bounded live differential checking for a deliberately small relational model.

  The checker enumerates every subset of two canonical three-row tables and
  compares an independent Elixir interpreter with Selecto SQL executed by
  PostgreSQL. Its scope includes bag projection, nullable predicates, inner and
  left joins, grouping with count and sum, PostgreSQL null ordering, and limit.

  This is live conformance evidence, not a database-independent theorem. A passing
  report uses `proof_level: :bounded_live_differential` and records the exact
  number of database-backed cases checked.
  """

  alias SelectoDBPostgreSQL.Adapter

  @model "selecto_db_postgresql.relational_semantics.v1"
  @left_table "selecto_semantic_left"
  @right_table "selecto_semantic_right"

  @left_rows [
    %{id: 1, group_key: "a", value: nil, rank: 2, right_id: 1},
    %{id: 2, group_key: "a", value: 10, rank: 1, right_id: 2},
    %{id: 3, group_key: nil, value: 20, rank: 3, right_id: nil}
  ]

  @right_rows [
    %{id: 1, label: "one"},
    %{id: 2, label: nil},
    %{id: 4, label: "four"}
  ]

  @type join_kind :: :inner | :left
  @type query_spec ::
          :bag_projection
          | :is_null
          | :greater_than
          | :filter_conjunction
          | :group_count
          | :group_sum
          | :order_ascending
          | :order_descending
          | {:parameter_equal, String.t()}
          | {:limit, non_neg_integer()}
          | {:join, join_kind()}

  @type report :: %{
          format: String.t(),
          format_version: pos_integer(),
          proof_level: :bounded_live_differential,
          model: String.t(),
          state_count: non_neg_integer(),
          invariant_count: pos_integer(),
          check_count: non_neg_integer(),
          proved?: boolean(),
          counterexamples: [map()]
        }

  @doc "Runs every bounded relational case against a live PostgreSQL connection."
  @spec check(term()) :: report()
  def check(connection) do
    cases = cases()

    counterexamples =
      case prepare_tables(connection) do
        :ok ->
          {counterexamples, _loaded_dataset} =
            cases
            |> Enum.with_index()
            |> Enum.reduce({[], nil}, fn {verification_case, index},
                                         {counterexamples, loaded_dataset} ->
              dataset = {verification_case.left_mask, verification_case.right_mask}

              case check_case(connection, verification_case, index, dataset != loaded_dataset) do
                [] -> {counterexamples, dataset}
                failures -> {counterexamples ++ failures, dataset}
              end
            end)

          counterexamples

        {:error, reason} ->
          [%{state_index: 0, state: %{phase: :setup}, reason: portable(reason)}]
      end

    %{
      format: "selecto.formal_verification",
      format_version: 1,
      proof_level: :bounded_live_differential,
      model: @model,
      state_count: length(cases),
      invariant_count: 1,
      check_count: length(cases),
      proved?: counterexamples == [],
      counterexamples: counterexamples
    }
  end

  @doc false
  def cases do
    unary =
      for left_mask <- 0..7,
          query <- [
            :bag_projection,
            :is_null,
            :greater_than,
            {:parameter_equal, "a"},
            {:parameter_equal, "z"},
            :filter_conjunction,
            :group_count,
            :group_sum,
            :order_ascending,
            :order_descending,
            {:limit, 0},
            {:limit, 1},
            {:limit, 2}
          ] do
        %{left_mask: left_mask, right_mask: 0, query: query}
      end

    joins =
      for left_mask <- 0..7,
          right_mask <- 0..7,
          join_type <- [:inner, :left] do
        %{left_mask: left_mask, right_mask: right_mask, query: {:join, join_type}}
      end

    unary ++ joins
  end

  defp check_case(connection, verification_case, index, reload?) do
    left_rows = subset(@left_rows, verification_case.left_mask)
    right_rows = subset(@right_rows, verification_case.right_mask)

    result =
      with :ok <- maybe_load_rows(connection, left_rows, right_rows, reload?),
           {:ok, actual} <- execute_selecto(connection, verification_case.query),
           expected <- interpret(verification_case.query, left_rows, right_rows),
           :ok <- compare(verification_case.query, actual, expected) do
        :ok
      end

    case result do
      :ok ->
        []

      {:error, reason} ->
        [
          %{
            invariant: "generated SQL matches independent relational semantics",
            invariant_index: 0,
            state_index: index,
            state: portable(verification_case),
            reason: portable(reason)
          }
        ]
    end
  rescue
    exception ->
      [
        %{
          invariant: "generated SQL matches independent relational semantics",
          invariant_index: 0,
          state_index: index,
          state: portable(verification_case),
          reason: %{
            exception: inspect(exception.__struct__),
            message: Exception.message(exception)
          }
        }
      ]
  catch
    kind, reason ->
      [
        %{
          invariant: "generated SQL matches independent relational semantics",
          invariant_index: 0,
          state_index: index,
          state: portable(verification_case),
          reason: %{caught: kind, value: portable(reason)}
        }
      ]
  end

  defp maybe_load_rows(connection, left_rows, right_rows, true),
    do: load_rows(connection, left_rows, right_rows)

  defp maybe_load_rows(_connection, _left_rows, _right_rows, false), do: :ok

  defp prepare_tables(connection) do
    with {:ok, _result} <-
           Adapter.execute(
             connection,
             """
             CREATE TEMP TABLE IF NOT EXISTS #{@left_table} (
               id integer PRIMARY KEY,
               group_key text NULL,
               value integer NULL,
               rank integer NOT NULL,
               right_id integer NULL
             )
             """,
             [],
             []
           ),
         {:ok, _result} <-
           Adapter.execute(
             connection,
             """
             CREATE TEMP TABLE IF NOT EXISTS #{@right_table} (
               id integer PRIMARY KEY,
               label text NULL
             )
             """,
             [],
             []
           ) do
      :ok
    end
  end

  defp load_rows(connection, left_rows, right_rows) do
    with {:ok, _result} <-
           Adapter.execute(
             connection,
             "TRUNCATE TABLE #{@left_table}, #{@right_table}",
             [],
             []
           ),
         :ok <- insert_left_rows(connection, left_rows),
         :ok <- insert_right_rows(connection, right_rows) do
      :ok
    end
  end

  defp insert_left_rows(connection, rows) do
    Enum.reduce_while(rows, :ok, fn row, :ok ->
      case Adapter.execute(
             connection,
             "INSERT INTO #{@left_table} (id, group_key, value, rank, right_id) VALUES ($1, $2, $3, $4, $5)",
             [row.id, row.group_key, row.value, row.rank, row.right_id],
             []
           ) do
        {:ok, _result} -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp insert_right_rows(connection, rows) do
    Enum.reduce_while(rows, :ok, fn row, :ok ->
      case Adapter.execute(
             connection,
             "INSERT INTO #{@right_table} (id, label) VALUES ($1, $2)",
             [row.id, row.label],
             []
           ) do
        {:ok, _result} -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp execute_selecto(connection, query_spec) do
    query = build_query(connection, query_spec)
    {sql, _aliases, params} = Selecto.gen_sql(query, [])

    with :ok <- parameter_contract(query_spec, sql, params) do
      case Adapter.execute(connection, sql, params, []) do
        {:ok, %{rows: rows}} -> {:ok, normalize(rows)}
        {:error, reason} -> {:error, {:postgresql_execution_failed, reason, sql, params}}
      end
    end
  end

  defp build_query(connection, {:join, join_type}) when join_type in [:inner, :left] do
    join_type
    |> domain()
    |> Selecto.configure(connection, adapter: Adapter, validate: false)
    |> Selecto.join(:semantic_right, type: join_type)
    |> Selecto.select(["id", "semantic_right.label"])
    |> Selecto.order_by([{:asc, "id"}])
  end

  defp build_query(connection, :bag_projection) do
    base_query(connection)
    |> Selecto.select(["group_key"])
  end

  defp build_query(connection, :is_null) do
    base_query(connection)
    |> Selecto.select(["id", "value"])
    |> Selecto.filter({"value", nil})
  end

  defp build_query(connection, :greater_than) do
    base_query(connection)
    |> Selecto.select(["id", "value"])
    |> Selecto.filter({"value", {:gt, 10}})
  end

  defp build_query(connection, {:parameter_equal, value}) when is_binary(value) do
    base_query(connection)
    |> Selecto.select(["id", "group_key"])
    |> Selecto.filter({"group_key", value})
  end

  defp build_query(connection, :filter_conjunction) do
    base_query(connection)
    |> Selecto.select(["id", "value", "rank"])
    |> Selecto.filter([{"value", {:gt, 5}}, {"rank", {:lt, 3}}])
  end

  defp build_query(connection, :group_count) do
    base_query(connection)
    |> Selecto.select(["group_key", {:count, "*"}])
    |> Selecto.group_by(["group_key"])
  end

  defp build_query(connection, :group_sum) do
    base_query(connection)
    |> Selecto.select(["group_key", {:sum, "value"}])
    |> Selecto.group_by(["group_key"])
  end

  defp build_query(connection, :order_ascending) do
    base_query(connection)
    |> Selecto.select(["id", "value"])
    |> Selecto.order_by([{:asc, "value"}, {:asc, "id"}])
  end

  defp build_query(connection, :order_descending) do
    base_query(connection)
    |> Selecto.select(["id", "value"])
    |> Selecto.order_by([{:desc, "value"}, {:asc, "id"}])
  end

  defp build_query(connection, {:limit, limit}) when is_integer(limit) and limit >= 0 do
    base_query(connection)
    |> Selecto.select(["id", "value"])
    |> Selecto.order_by([{:asc, "id"}])
    |> Selecto.limit(limit)
  end

  defp base_query(connection) do
    :left
    |> domain()
    |> Selecto.configure(connection, adapter: Adapter, validate: false)
  end

  @spec domain(join_kind()) :: Selecto.Types.domain()
  defp domain(join_type) when join_type in [:inner, :left] do
    %{
      name: "Bounded relational semantics",
      source: %{
        source_table: @left_table,
        primary_key: :id,
        fields: [:id, :group_key, :value, :rank, :right_id],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          group_key: %{type: :string},
          value: %{type: :integer},
          rank: %{type: :integer},
          right_id: %{type: :integer}
        },
        associations: %{
          semantic_right: %{
            queryable: :semantic_right,
            field: :semantic_right,
            owner_key: :right_id,
            related_key: :id
          }
        }
      },
      schemas: %{
        semantic_right: %{
          name: "Semantic right",
          source_table: @right_table,
          primary_key: :id,
          fields: [:id, :label],
          redact_fields: [],
          columns: %{id: %{type: :integer}, label: %{type: :string}},
          associations: %{}
        }
      },
      joins: %{
        semantic_right: %{
          name: "Semantic right",
          type: join_type,
          display_field: :label
        }
      }
    }
  end

  defp interpret(:bag_projection, left_rows, _right_rows) do
    Enum.map(left_rows, &[&1.group_key])
  end

  defp interpret(:is_null, left_rows, _right_rows) do
    left_rows
    |> Enum.filter(&is_nil(&1.value))
    |> Enum.map(&[&1.id, &1.value])
  end

  defp interpret(:greater_than, left_rows, _right_rows) do
    left_rows
    |> Enum.filter(&(is_integer(&1.value) and &1.value > 10))
    |> Enum.map(&[&1.id, &1.value])
  end

  defp interpret({:parameter_equal, value}, left_rows, _right_rows) do
    left_rows
    |> Enum.filter(&(&1.group_key == value))
    |> Enum.map(&[&1.id, &1.group_key])
  end

  defp interpret(:filter_conjunction, left_rows, _right_rows) do
    left_rows
    |> Enum.filter(&(is_integer(&1.value) and &1.value > 5 and &1.rank < 3))
    |> Enum.map(&[&1.id, &1.value, &1.rank])
  end

  defp interpret(:group_count, left_rows, _right_rows) do
    left_rows
    |> Enum.group_by(& &1.group_key)
    |> Enum.map(fn {group_key, rows} -> [group_key, length(rows)] end)
  end

  defp interpret(:group_sum, left_rows, _right_rows) do
    left_rows
    |> Enum.group_by(& &1.group_key)
    |> Enum.map(fn {group_key, rows} ->
      values = rows |> Enum.map(& &1.value) |> Enum.reject(&is_nil/1)
      [group_key, if(values == [], do: nil, else: Enum.sum(values))]
    end)
  end

  defp interpret(:order_ascending, left_rows, _right_rows) do
    left_rows
    |> Enum.sort_by(fn row -> {if(is_nil(row.value), do: 1, else: 0), row.value || 0, row.id} end)
    |> Enum.map(&[&1.id, &1.value])
  end

  defp interpret(:order_descending, left_rows, _right_rows) do
    left_rows
    |> Enum.sort_by(fn row ->
      {if(is_nil(row.value), do: 0, else: 1), -(row.value || 0), row.id}
    end)
    |> Enum.map(&[&1.id, &1.value])
  end

  defp interpret({:limit, limit}, left_rows, _right_rows) do
    left_rows
    |> Enum.sort_by(& &1.id)
    |> Enum.take(limit)
    |> Enum.map(&[&1.id, &1.value])
  end

  defp interpret({:join, join_type}, left_rows, right_rows) do
    right_by_id = Map.new(right_rows, &{&1.id, &1})

    left_rows
    |> Enum.sort_by(& &1.id)
    |> Enum.flat_map(fn left ->
      case Map.get(right_by_id, left.right_id) do
        nil when join_type == :inner -> []
        nil -> [[left.id, nil]]
        right -> [[left.id, right.label]]
      end
    end)
  end

  defp compare(query, actual, expected)
       when query in [
              :bag_projection,
              :is_null,
              :greater_than,
              :filter_conjunction,
              :group_count,
              :group_sum
            ] or
              (is_tuple(query) and elem(query, 0) == :parameter_equal) do
    if bag(actual) == bag(normalize(expected)),
      do: :ok,
      else: {:error, %{query: portable(query), expected: expected, actual: actual}}
  end

  defp compare(query, actual, expected) do
    expected = normalize(expected)

    if actual == expected,
      do: :ok,
      else: {:error, %{query: portable(query), expected: expected, actual: actual}}
  end

  defp parameter_contract({:parameter_equal, value}, sql, params) do
    if params == [value] and String.contains?(sql, "$1") and
         not String.contains?(sql, "'#{value}'") do
      :ok
    else
      {:error, {:parameter_contract_mismatch, sql, params, [value]}}
    end
  end

  defp parameter_contract(:filter_conjunction, sql, params) do
    if params == [5, 3] and String.contains?(sql, "$1") and String.contains?(sql, "$2") do
      :ok
    else
      {:error, {:conjunction_parameter_contract_mismatch, sql, params}}
    end
  end

  defp parameter_contract(_query, _sql, _params), do: :ok

  defp subset(rows, mask) do
    rows
    |> Enum.with_index()
    |> Enum.filter(fn {_row, index} -> Bitwise.band(mask, Bitwise.bsl(1, index)) != 0 end)
    |> Enum.map(&elem(&1, 0))
  end

  defp bag(rows), do: Enum.frequencies_by(rows, &:erlang.term_to_binary/1)

  defp normalize(value) when is_list(value), do: Enum.map(value, &normalize/1)
  defp normalize(%Decimal{} = value), do: Decimal.to_string(value, :normal)
  defp normalize(value), do: value

  defp portable(%module{} = value),
    do: value |> Map.from_struct() |> portable() |> Map.put(:struct, inspect(module))

  defp portable(value) when is_map(value),
    do: Map.new(value, fn {key, item} -> {key, portable(item)} end)

  defp portable(value) when is_tuple(value), do: value |> Tuple.to_list() |> Enum.map(&portable/1)
  defp portable(value) when is_list(value), do: Enum.map(value, &portable/1)
  defp portable(value), do: value
end
