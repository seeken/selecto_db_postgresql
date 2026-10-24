defmodule SelectoDBPostgreSQL.PostImage do
  @moduledoc false

  # TW-09 post-image check for guarded INSERT and UPDATE commands
  # (`metadata.post_image`, set by SelectoUpdato: the engine tenant and the
  # trusted and host scope, plus the full guard for an insert).
  #
  # The statement returns the written rows' keys (tagged RETURNING columns).
  # After it completes, when every non-deferred BEFORE and AFTER row trigger,
  # rule and generated column has run, one tagged SELECT inside the same
  # transaction re-reads those rows by key under the scope predicate and
  # reports what could defeat the check:
  #
  # - `selecto_post_image_matched`: written rows still inside the scope;
  # - `selecto_post_image_rows`: rows carrying the returned keys;
  # - `selecto_post_image_blocked`: the relation is not a plain or
  #   partitioned table (a view), or it has enabled non-internal
  #   statement-level or deferrable triggers for the event (they act on
  #   other rows or at commit, after this check), or rewrite rules for it;
  # - `selecto_post_image_keyed`: the key is exactly an immediate, valid,
  #   non-partial, non-expression unique index over NOT NULL columns.
  #
  # Unless the relation is unblocked and keyed and every written row matches,
  # the transaction rolls back with a typed refusal. Row-level triggers
  # (updated_at, audit) keep working; triggers that change OTHER rows are
  # outside the scope guarantee.

  alias Selecto.Write.{Command, Error}

  @key_alias "selecto_post_image_key_"

  @doc "The command to run: its RETURNING extended by the tagged key columns."
  def prepare(%Command{metadata: %{post_image: %{key: [_ | _]}}, operation: operation} = command)
      when operation in [:insert, :update, :upsert],
      do: {:post_image, command}

  def prepare(command), do: {:plain, command}

  @doc """
  The statement text that returns the written rows' keys: unchanged when its
  RETURNING already names every key column (`:all`, or a list holding them),
  otherwise extended by tagged key columns.
  """
  def returning(text, %Command{metadata: %{post_image: %{key: key}}, returning: returning}) do
    if returns_keys?(returning, key) do
      text
    else
      keys =
        key
        |> Enum.with_index(1)
        |> Enum.map_join(", ", fn {column, index} ->
          "#{quote_identifier(column)} AS #{@key_alias}#{index}"
        end)

      if returning == :none, do: text <> " RETURNING " <> keys, else: text <> ", " <> keys
    end
  end

  defp returns_keys?(:all, _key), do: true

  defp returns_keys?(fields, key) when is_list(fields) do
    names = Enum.map(fields, &to_string/1)
    Enum.all?(key, &(&1 in names))
  end

  defp returns_keys?(_returning, _key), do: false

  @doc """
  The statement result without the tagged key columns, and the written rows'
  keys (from the tags, or from the returned key columns).
  """
  def split_result(%{columns: columns, rows: rows} = result, %Command{
        metadata: %{post_image: %{key: key}}
      })
      when is_list(columns) do
    tagged =
      columns
      |> Enum.with_index()
      |> Enum.filter(fn {column, _index} -> String.starts_with?(column, @key_alias) end)
      |> Enum.map(&elem(&1, 1))

    if tagged == [] do
      indexes = Enum.map(key, fn column -> Enum.find_index(columns, &(&1 == column)) end)

      if Enum.any?(indexes, &is_nil/1),
        do: {result, :unknown},
        else: {result, Enum.map(rows, fn row -> Enum.map(indexes, &Enum.at(row, &1)) end)}
    else
      keys = Enum.map(rows, fn row -> Enum.map(tagged, &Enum.at(row, &1)) end)

      keep = fn list ->
        list
        |> Enum.with_index()
        |> Enum.reject(&(elem(&1, 1) in tagged))
        |> Enum.map(&elem(&1, 0))
      end

      # A statement that returned only the tagged keys reports no rows, as it
      # would have without RETURNING.
      result =
        case keep.(columns) do
          [] -> %{result | columns: [], rows: []}
          kept -> %{result | columns: kept, rows: Enum.map(rows, keep)}
        end

      {result, keys}
    end
  end

  def split_result(result, _command), do: {result, :unknown}

  @doc """
  Re-reads the written rows. `compile_predicate` compiles the portable scope
  predicate with placeholders after the given offset; `execute` runs SQL on
  the transaction's connection.
  """
  def verify(_command, :unknown, _compile_predicate, _execute),
    do: refused(:query_rule_not_evaluable, "cannot identify the written rows")

  def verify(%Command{} = command, keys, compile_predicate, execute) do
    %{predicate: predicate, key: key} = command.metadata.post_image
    relation = quote_relation(command.relation)
    affected = length(keys)

    {key_match, key_params} = key_match(key, keys, 2)

    with {:ok, scope} <- compile_predicate.(untyped_text(predicate), 1 + length(key_params)) do
      key_names = Enum.sort(key)
      names_param = 1 + length(key_params) + length(scope.params) + 1

      sql = """
      SELECT
        (SELECT count(*) FROM #{relation} WHERE #{key_match} AND (#{scope.text})) AS selecto_post_image_matched,
        (SELECT count(*) FROM #{relation} WHERE #{key_match}) AS selecto_post_image_rows,
        (CASE WHEN c.relkind IN ('r', 'p') THEN 0 ELSE 1 END
          -- Statement-level triggers and rewrite rules for the event on the
          -- relation or any of its partitions.
          + (SELECT count(*) FROM pg_catalog.pg_trigger t
             WHERE (t.tgrelid = c.oid OR t.tgrelid IN (SELECT relid FROM pg_catalog.pg_partition_tree(c.oid)))
               AND NOT t.tgisinternal AND t.tgenabled <> 'D'
               AND (t.tgtype & #{event_bits(command.operation)}) <> 0
               AND (t.tgtype & 1) = 0)
          + (SELECT count(*) FROM pg_catalog.pg_rewrite r
             WHERE (r.ev_class = c.oid OR r.ev_class IN (SELECT relid FROM pg_catalog.pg_partition_tree(c.oid)))
               AND r.rulename <> '_RETURN'
               AND r.ev_type IN (#{rule_events(command.operation)}))
          -- A deferrable constraint trigger fires at commit, after this
          -- check, and can sit on any table a row trigger writes to (an
          -- audit table) or on a leaf partition: any in the database refuses.
          -- This runs after the DML (a trigger committed while the write
          -- waited on a row lock is seen), which needs a fresh statement
          -- snapshot: outside READ COMMITTED the check is refused.
          + (SELECT count(*) FROM pg_catalog.pg_trigger t
             WHERE t.tgdeferrable AND NOT t.tgisinternal AND t.tgenabled <> 'D')
          + (CASE WHEN pg_catalog.current_setting('transaction_isolation') = 'read committed'
                  THEN 0 ELSE 1 END)
          -- Inheritance (non-partition) children share the parent's key
          -- values, so the key does not name one row.
          + (SELECT count(*) FROM pg_catalog.pg_inherits i
             WHERE i.inhparent = c.oid AND c.relkind = 'r')) AS selecto_post_image_blocked,
        EXISTS (
          SELECT 1 FROM pg_catalog.pg_index i
          WHERE i.indrelid = c.oid AND i.indisunique AND i.indimmediate AND i.indisvalid
            AND i.indpred IS NULL AND i.indexprs IS NULL AND i.indnatts = i.indnkeyatts
            AND i.indnkeyatts = cardinality($#{names_param}::text[])
            AND (SELECT array_agg(a.attname::text ORDER BY a.attname::text)
                 FROM pg_catalog.pg_attribute a
                 WHERE a.attrelid = c.oid AND a.attnum = ANY (i.indkey::int2[]) AND a.attnotnull)
                = $#{names_param}::text[]) AS selecto_post_image_keyed
      FROM pg_catalog.pg_class c WHERE c.oid = pg_catalog.to_regclass($1::text)
      """

      params = [relation | key_params] ++ scope.params ++ [key_names]

      case execute.(sql, params) do
        {:ok, %{rows: [[matched, rows, blocked, keyed]]}} ->
          cond do
            blocked != 0 or keyed != true ->
              refused(
                :query_rule_not_evaluable,
                "the relation cannot prove the written rows' scope"
              )

            rows != affected or matched != affected ->
              refused(:query_rule_violation, "the written rows left the trusted write scope")

            true ->
              :ok
          end

        {:ok, _other} ->
          refused(:query_rule_not_evaluable, "the relation cannot prove the written rows' scope")

        {:error, _reason} = error ->
          error
      end
    end
  end

  # Text literals of the scope compare as untyped SQL literals, as the host's
  # own read compares them (WriteCompiler.compile_value/3 `:untyped_text`).
  defp untyped_text({:literal, value}) when is_binary(value), do: {:untyped_text, value}

  defp untyped_text(tuple) when is_tuple(tuple),
    do: tuple |> Tuple.to_list() |> Enum.map(&untyped_text/1) |> List.to_tuple()

  defp untyped_text(list) when is_list(list), do: Enum.map(list, &untyped_text/1)
  defp untyped_text(value), do: value

  # Single-column keys match `= ANY($n)`; composite keys a VALUES list.
  defp key_match([column], keys, first) do
    {"#{quote_identifier(column)} = ANY ($#{first})", [Enum.map(keys, &hd/1)]}
  end

  defp key_match(columns, [], _first) do
    {"(#{Enum.map_join(columns, ", ", &quote_identifier/1)}) IN (SELECT #{Enum.map_join(columns, ", ", fn _ -> "NULL" end)} WHERE false)",
     []}
  end

  defp key_match(columns, keys, first) do
    width = length(columns)

    values =
      keys
      |> Enum.with_index()
      |> Enum.map_join(", ", fn {_row, row_index} ->
        "(" <>
          Enum.map_join(0..(width - 1), ", ", fn column_index ->
            "$#{first + row_index * width + column_index}"
          end) <> ")"
      end)

    {"(#{Enum.map_join(columns, ", ", &quote_identifier/1)}) IN (VALUES #{values})",
     List.flatten(keys)}
  end

  # tgtype bits: INSERT 4, UPDATE 16 (an upsert can fire either).
  defp event_bits(:insert), do: 4
  defp event_bits(:update), do: 16
  defp event_bits(:upsert), do: 20
  defp event_bits(:delete), do: 8

  # pg_rewrite.ev_type: '2' UPDATE, '3' INSERT, '4' DELETE.
  defp rule_events(:insert), do: "'3'"
  defp rule_events(:update), do: "'2'"
  defp rule_events(:upsert), do: "'2', '3'"
  defp rule_events(:delete), do: "'4'"

  @doc """
  A bound-scope DELETE (`metadata.bound_scope`, set by SelectoUpdato) leaves
  no row to re-read, but a statement-level DELETE trigger or a DELETE rule
  acts on other rows (another tenant's) with the statement. Before the
  statement the relation is locked ROW EXCLUSIVE (a concurrent CREATE
  TRIGGER then waits, or deadlocks, rather than slipping in after the
  check) and checked; after it, checked again. Enabled non-internal
  statement-level DELETE triggers and DELETE rules on the relation or
  anywhere in its partition or inheritance tree, any enabled non-internal
  deferrable trigger in the database, and a relation that is not a plain or
  partitioned table, refuse the delete. Row-level triggers that
  modify other rows are outside the scope guarantee.
  """
  #
  # The same contract covers a bound-scope INSERT, UPDATE or UPSERT that
  # carries no post-image of its own (a graph descendant bound by its root
  # or an ancestor, w3exrev18 item 33a): statement-level triggers and rules
  # for its event are refused.
  def bound_delete?(%Command{operation: :delete, metadata: %{bound_scope: true}}), do: true

  def bound_delete?(%Command{operation: operation, metadata: %{bound_scope: true} = metadata})
      when operation in [:insert, :update, :upsert],
      do: not Map.has_key?(metadata, :post_image)

  def bound_delete?(_command), do: false

  def lock_delete(%Command{} = command, execute) do
    case execute.("LOCK TABLE #{quote_relation(command.relation)} IN ROW EXCLUSIVE MODE", []) do
      {:ok, _} -> check_delete(command, execute)
      _ -> refused(:query_rule_not_evaluable, "cannot lock the delete's relation")
    end
  end

  def check_delete(%Command{} = command, execute) do
    sql = """
    WITH RECURSIVE tree(oid) AS (
      SELECT pg_catalog.to_regclass($1::text)
      UNION
      SELECT i.inhrelid FROM pg_catalog.pg_inherits i JOIN tree ON i.inhparent = tree.oid
    )
    SELECT
      (SELECT count(*) FROM pg_catalog.pg_class c
       WHERE c.oid = pg_catalog.to_regclass($1::text) AND c.relkind IN ('r', 'p'))
        AS selecto_delete_relation,
      (SELECT count(*) FROM pg_catalog.pg_trigger t
       WHERE t.tgrelid IN (SELECT oid FROM tree)
         AND NOT t.tgisinternal AND t.tgenabled <> 'D'
         AND (t.tgtype & #{event_bits(command.operation)}) <> 0 AND (t.tgtype & 1) = 0)
      + (SELECT count(*) FROM pg_catalog.pg_rewrite r
         WHERE r.ev_class IN (SELECT oid FROM tree)
           AND r.rulename <> '_RETURN' AND r.ev_type IN (#{rule_events(command.operation)}))
      -- A deferrable trigger fires at commit, after this check, and can sit
      -- on any table a row trigger writes to: any in the database refuses,
      -- as for inserts, updates and upserts (w3exrev17).
      + (SELECT count(*) FROM pg_catalog.pg_trigger t
         WHERE t.tgdeferrable AND NOT t.tgisinternal AND t.tgenabled <> 'D')
        AS selecto_delete_blocked
    """

    case execute.(sql, [quote_relation(command.relation)]) do
      {:ok, %{rows: [[1, 0]]}} ->
        :ok

      {:ok, %{rows: [[_relation, _blocked]]}} ->
        refused(
          :query_rule_not_evaluable,
          "a bound-scope write cannot run under statement-level triggers, rules or deferrable triggers"
        )

      _ ->
        refused(:query_rule_not_evaluable, "cannot check the delete's relation")
    end
  end

  defp refused(code, message),
    do:
      {:error,
       Error.new(:query_enforcement, "PostgreSQL " <> message,
         details: %{code: code, adapter: :postgresql}
       )}

  defp quote_relation(relation),
    do: relation |> to_string() |> String.split(".") |> Enum.map_join(".", &quote_identifier/1)

  defp quote_identifier(identifier),
    do: "\"" <> String.replace(to_string(identifier), "\"", "\"\"") <> "\""
end
