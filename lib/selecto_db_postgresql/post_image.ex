defmodule SelectoDBPostgreSQL.PostImage do
  @moduledoc false

  # TW-09 post-image check for guarded INSERT and UPDATE commands
  # (`metadata.post_image`, set by SelectoUpdato: the engine tenant and the
  # trusted and host scope, plus the full guard for an insert).
  #
  # Before the statement the relation is locked ROW EXCLUSIVE and resolved
  # to its oid, schema and name, and to the oids its rows may live in (the
  # relation and its partitions): every later statement names it by that
  # schema-qualified name and keeps only rows of those oids (items 51, 51a).
  #
  # The statement returns the written rows' physical identity, (tableoid,
  # ctid), as tagged RETURNING columns. A ctid names exactly one row version
  # of one relation, whatever the key's type, collation or text image under
  # the session's settings (items 34, 40, 43, 47). After the statement
  # completes, when every non-deferred BEFORE and AFTER row trigger, rule and
  # generated column has run, one tagged SELECT inside the same
  # transaction re-reads those rows under the scope predicate and reports
  # what could defeat the check:
  #
  # - `selecto_post_image_matched`: written rows still inside the scope;
  # - `selecto_post_image_rows`: rows carrying the returned identities (a
  #   row an AFTER trigger updated again has a new ctid and refuses);
  # - `selecto_post_image_blocked`: the relation is not a plain or
  #   partitioned table (a view), or it has enabled non-internal
  #   statement-level or deferrable triggers for the event (they act on
  #   other rows or at commit, after this check), or rewrite rules for it.
  #
  # Unless the relation is unblocked and every written row matches, the
  # transaction rolls back with a typed refusal. Row-level triggers
  # (updated_at, audit) keep working; triggers that change OTHER rows are
  # outside the scope guarantee.

  alias Selecto.Write.{Command, Error}

  # Any enabled non-internal deferrable trigger in the database: it fires at
  # COMMIT, after this check, and can sit on any table a row trigger writes to.
  @deferrable "SELECT pg_catalog.count(*) FROM pg_catalog.pg_trigger t " <>
                "WHERE t.tgdeferrable AND NOT t.tgisinternal AND t.tgenabled OPERATOR(pg_catalog.<>) 'D'"

  @key_alias "selecto_post_image_key_"

  @doc "The command to run: its RETURNING extended by the tagged identity columns."
  def prepare(%Command{metadata: %{post_image: %{key: [_ | _]}}, operation: operation} = command)
      when operation in [:insert, :update, :upsert],
      do: {:post_image, command}

  def prepare(command), do: {:plain, command}

  @doc """
  Locks the written relation ROW EXCLUSIVE and resolves it, before the
  statement: its oid, its schema-qualified name and the oids of the
  relation and its partitions.
  """
  def resolve(%Command{} = command, lock, execute) do
    sql = """
    SELECT c.oid AS selecto_guard_relation_oid,
           n.nspname::pg_catalog.text AS selecto_guard_relation_schema,
           c.relname::pg_catalog.text AS selecto_guard_relation_name,
           ARRAY(SELECT t.relid FROM pg_catalog.pg_partition_tree(c.oid) t
                 UNION SELECT c.oid) AS selecto_guard_relation_oids
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON n.oid OPERATOR(pg_catalog.=) c.relnamespace
    WHERE c.oid OPERATOR(pg_catalog.=) pg_catalog.to_regclass($1::pg_catalog.text)
    """

    with {:ok, _} <-
           lock_result(
             lock.("LOCK TABLE #{quote_relation(command.relation)} IN ROW EXCLUSIVE MODE", [])
           ),
         {:ok, %{rows: [[oid, schema, name, oids]]}} when is_integer(oid) <-
           execute.(sql, [quote_relation(command.relation)]) do
      {:ok,
       %{
         oid: oid,
         relation: quote_identifier(schema) <> "." <> quote_identifier(name),
         oids: Enum.uniq([oid | List.wrap(oids)])
       }}
    else
      _ -> refused(:query_rule_not_evaluable, "cannot lock and resolve the written relation")
    end
  end

  defp lock_result({:ok, _} = ok), do: ok
  defp lock_result(_other), do: :error

  @doc """
  The statement text that returns the written rows' (tableoid, ctid) as
  tagged columns.
  """
  def returning(text, %Command{returning: returning}) do
    keys =
      "tableoid AS #{@key_alias}1, CAST(ctid AS pg_catalog.text) AS #{@key_alias}2"

    if returning == :none, do: text <> " RETURNING " <> keys, else: text <> ", " <> keys
  end

  @doc """
  The statement result without the tagged identity columns, and the written
  rows' identities.
  """
  def split_result(%{columns: columns, rows: rows} = result, %Command{})
      when is_list(columns) and is_list(rows) do
    tagged =
      columns
      |> Enum.with_index()
      |> Enum.filter(fn {column, _index} -> String.starts_with?(column, @key_alias) end)
      |> Enum.map(&elem(&1, 1))

    case tagged do
      [oid_index, ctid_index] ->
        keys = Enum.map(rows, fn row -> {Enum.at(row, oid_index), Enum.at(row, ctid_index)} end)

        keep = fn list ->
          list
          |> Enum.with_index()
          |> Enum.reject(&(elem(&1, 1) in tagged))
          |> Enum.map(&elem(&1, 0))
        end

        # A statement that returned only the tagged columns reports no rows,
        # as it would have without RETURNING.
        result =
          case keep.(columns) do
            [] -> %{result | columns: [], rows: []}
            kept -> %{result | columns: kept, rows: Enum.map(rows, keep)}
          end

        {result, keys}

      _other ->
        {result, :unknown}
    end
  end

  def split_result(result, _command), do: {result, :unknown}

  @doc """
  Re-reads the written rows. `compile_predicate` compiles the portable scope
  predicate with placeholders after the given offset; `execute` runs SQL on
  the transaction's connection.
  """
  def verify(_command, _target, :unknown, _compile_predicate, _execute),
    do: refused(:query_rule_not_evaluable, "cannot identify the written rows")

  def verify(%Command{} = command, target, keys, compile_predicate, execute) do
    %{predicate: predicate} = command.metadata.post_image
    affected = length(keys)

    if Enum.all?(keys, fn {oid, ctid} -> is_integer(oid) and is_binary(ctid) end) do
      with {:ok, scope} <- compile_predicate.(untyped_text(predicate), 3) do
        rows = """
        FROM #{target.relation} AS selecto_target
        WHERE selecto_target.tableoid OPERATOR(pg_catalog.=) ANY ($1::pg_catalog.oid[])
          AND EXISTS (
            SELECT 1
            FROM ROWS FROM (pg_catalog.unnest($2::pg_catalog.oid[]),
                            pg_catalog.unnest(CAST($3::pg_catalog.text[] AS pg_catalog.tid[])))
              AS selecto_key(o, t)
            WHERE selecto_key.o OPERATOR(pg_catalog.=) selecto_target.tableoid
              AND selecto_key.t OPERATOR(pg_catalog.=) selecto_target.ctid)
        """

        oid_param = 4 + length(scope.params)

        sql = """
        SELECT
          (SELECT pg_catalog.count(*) #{rows} AND (#{scope.text})) AS selecto_post_image_matched,
          (SELECT pg_catalog.count(*) #{rows}) AS selecto_post_image_rows,
          #{blocked_sql(command)} AS selecto_post_image_blocked,
          (#{@deferrable}) AS selecto_post_image_deferrable
        FROM pg_catalog.pg_class c WHERE c.oid OPERATOR(pg_catalog.=) $#{oid_param}::pg_catalog.oid
        """

        params =
          [target.oids, Enum.map(keys, &elem(&1, 0)), Enum.map(keys, &elem(&1, 1))] ++
            scope.params ++ [target.oid]

        case execute.(sql, params) do
          {:ok, %{rows: [[matched, rows, blocked, deferred]]}} ->
            cond do
              blocked != 0 or deferred != 0 ->
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
            refused(
              :query_rule_not_evaluable,
              "the relation cannot prove the written rows' scope"
            )

          {:error, _reason} = error ->
            error
        end
      end
    else
      refused(:query_rule_not_evaluable, "cannot identify the written rows")
    end
  end

  # Every term an int8 count, summed with pg_catalog's int8 operator; the
  # trigger type is cast to int4 for pg_catalog's int4 & (R48).
  defp blocked_sql(command) do
    [
      "CASE WHEN c.relkind OPERATOR(pg_catalog.=) ANY (ARRAY['r', 'p']::pg_catalog.\"char\"[]) THEN 0 ELSE 1 END",
      # Statement-level triggers and rewrite rules for the event on the
      # relation or any of its partitions.
      """
      SELECT pg_catalog.count(*) FROM pg_catalog.pg_trigger t
       WHERE (t.tgrelid OPERATOR(pg_catalog.=) c.oid
              OR t.tgrelid OPERATOR(pg_catalog.=) ANY (SELECT relid FROM pg_catalog.pg_partition_tree(c.oid)))
         AND NOT t.tgisinternal AND t.tgenabled OPERATOR(pg_catalog.<>) 'D'
         AND #{statement_trigger(command)}
      """,
      """
      SELECT pg_catalog.count(*) FROM pg_catalog.pg_rewrite r
       WHERE (r.ev_class OPERATOR(pg_catalog.=) c.oid
              OR r.ev_class OPERATOR(pg_catalog.=) ANY (SELECT relid FROM pg_catalog.pg_partition_tree(c.oid)))
         AND r.rulename OPERATOR(pg_catalog.<>) '_RETURN'
         AND r.ev_type OPERATOR(pg_catalog.=) ANY (ARRAY[#{rule_events(command.operation)}]::pg_catalog."char"[])
      """,
      # A deferrable constraint trigger fires at commit, after this check,
      # and can sit on any table a row trigger writes to (an audit table)
      # or on a leaf partition: any in the database refuses, by the column
      # beside this one and its count. This runs after the DML (a trigger
      # committed while the write waited on a row lock is seen), which needs
      # a fresh statement snapshot: outside READ COMMITTED the check is
      # refused.
      "CASE WHEN pg_catalog.current_setting('transaction_isolation') OPERATOR(pg_catalog.=) 'read committed' THEN 0 ELSE 1 END",
      # Inheritance (non-partition) children: the parent's rows are not the
      # only ones the relation's name reads.
      """
      SELECT pg_catalog.count(*) FROM pg_catalog.pg_inherits i
       WHERE i.inhparent OPERATOR(pg_catalog.=) c.oid AND c.relkind OPERATOR(pg_catalog.=) 'r'
      """
    ]
    |> int8_sum()
  end

  # pg_trigger.tgtype bits: a statement-level trigger for the event.
  defp statement_trigger(command),
    do:
      "(CAST(t.tgtype AS pg_catalog.int4) OPERATOR(pg_catalog.&) #{event_bits(command.operation)}) " <>
        "OPERATOR(pg_catalog.<>) 0 AND " <>
        "(CAST(t.tgtype AS pg_catalog.int4) OPERATOR(pg_catalog.&) 1) OPERATOR(pg_catalog.=) 0"

  defp int8_sum(terms) do
    "(" <>
      Enum.map_join(
        terms,
        " OPERATOR(pg_catalog.+) ",
        &"CAST((#{String.trim(&1)}) AS pg_catalog.int8)"
      ) <>
      ")"
  end

  # Text literals of the scope compare as untyped SQL literals, as the host's
  # own read compares them (WriteCompiler.compile_value/3 `:untyped_text`).
  defp untyped_text({:literal, value}) when is_binary(value), do: {:untyped_text, value}

  defp untyped_text(tuple) when is_tuple(tuple),
    do: tuple |> Tuple.to_list() |> Enum.map(&untyped_text/1) |> List.to_tuple()

  defp untyped_text(list) when is_list(list), do: Enum.map(list, &untyped_text/1)
  defp untyped_text(value), do: value

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

  def lock_delete(%Command{} = command, execute, resolve_execute) do
    with {:ok, target} <- resolve(command, execute, resolve_execute),
         :ok <- check_delete(command, target, execute) do
      {:ok, target}
    end
  end

  # Checked by the oid resolved under the lock, never by the relation's
  # name, which a row trigger's search_path switch could redirect (item 51a).
  def check_delete(%Command{} = command, %{oid: oid}, execute) do
    sql = """
    WITH RECURSIVE tree(oid) AS (
      SELECT $1::pg_catalog.oid
      UNION
      SELECT i.inhrelid FROM pg_catalog.pg_inherits i
      JOIN tree ON i.inhparent OPERATOR(pg_catalog.=) tree.oid
    )
    SELECT
      (SELECT pg_catalog.count(*) FROM pg_catalog.pg_class c
       WHERE c.oid OPERATOR(pg_catalog.=) $1::pg_catalog.oid AND c.relkind OPERATOR(pg_catalog.=) ANY (ARRAY['r', 'p']::pg_catalog."char"[]))
        AS selecto_delete_relation,
      (SELECT pg_catalog.count(*) FROM pg_catalog.pg_trigger t
       WHERE t.tgrelid OPERATOR(pg_catalog.=) ANY (SELECT oid FROM tree)
         AND NOT t.tgisinternal AND t.tgenabled OPERATOR(pg_catalog.<>) 'D'
         AND #{statement_trigger(command)})
        OPERATOR(pg_catalog.+) (SELECT pg_catalog.count(*) FROM pg_catalog.pg_rewrite r
         WHERE r.ev_class OPERATOR(pg_catalog.=) ANY (SELECT oid FROM tree)
           AND r.rulename OPERATOR(pg_catalog.<>) '_RETURN' AND r.ev_type OPERATOR(pg_catalog.=) ANY (ARRAY[#{rule_events(command.operation)}]::pg_catalog."char"[]))
        AS selecto_delete_blocked,
      -- A deferrable trigger fires at commit, after this check, and can sit
      -- on any table a row trigger writes to: any in the database refuses,
      -- as for inserts, updates and upserts (w3exrev17).
      (#{@deferrable}) AS selecto_delete_deferrable
    """

    case execute.(sql, [oid]) do
      {:ok, %{rows: [[relation, blocked, deferred]]}} ->
        if relation == 1 and blocked == 0 and deferred == 0,
          do: :ok,
          else:
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
