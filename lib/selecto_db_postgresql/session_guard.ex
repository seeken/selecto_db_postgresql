defmodule SelectoDBPostgreSQL.SessionGuard.Deferrable do
  @moduledoc false

  # The deferrable triggers that can run user code at COMMIT (R23, R32):
  # every enabled deferrable trigger except
  # (1) a foreign-key trigger whose function is a pg_catalog RI_FKey_*
  #     function (deferred foreign keys keep working), and
  # (2) a unique or exclusion recheck (pg_catalog.unique_key_recheck) on an
  #     index with no expressions or predicate whose operator classes, their
  #     families' operators and support functions, the exclusion operators
  #     and their functions, and the collations are all pg_catalog's.
  # A DEFERRABLE EXCLUDE whose opclass calls plpgsql counts.

  @catalog "CAST(11 AS pg_catalog.oid)"

  @count_sql "SELECT pg_catalog.count(*) FROM pg_catalog.pg_trigger t " <>
               "JOIN pg_catalog.pg_proc f ON f.oid OPERATOR(pg_catalog.=) t.tgfoid " <>
               "WHERE t.tgdeferrable AND t.tgenabled OPERATOR(pg_catalog.<>) 'D' " <>
               "AND NOT (t.tgisinternal AND f.pronamespace OPERATOR(pg_catalog.=) #{@catalog} AND (" <>
               "pg_catalog.left(CAST(f.proname AS pg_catalog.text), 8) OPERATOR(pg_catalog.=) 'RI_FKey_' " <>
               "OR (f.proname OPERATOR(pg_catalog.=) 'unique_key_recheck' AND EXISTS (" <>
               "SELECT 1 FROM pg_catalog.pg_constraint c " <>
               "JOIN pg_catalog.pg_index i ON i.indexrelid OPERATOR(pg_catalog.=) c.conindid " <>
               "WHERE c.oid OPERATOR(pg_catalog.=) t.tgconstraint " <>
               "AND i.indexprs IS NULL AND i.indpred IS NULL " <>
               "AND NOT EXISTS (SELECT 1 FROM pg_catalog.unnest(CAST(i.indclass AS pg_catalog.oid[])) AS k(o) " <>
               "JOIN pg_catalog.pg_opclass x ON x.oid OPERATOR(pg_catalog.=) k.o " <>
               "LEFT JOIN pg_catalog.pg_amproc ap ON ap.amprocfamily OPERATOR(pg_catalog.=) x.opcfamily " <>
               "LEFT JOIN pg_catalog.pg_proc pp ON pp.oid OPERATOR(pg_catalog.=) ap.amproc " <>
               "LEFT JOIN pg_catalog.pg_amop ao ON ao.amopfamily OPERATOR(pg_catalog.=) x.opcfamily " <>
               "LEFT JOIN pg_catalog.pg_operator op ON op.oid OPERATOR(pg_catalog.=) ao.amopopr " <>
               "LEFT JOIN pg_catalog.pg_proc opf ON opf.oid OPERATOR(pg_catalog.=) op.oprcode " <>
               "WHERE x.opcnamespace OPERATOR(pg_catalog.<>) #{@catalog} " <>
               "OR pp.pronamespace OPERATOR(pg_catalog.<>) #{@catalog} " <>
               "OR op.oprnamespace OPERATOR(pg_catalog.<>) #{@catalog} " <>
               "OR opf.pronamespace OPERATOR(pg_catalog.<>) #{@catalog}) " <>
               "AND NOT EXISTS (SELECT 1 FROM pg_catalog.unnest(c.conexclop) AS e(o) " <>
               "JOIN pg_catalog.pg_operator op ON op.oid OPERATOR(pg_catalog.=) e.o " <>
               "LEFT JOIN pg_catalog.pg_proc opf ON opf.oid OPERATOR(pg_catalog.=) op.oprcode " <>
               "WHERE op.oprnamespace OPERATOR(pg_catalog.<>) #{@catalog} " <>
               "OR opf.pronamespace OPERATOR(pg_catalog.<>) #{@catalog}) " <>
               "AND NOT EXISTS (SELECT 1 FROM pg_catalog.unnest(CAST(i.indcollation AS pg_catalog.oid[])) AS l(o) " <>
               "JOIN pg_catalog.pg_collation co ON co.oid OPERATOR(pg_catalog.=) l.o " <>
               "WHERE co.collnamespace OPERATOR(pg_catalog.<>) #{@catalog})))))"

  @doc "The count statement (no parameters); every name pg_catalog-qualified."
  def count_sql, do: @count_sql
end

defmodule SelectoDBPostgreSQL.SessionGuard do
  @moduledoc false

  # Session state a statement can change for the statements after it
  # (brief items 54, 54a, 54b, 56, 56b, 56c, 59, 61).
  #
  # A row trigger, or a function a domain view calls, can run
  # set_config('search_path' | 'TimeZone' | 'DateStyle' | ..., ..., true) or
  # create a TEMP relation or type. PostgreSQL searches pg_temp first, for
  # relations and for type names, whatever search_path says, and a session
  # temp relation survives COMMIT. Later SQL on the connection then resolves
  # names, operators and literals differently.
  #
  # The snapshot is one pg_catalog-qualified statement that a changed
  # search_path cannot redirect: the settings that name resolution and
  # literal parsing depend on, and the session temp namespace's relations
  # (oid, xmin, name), rewrite rules (views) and types. It is compared
  # client-side. Every Selecto write takes it before its first statement and
  # again as the FIRST statement after every DML; every Selecto read takes it
  # before and after the statement (and after every stream batch). Any
  # change refuses with `session_state_changed`: the write's transaction or
  # savepoint rolls back (which also reverts set_config), a read in a
  # host-owned transaction rolls back to its own savepoint, and a read in
  # autocommit restores the settings it saw before.
  #
  # Selecto keeps no session marker or GUC of its own to prove ownership
  # (item 57), so there is nothing a trigger can forge.
  #
  # The shadow check refuses SQL whose names a temp relation or type would
  # capture: a relation of the session temp schema named like any identifier
  # of the statement while a same-named relation exists in a schema of the
  # search path (current_schemas(false)), and a temp type (not a temp
  # table's row type or its array) named like an identifier of the statement
  # or a type the generated SQL casts to. A temp relation with no namesake is
  # the host's own (a domain over a temp table keeps working), as is an
  # unrelated host temp table (item 19).

  alias Selecto.Write.Error

  @settings ["search_path", "TimeZone", "DateStyle", "IntervalStyle", "extra_float_digits"]

  # Type names the generated SQL casts to or literals parse through.
  @generated_types ~w(text varchar bpchar char date time timetz timestamp timestamptz uuid
                      jsonb json jsonpath regconfig regclass oid tid numeric int2 int4 int8
                      float4 float8 bool interval bytea inet cidr name)

  @temp "pg_catalog.pg_my_temp_schema()"

  # search_path is compared on its own (R30: Selecto pins it around its
  # statements), every other setting and the temp catalog in one string.
  @snapshot_columns "pg_catalog.concat_ws(' | ', " <>
                      Enum.map_join(
                        @settings -- ["search_path"],
                        ", ",
                        &"pg_catalog.current_setting('#{&1}')"
                      ) <>
                      ", (SELECT pg_catalog.string_agg(pg_catalog.concat_ws(':', CAST(c.oid AS pg_catalog.text), " <>
                      "CAST(c.xmin AS pg_catalog.text), CAST(c.relname AS pg_catalog.text)), ',' ORDER BY c.oid) " <>
                      "FROM pg_catalog.pg_class c WHERE c.relnamespace OPERATOR(pg_catalog.=) #{@temp}), " <>
                      "(SELECT pg_catalog.string_agg(pg_catalog.concat_ws(':', CAST(r.oid AS pg_catalog.text), " <>
                      "CAST(r.xmin AS pg_catalog.text)), ',' ORDER BY r.oid) FROM pg_catalog.pg_rewrite r " <>
                      "JOIN pg_catalog.pg_class c ON c.oid OPERATOR(pg_catalog.=) r.ev_class " <>
                      "WHERE c.relnamespace OPERATOR(pg_catalog.=) #{@temp}), " <>
                      "(SELECT pg_catalog.string_agg(pg_catalog.concat_ws(':', CAST(t.oid AS pg_catalog.text), " <>
                      "CAST(t.xmin AS pg_catalog.text), CAST(t.typname AS pg_catalog.text)), ',' ORDER BY t.oid) " <>
                      "FROM pg_catalog.pg_type t WHERE t.typnamespace OPERATOR(pg_catalog.=) #{@temp}), 'end') " <>
                      "AS selecto_guard_session, " <>
                      "(" <>
                      SelectoDBPostgreSQL.SessionGuard.Deferrable.count_sql() <>
                      ") AS selecto_guard_deferrable, " <>
                      Enum.map_join(Enum.with_index(@settings), ", ", fn {name, index} ->
                        "pg_catalog.current_setting('#{name}') AS selecto_guard_setting_#{index}"
                      end)

  @snapshot_sql "SELECT " <> @snapshot_columns

  @shadow_sql "SELECT (SELECT pg_catalog.count(*) FROM pg_catalog.pg_class c " <>
                "WHERE c.relnamespace OPERATOR(pg_catalog.=) #{@temp} " <>
                "AND CAST(c.relname AS pg_catalog.text) OPERATOR(pg_catalog.=) ANY ($1::pg_catalog.text[]) " <>
                "AND EXISTS (SELECT 1 FROM pg_catalog.pg_class o JOIN pg_catalog.pg_namespace s " <>
                "ON s.oid OPERATOR(pg_catalog.=) o.relnamespace " <>
                "WHERE o.relname OPERATOR(pg_catalog.=) c.relname " <>
                "AND o.relnamespace OPERATOR(pg_catalog.<>) c.relnamespace " <>
                "AND s.nspname OPERATOR(pg_catalog.=) ANY (pg_catalog.current_schemas(false)))) " <>
                "+ (SELECT pg_catalog.count(*) FROM pg_catalog.pg_type t " <>
                "WHERE t.typnamespace OPERATOR(pg_catalog.=) #{@temp} " <>
                "AND t.typrelid OPERATOR(pg_catalog.=) 0 " <>
                "AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_type e WHERE e.oid OPERATOR(pg_catalog.=) t.typelem " <>
                "AND e.typrelid OPERATOR(pg_catalog.<>) 0) " <>
                "AND CAST(t.typname AS pg_catalog.text) OPERATOR(pg_catalog.=) ANY ($2::pg_catalog.text[])) " <>
                "AS selecto_guard_shadowed, " <> @snapshot_columns

  @doc "The snapshot statement (no parameters)."
  def snapshot_sql, do: @snapshot_sql

  @doc """
  Refuses `sql` (one statement text or several) when a temp relation or type
  would capture one of its names, and returns the session snapshot taken in
  the same statement.
  """
  def check(execute, sql, relations \\ []) do
    case execute.(@shadow_sql, [Enum.uniq(relations ++ relation_tokens(sql)), tokens(sql)]) do
      {:ok, %{rows: [[0, snapshot, deferrable | settings]]}} when is_binary(snapshot) ->
        {:ok,
         %{snapshot: snapshot, deferrable: deferrable, settings: Enum.zip(@settings, settings)}}

      {:ok, %{rows: [[shadowed | _]]}} when is_integer(shadowed) and shadowed > 0 ->
        refused(
          :query_rule_not_evaluable,
          :temporary_shadow,
          "a temporary relation or type of the session shadows a name the statement uses"
        )

      {:ok, _other} ->
        unavailable()

      {:error, _reason} ->
        unavailable()
    end
  end

  @doc "Takes the session snapshot."
  def snapshot(execute) do
    case execute.(@snapshot_sql, []) do
      {:ok, %{rows: [[snapshot, deferrable | settings]]}} when is_binary(snapshot) ->
        {:ok,
         %{snapshot: snapshot, deferrable: deferrable, settings: Enum.zip(@settings, settings)}}

      _other ->
        unavailable()
    end
  end

  @doc """
  Compares the session with `before`. Run as the FIRST statement after a
  DML or a read, before any other SQL.
  """
  def compare(execute, %{snapshot: before} = baseline, opts \\ []) do
    case snapshot(execute) do
      {:ok, %{snapshot: ^before} = current} ->
        if search_path(current) == expected_path(baseline, opts),
          do: deferrable_free(current, opts),
          else: changed()

      {:ok, _changed} ->
        changed()

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Whether `current` (a check/2 or snapshot/1 state) matches `baseline`:
  the same snapshot and the expected search_path (`search_path:` in
  `opts`, the pinned value while Selecto's pin is in place, else the
  baseline's own).
  """
  def same?(current, baseline, opts \\ []) do
    current.snapshot == baseline.snapshot and
      search_path(current) == expected_path(baseline, opts)
  end

  @doc "The search_path of a snapshot state."
  def search_path(%{settings: settings}), do: :proplists.get_value("search_path", settings, nil)

  defp expected_path(baseline, opts),
    do: Keyword.get(opts, :search_path) || search_path(baseline)

  @doc """
  R30: pins search_path to `path` for the rest of the current transaction
  (set_config(..., true)); run only inside Selecto's own transaction or
  savepoint, after the snapshot it is compared with. Returns the value as
  the server normalised it.
  """
  def pin(execute, path) do
    case execute.(
           "SELECT pg_catalog.set_config('search_path', $1::pg_catalog.text, true) " <>
             "AS selecto_guard_pin",
           [path]
         ) do
      {:ok, %{rows: [[value]]}} when is_binary(value) -> {:ok, value}
      _other -> unavailable()
    end
  end

  @doc """
  R30: restores the host's search_path `previous` (set_config(..., true)) in
  the same expression that checks the pin `pinned` is still in place, so a
  statement that switched it in between refuses.
  """
  def unpin(execute, pinned, previous) do
    case execute.(
           "SELECT CASE WHEN pg_catalog.current_setting('search_path') OPERATOR(pg_catalog.=) " <>
             "$1::pg_catalog.text THEN pg_catalog.set_config('search_path', $2::pg_catalog.text, true) " <>
             "END AS selecto_guard_unpin",
           [pinned, previous]
         ) do
      {:ok, %{rows: [[value]]}} when is_binary(value) -> :ok
      {:ok, %{rows: [[nil]]}} -> changed()
      _other -> unavailable()
    end
  end

  @doc """
  R23, R32 (rulings): a deferrable trigger fires at COMMIT, after every
  check of the write, so no PostgreSQL write runs while one exists that can
  run user code (`SelectoDBPostgreSQL.SessionGuard.Deferrable`). Writes
  check before their DML and in the first post-DML statement
  (`writes: true`).
  """
  def deferrable_free(state, opts \\ [])

  def deferrable_free(%{deferrable: count}, opts) when is_integer(count) and count > 0 do
    if Keyword.get(opts, :writes) do
      refused(
        :query_rule_not_evaluable,
        :deferred_triggers,
        "writes are refused while a deferrable trigger exists: it fires at COMMIT, after every check"
      )
    else
      :ok
    end
  end

  def deferrable_free(_state, _opts), do: :ok

  @doc """
  Restores the settings of `before` for the session (autocommit reads, where
  no transaction state is left to roll back), best effort.
  """
  def restore(execute, %{settings: settings}) do
    Enum.each(settings, fn {name, value} ->
      _ =
        execute.(
          "SELECT pg_catalog.set_config($1::pg_catalog.text, $2::pg_catalog.text, false)",
          [name, value]
        )
    end)

    :ok
  end

  @doc "The refusal for a changed session."
  def changed,
    do:
      refused(
        :query_rule_violation,
        :session_state_changed,
        "the statement changed the session's settings or temporary relations"
      )

  @doc false
  # Every identifier-like token of the statement: quoted identifiers as
  # written, unquoted words as written and folded to lower case, plus the
  # type names the generated SQL casts to. Over-inclusion can only refuse.
  def tokens(sql) when is_list(sql), do: sql |> Enum.flat_map(&tokens/1) |> Enum.uniq()

  def tokens(sql) when is_binary(sql) do
    quoted =
      ~r/"((?:[^"]|"")*)"/
      |> Regex.scan(sql, capture: :all_but_first)
      |> Enum.map(fn [name] -> String.replace(name, "\"\"", "\"") end)

    words =
      ~r/[A-Za-z_\x{80}-\x{10FFFF}][A-Za-z0-9_$\x{80}-\x{10FFFF}]*/u
      |> Regex.scan(sql)
      |> Enum.flat_map(fn [word] -> [word, String.downcase(word)] end)

    Enum.uniq(quoted ++ words ++ @generated_types)
  end

  def tokens(sql), do: sql |> IO.iodata_to_binary() |> tokens()

  @doc false
  # The relation names a statement names after FROM, JOIN, INTO, UPDATE or
  # TABLE (parenthesised joins included), as written: the unqualified name
  # or the last part of a qualified one, quoted identifiers unescaped and
  # bare words folded. The temp-relation shadow check matches only these
  # (R28): a temp table named like a column refuses nothing.
  def relation_tokens(sql) when is_list(sql),
    do: sql |> Enum.flat_map(&relation_tokens/1) |> Enum.uniq()

  def relation_tokens(sql) when is_binary(sql) do
    ident = ~s/(?:"(?:[^"]|"")*"|[A-Za-z_][A-Za-z0-9_$]*)/

    ~r/\b(?:from|join|into|update|table|only)\s*(?:\(\s*)*(#{ident}(?:\s*\.\s*#{ident})*)/i
    |> Regex.scan(sql, capture: :all_but_first)
    |> Enum.flat_map(fn [name] ->
      last = ~r/#{ident}/ |> Regex.scan(name) |> List.last() |> hd()

      if String.starts_with?(last, "\""),
        do: [last |> String.slice(1..-2//1) |> String.replace("\"\"", "\"")],
        else: [last, String.downcase(last)]
    end)
    |> Enum.uniq()
  end

  def relation_tokens(sql), do: sql |> IO.iodata_to_binary() |> relation_tokens()

  defp unavailable,
    do:
      refused(
        :query_rule_not_evaluable,
        :session_state_unavailable,
        "cannot read the session state"
      )

  defp refused(code, reason, message),
    do:
      {:error,
       Error.new(:query_enforcement, "PostgreSQL " <> message,
         details: %{code: code, reason: reason, adapter: :postgresql}
       )}
end
