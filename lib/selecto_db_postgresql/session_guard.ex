defmodule SelectoDBPostgreSQL.SessionGuard.Deferrable do
  @moduledoc false

  # The deferrable triggers that can run user code at COMMIT (R23, R32,
  # R32a, R37): every enabled deferrable trigger except
  # (1) a foreign-key trigger whose function is a pg_catalog RI_FKey_*
  #     function, whose equality operators (conpfeqop, conppeqop,
  #     conffeqop) and their functions are pg_catalog's, whose referenced
  #     unique index passes the index test below, and whose key columns on
  #     both sides have pg_catalog base types (or domains over them);
  # (2) a unique or exclusion recheck (pg_catalog.unique_key_recheck) whose
  #     index passes the index test and whose exclusion operators and their
  #     functions are pg_catalog's.
  # Index test: no expressions, no predicate, every collation 0 or
  # pg_catalog's, every operator class, its family's operators and support
  # functions pg_catalog's, and every key column of a pg_catalog base type.
  # A DEFERRABLE EXCLUDE whose opclass, expression, collation or key type is
  # user-defined counts, as does a constraint trigger.

  @catalog "CAST(11 AS pg_catalog.oid)"

  defp base_types(relation, attnums),
    do:
      "NOT EXISTS (SELECT 1 FROM pg_catalog.pg_attribute a " <>
        "JOIN pg_catalog.pg_type ty ON ty.oid OPERATOR(pg_catalog.=) a.atttypid " <>
        "LEFT JOIN pg_catalog.pg_type bt ON bt.oid OPERATOR(pg_catalog.=) ty.typbasetype " <>
        "WHERE a.attrelid OPERATOR(pg_catalog.=) #{relation} " <>
        "AND a.attnum OPERATOR(pg_catalog.=) ANY (#{attnums}) " <>
        "AND NOT ((ty.typtype OPERATOR(pg_catalog.=) 'b' AND ty.typnamespace OPERATOR(pg_catalog.=) #{@catalog}) " <>
        "OR (ty.typtype OPERATOR(pg_catalog.=) 'd' AND bt.typtype OPERATOR(pg_catalog.=) 'b' " <>
        "AND bt.typnamespace OPERATOR(pg_catalog.=) #{@catalog})))"

  defp catalog_operators(array),
    do:
      "NOT EXISTS (SELECT 1 FROM pg_catalog.unnest(#{array}) AS e(o) " <>
        "JOIN pg_catalog.pg_operator op ON op.oid OPERATOR(pg_catalog.=) e.o " <>
        "LEFT JOIN pg_catalog.pg_proc opf ON opf.oid OPERATOR(pg_catalog.=) op.oprcode " <>
        "WHERE op.oprnamespace OPERATOR(pg_catalog.<>) #{@catalog} " <>
        "OR opf.pronamespace OPERATOR(pg_catalog.<>) #{@catalog})"

  defp catalog_index(index),
    do:
      "EXISTS (SELECT 1 FROM pg_catalog.pg_index i " <>
        "WHERE i.indexrelid OPERATOR(pg_catalog.=) #{index} " <>
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
        "AND NOT EXISTS (SELECT 1 FROM pg_catalog.unnest(CAST(i.indcollation AS pg_catalog.oid[])) AS l(o) " <>
        "JOIN pg_catalog.pg_collation co ON co.oid OPERATOR(pg_catalog.=) l.o " <>
        "WHERE co.collnamespace OPERATOR(pg_catalog.<>) #{@catalog}) " <>
        "AND " <> base_types("i.indrelid", "CAST(i.indkey AS pg_catalog.int2[])") <> ")"

  @doc """
  The deferrable triggers of any kind: a cheap test that needs the count below
  only when it finds one. Every trigger the count sees is one of these.
  """
  def probe_sql,
    do:
      "SELECT pg_catalog.count(*) FROM pg_catalog.pg_trigger t " <>
        "WHERE t.tgdeferrable AND t.tgenabled OPERATOR(pg_catalog.<>) 'D'"

  @doc "The count statement (no parameters); every name pg_catalog-qualified."
  def count_sql,
    do:
      "SELECT pg_catalog.count(*) FROM pg_catalog.pg_trigger t " <>
        "JOIN pg_catalog.pg_proc f ON f.oid OPERATOR(pg_catalog.=) t.tgfoid " <>
        "WHERE t.tgdeferrable AND t.tgenabled OPERATOR(pg_catalog.<>) 'D' " <>
        "AND NOT (t.tgisinternal AND f.pronamespace OPERATOR(pg_catalog.=) #{@catalog} AND EXISTS (" <>
        "SELECT 1 FROM pg_catalog.pg_constraint c WHERE c.oid OPERATOR(pg_catalog.=) t.tgconstraint AND (" <>
        "(pg_catalog.left(CAST(f.proname AS pg_catalog.text), 8) OPERATOR(pg_catalog.=) 'RI_FKey_' " <>
        "AND " <>
        catalog_operators("c.conpfeqop") <>
        " AND " <>
        catalog_operators("c.conppeqop") <>
        " AND " <>
        catalog_operators("c.conffeqop") <>
        " AND " <>
        catalog_index("c.conindid") <>
        " AND " <>
        base_types("c.conrelid", "c.conkey") <>
        " AND " <>
        base_types("c.confrelid", "c.confkey") <>
        ") " <>
        "OR (f.proname OPERATOR(pg_catalog.=) 'unique_key_recheck' AND " <>
        catalog_index("c.conindid") <>
        " AND " <>
        "(c.conexclop IS NULL OR " <> catalog_operators("c.conexclop") <> ")))))"
end

defmodule SelectoDBPostgreSQL.SessionGuard.Control do
  @moduledoc false

  # See SessionGuard.control/1.
  @enforce_keys [:sql]
  defstruct [:sql]
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
  # savepoint rolls back (which also reverts set_config), and a read rolls
  # back to its own savepoint (host transaction) or its own transaction
  # (autocommit).
  #
  # Selecto keeps no session marker or GUC of its own to prove ownership
  # (item 57), so there is nothing a trigger can forge.
  #
  # The shadow check refuses SQL whose names a temp TYPE would capture (not
  # a temp table's row type or its array): one named like an identifier of
  # the statement or a type the generated SQL casts to. Relations are
  # qualified to the pinned resolution's schema (RelationPath, R38a), so a
  # temp relation named like one captures nothing; an unrelated host temp
  # table keeps working (item 19).

  alias Selecto.Write.Error

  @settings [
    "search_path",
    "TimeZone",
    "DateStyle",
    "IntervalStyle",
    "extra_float_digits",
    "bytea_output",
    "standard_conforming_strings",
    "timezone_abbreviations",
    "client_encoding"
  ]

  # R30, R35 (amended): the settings every Selecto statement runs under,
  # each at its pg_settings.reset_val (the server, database, role or startup
  # value, which SET and set_config do not change); search_path is given
  # per statement. A masked set_config(guc, evil, false) persisted by
  # earlier SQL therefore never decides how a Selecto statement parses or
  # renders a value.
  @pinned ~w(TimeZone DateStyle IntervalStyle extra_float_digits bytea_output standard_conforming_strings timezone_abbreviations)

  # Type names the generated SQL casts to or literals parse through.
  @generated_types ~w(text varchar bpchar char date time timetz timestamp timestamptz uuid
                      jsonb json jsonpath regconfig regclass oid tid numeric int2 int4 int8
                      float4 float8 bool interval bytea inet cidr name)

  # pg_settings is a scan of every setting: once per pin, not once per setting.
  @reset_values "selecto_reset_values AS MATERIALIZED (SELECT s.name, s.reset_val " <>
                  "FROM pg_catalog.pg_settings s WHERE s.name OPERATOR(pg_catalog.=) ANY (ARRAY[" <>
                  Enum.map_join(@pinned, ", ", &"'#{&1}'") <>
                  "]::pg_catalog.text[]))"

  @temp "pg_catalog.pg_my_temp_schema()"

  # The settings are compared one by one (R30, R35: Selecto pins them
  # around its statements), the temp catalog in one string.
  @temp_columns "pg_catalog.concat_ws(' | ', 'temp'" <>
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
                  "AS selecto_guard_session"

  # client_encoding by its canonical name: the setting keeps a spelling like
  # UNICODE, which is UTF8.
  @setting_columns Enum.map_join(Enum.with_index(@settings), ", ", fn
                     {"client_encoding", index} ->
                       "CAST(pg_catalog.pg_client_encoding() AS pg_catalog.text) " <>
                         "AS selecto_guard_setting_#{index}"

                     {name, index} ->
                       "pg_catalog.current_setting('#{name}') AS selecto_guard_setting_#{index}"
                   end)

  # A read leaves the deferrable triggers out (R61): only writes refuse on
  # them, and another session's trigger is no change of this one.
  @read_columns @temp_columns <> ", " <> @setting_columns

  @snapshot_columns @temp_columns <>
                      ", (" <>
                      SelectoDBPostgreSQL.SessionGuard.Deferrable.probe_sql() <>
                      ") AS selecto_guard_deferrable, " <> @setting_columns

  @snapshot_sql "SELECT " <> @snapshot_columns

  # R38a: relations are qualified to the schema the pinned (reset or
  # declared) resolution chose, never pg_temp unless declared, so a temp
  # relation named like one cannot capture it; $1 (the relation names) is
  # kept for the call shape only. A temp TYPE named like an identifier or a
  # generated cast type is still refused (defence in depth: the pinned path
  # searches pg_temp last).
  @shadow_count "CAST(CASE WHEN $1::pg_catalog.text[] IS NULL THEN 0 ELSE 0 END AS pg_catalog.int8) " <>
                  "OPERATOR(pg_catalog.+) (SELECT pg_catalog.count(*) FROM pg_catalog.pg_type t " <>
                  "WHERE t.typnamespace OPERATOR(pg_catalog.=) #{@temp} " <>
                  "AND t.typrelid OPERATOR(pg_catalog.=) 0 " <>
                  "AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_type e WHERE e.oid OPERATOR(pg_catalog.=) t.typelem " <>
                  "AND e.typrelid OPERATOR(pg_catalog.<>) 0) " <>
                  "AND CAST(t.typname AS pg_catalog.text) OPERATOR(pg_catalog.=) ANY ($2::pg_catalog.text[])) " <>
                  "AS selecto_guard_shadowed"

  @shadow_sql "SELECT " <> @shadow_count <> ", " <> @snapshot_columns

  @doc """
  A transaction statement of the adapter's own, marked: only a statement
  built this way takes the simple protocol, which would run anything after a
  semicolon. Text is never classified by what it starts with, and the mark
  carries one of these statements or none: control_sql/1 checks it again.
  """
  def control(sql) when is_binary(sql) do
    true = control_sql?(sql)
    %__MODULE__.Control{sql: sql}
  end

  @doc "The text of a marked statement, or nil: a hand-built mark is no mark."
  def control_sql(%__MODULE__.Control{sql: sql}) when is_binary(sql),
    do: if(control_sql?(sql), do: sql)

  def control_sql(_other), do: nil

  defp control_sql?(sql),
    do:
      Regex.match?(
        ~r/\A(?:BEGIN|COMMIT|ROLLBACK|(?:SAVEPOINT|RELEASE SAVEPOINT|ROLLBACK TO SAVEPOINT) selecto_[a-z]+_[0-9a-f]{16})\z/,
        sql
      )

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
        utf8(%{
          snapshot: snapshot,
          deferrable: deferrable,
          settings: Enum.zip(@settings, settings)
        })

      {:ok, %{rows: [[shadowed | _]]}} when is_integer(shadowed) and shadowed > 0 ->
        shadowed()

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
        utf8(%{
          snapshot: snapshot,
          deferrable: deferrable,
          settings: Enum.zip(@settings, settings)
        })

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
        if settings_expected?(current, baseline, opts),
          do: deferrable_free(execute, current, opts),
          else: changed()

      {:ok, _changed} ->
        changed()

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Whether `current` (a check/2 or snapshot/1 state) matches `baseline`:
  the same temp catalog and the expected settings (`pinned:` in `opts`, the
  values pin/2 set while Selecto's pin is in place, else the baseline's).
  """
  def same?(current, baseline, opts \\ []) do
    current.snapshot == baseline.snapshot and settings_expected?(current, baseline, opts)
  end

  defp settings_expected?(current, baseline, opts) do
    pinned = Keyword.get(opts, :pinned) || %{}

    Enum.all?(baseline.settings, fn {name, value} ->
      :proplists.get_value(name, current.settings, nil) == Map.get(pinned, name, value)
    end)
  end

  @doc "The search_path of a snapshot state."
  def search_path(%{settings: settings}), do: :proplists.get_value("search_path", settings, nil)

  @doc """
  R30, R35: pins search_path to `path` and every other result-affecting
  setting to its reset value for the rest of the current transaction
  (set_config(..., true)), in one statement; run only inside Selecto's own
  transaction or savepoint, after the snapshot it is compared with. Returns
  `{:ok, pinned}`, each setting's value as the server normalised it, and
  `:isolation`, the transaction's level (R49a).
  """
  def pin(execute, path, opts \\ []) do
    sql = "WITH " <> @reset_values <> " SELECT " <> pin_columns("$1", opts)

    # An unreadable reset value (NULL, or an error) fails closed.
    case execute.(sql, [path]) do
      {:ok, %{rows: [values]}} -> pinned(values)
      _other -> unavailable()
    end
  end

  @doc """
  A read's opening: the check/3 of `sql`, then pin/3 to `path`, in one
  statement whose snapshot is taken before any pin (a materialised CTE the
  pin reads). Returns `{:ok, before, pinned}`. Refuses as check/3 does; the
  caller's rollback undoes the pin.
  """
  def open_read(execute, sql, path, opts \\ []) do
    statement =
      "WITH selecto_before AS MATERIALIZED (SELECT " <>
        @shadow_count <>
        ", " <>
        @read_columns <>
        "), " <>
        @reset_values <>
        " SELECT b.*, " <> pin_columns("$3", opts) <> " FROM selecto_before b"

    params = [Enum.uniq(relation_tokens(sql)), tokens(sql), path]
    width = length(@settings) + 2

    case execute.(statement, params) do
      {:ok, %{rows: [row]}} when length(row) > width ->
        {[shadowed, snapshot | settings], values} = Enum.split(row, width)

        cond do
          not is_integer(shadowed) or not is_binary(snapshot) ->
            unavailable()

          shadowed > 0 ->
            shadowed()

          true ->
            state = %{snapshot: snapshot, deferrable: 0, settings: Enum.zip(@settings, settings)}

            with {:ok, state} <- utf8(state),
                 {:ok, pinned} <- pinned(values) do
              {:ok, state, pinned}
            end
        end

      _other ->
        unavailable()
    end
  end

  @doc """
  The close of a read or (`writes: true`) a write: compare/3 and unpin/3 in
  one statement, the snapshot taken while the pins should still hold and
  before unpin's restore. Refuses as they do.
  """
  def close(execute, %{snapshot: before} = baseline, pinned, opts \\ []) do
    {unpin_sql, params} = unpin_statement(pinned, baseline)
    writes? = Keyword.get(opts, :writes) == true
    columns = if writes?, do: @snapshot_columns, else: @read_columns

    statement =
      "WITH selecto_after AS MATERIALIZED (SELECT " <>
        columns <>
        ") SELECT a.*, " <> unpin_sql <> " FROM selecto_after a"

    case execute.(statement, params) do
      {:ok, %{rows: [[^before | row]]}} ->
        {deferrable, row} = if writes?, do: {hd(row), tl(row)}, else: {0, row}
        {settings, [restored]} = Enum.split(row, length(@settings))
        current = %{deferrable: deferrable, settings: Enum.zip(@settings, settings)}

        cond do
          not settings_expected?(current, baseline, pinned: pinned) -> changed()
          not is_binary(restored) -> changed()
          true -> deferrable_free(execute, current, opts)
        end

      {:ok, %{rows: [[snapshot | _]]}} when is_binary(snapshot) ->
        changed()

      _other ->
        unavailable()
    end
  end

  defp pin_columns(path, opts) do
    read_only =
      if Keyword.get(opts, :read_only) == true,
        do:
          ", pg_catalog.set_config('transaction_read_only', 'on', true) " <>
            "AS selecto_guard_pin_read_only",
        else: ""

    "pg_catalog.set_config('search_path', #{path}::pg_catalog.text, true) " <>
      "AS selecto_guard_pin_0, " <>
      Enum.map_join(Enum.with_index(@pinned, 1), ", ", fn {name, index} ->
        reset =
          "(SELECT r.reset_val FROM selecto_reset_values r " <>
            "WHERE r.name OPERATOR(pg_catalog.=) '#{name}')"

        "pg_catalog.set_config('#{name}', #{reset}, true) AS selecto_guard_pin_#{index}"
      end) <>
      ", pg_catalog.current_setting('transaction_isolation') AS selecto_guard_pin_isolation" <>
      read_only
  end

  defp pinned(values) do
    names = ["search_path" | @pinned]

    if length(values) > length(names) do
      {values, [isolation | _read_only]} = Enum.split(values, length(names))

      if Enum.all?(values, &is_binary/1),
        do:
          {:ok, values |> then(&Map.new(Enum.zip(names, &1))) |> Map.put(:isolation, isolation)},
        else: unavailable()
    else
      unavailable()
    end
  end

  @doc """
  R30, R35: restores the host's values (the settings of `before`) with
  set_config(..., true), in the same expression that checks every pin is
  still in place, so a statement that switched one in between refuses.
  """
  def unpin(execute, pinned, baseline) do
    {sql, params} = unpin_statement(pinned, baseline)

    case execute.("SELECT " <> sql, params) do
      {:ok, %{rows: [[value]]}} when is_binary(value) -> :ok
      {:ok, %{rows: [[nil]]}} -> changed()
      _other -> unavailable()
    end
  end

  defp unpin_statement(pinned, %{settings: settings}) do
    pinned = Map.delete(pinned, :isolation)
    names = Map.keys(pinned) |> Enum.sort()
    count = length(names)

    held =
      names
      |> Enum.with_index(1)
      |> Enum.map_join(" AND ", fn {name, index} ->
        "pg_catalog.current_setting('#{name}') OPERATOR(pg_catalog.=) $#{index}::pg_catalog.text"
      end)

    restore =
      names
      |> Enum.with_index(count + 1)
      |> Enum.map_join(", ", fn {name, index} ->
        "pg_catalog.set_config('#{name}', $#{index}::pg_catalog.text, true)"
      end)

    params =
      Enum.map(names, &Map.fetch!(pinned, &1)) ++
        Enum.map(names, &:proplists.get_value(&1, settings, nil))

    {"CASE WHEN #{held} THEN pg_catalog.concat_ws(',', #{restore}) END AS selecto_guard_unpin",
     params}
  end

  @doc """
  R23, R32 (rulings): a deferrable trigger fires at COMMIT, after every
  check of the write, so no PostgreSQL write runs while one exists that can
  run user code (`SelectoDBPostgreSQL.SessionGuard.Deferrable`). Writes
  check before their DML and in the first post-DML statement
  (`writes: true`).
  """
  def deferrable_free(execute, state, opts \\ [])

  def deferrable_free(execute, %{deferrable: count}, opts) when is_integer(count) and count > 0 do
    if Keyword.get(opts, :writes) do
      case deferrable_count(execute, count) do
        {:ok, 0} ->
          :ok

        {:ok, _found} ->
          refused(
            :query_rule_not_evaluable,
            :deferred_triggers,
            "writes are refused while a deferrable trigger exists: it fires at COMMIT, after every check"
          )

        {:error, _} = error ->
          error
      end
    else
      :ok
    end
  end

  def deferrable_free(_execute, _state, _opts), do: :ok

  # R65: Postgrex sends a parameter as UTF-8 bytes, which the server reads in
  # the session's client_encoding: under LATIN1 the tenant café arrives as
  # cafÃ©, another tenant's name. Every statement's check refuses it, and the
  # setting is compared with the rest, so a statement that changes it is
  # caught before the next one sends a parameter.
  defp utf8(state) do
    if :proplists.get_value("client_encoding", state.settings, nil) == "UTF8",
      do: {:ok, state},
      else:
        refused(
          :query_rule_not_evaluable,
          :client_encoding,
          "the session's client_encoding is not UTF8: parameters would be misread"
        )
  end

  defp shadowed,
    do:
      refused(
        :query_rule_not_evaluable,
        :temporary_shadow,
        "a temporary relation or type of the session shadows a name the statement uses"
      )

  @doc """
  The deferrable triggers that can run user code, given the probe's count:
  the full statement runs only when the probe found one.
  """
  def deferrable_count(_execute, 0), do: {:ok, 0}

  def deferrable_count(execute, probe) when is_integer(probe) do
    sql =
      "SELECT (" <>
        SelectoDBPostgreSQL.SessionGuard.Deferrable.count_sql() <>
        ") AS selecto_guard_deferrable"

    case execute.(sql, []) do
      {:ok, %{rows: [[count]]}} when is_integer(count) -> {:ok, count}
      _other -> unavailable()
    end
  end

  def deferrable_count(_execute, _probe), do: unavailable()

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
