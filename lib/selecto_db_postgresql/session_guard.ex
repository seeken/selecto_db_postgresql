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

  @snapshot_columns "pg_catalog.concat_ws(' | ', " <>
                      Enum.map_join(@settings, ", ", &"pg_catalog.current_setting('#{&1}')") <>
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
                      "AS selecto_session, " <>
                      Enum.map_join(Enum.with_index(@settings), ", ", fn {name, index} ->
                        "pg_catalog.current_setting('#{name}') AS selecto_setting_#{index}"
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
                "AND CAST(t.typname AS pg_catalog.text) OPERATOR(pg_catalog.=) ANY ($1::pg_catalog.text[])) " <>
                "AS selecto_shadowed, " <> @snapshot_columns

  @doc "The snapshot statement (no parameters)."
  def snapshot_sql, do: @snapshot_sql

  @doc """
  Refuses `sql` (one statement text or several) when a temp relation or type
  would capture one of its names, and returns the session snapshot taken in
  the same statement.
  """
  def check(execute, sql) do
    case execute.(@shadow_sql, [tokens(sql)]) do
      {:ok, %{rows: [[0, snapshot | settings]]}} when is_binary(snapshot) ->
        {:ok, %{snapshot: snapshot, settings: Enum.zip(@settings, settings)}}

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
      {:ok, %{rows: [[snapshot | settings]]}} when is_binary(snapshot) ->
        {:ok, %{snapshot: snapshot, settings: Enum.zip(@settings, settings)}}

      _other ->
        unavailable()
    end
  end

  @doc """
  Compares the session with `before`. Run as the FIRST statement after a
  DML or a read, before any other SQL.
  """
  def compare(execute, %{snapshot: before}) do
    case snapshot(execute) do
      {:ok, %{snapshot: ^before}} -> :ok
      {:ok, _changed} -> changed()
      {:error, _} = error -> error
    end
  end

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
