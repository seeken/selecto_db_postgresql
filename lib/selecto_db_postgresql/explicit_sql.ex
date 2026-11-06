defmodule SelectoDBPostgreSQL.ExplicitSQL do
  @moduledoc false

  # R46 FINAL-2 / R47 / R49 / R50: the last step before a Selecto statement
  # reaches PostgreSQL. A statement that already names every operator and
  # function runs as it is (no catalog read); any other goes through
  # SelectoDBPostgreSQL.Qualify with the catalog facts it needs, read here
  # under the statement's own pin: the candidates of each function it calls
  # among the pinned schemas (a temp schema never), and, when an extension
  # type exists in the database, what each column of each relation it reads
  # has of an extension member type: its operators and functions, always
  # pg_depend members of the type's own extension, never a user object
  # beside them. The result is linted again: nothing runs by name-based
  # operator or function lookup, and a statement that cannot be made
  # explicit refuses, typed.
  #
  # R49a: under REPEATABLE READ or SERIALIZABLE the catalog is read at the
  # transaction's snapshot while the statement plans against the current
  # catalog, so the types of the columns a statement names are read again
  # through the parser (pg_typeof((NULL::relation).column)); a difference
  # refuses.

  alias Selecto.Write.Error
  alias SelectoDBPostgreSQL.Qualify

  @op "OPERATOR(pg_catalog.=)"
  @type_class "CAST('pg_catalog.pg_type' AS pg_catalog.regclass)"
  @proc_class "CAST('pg_catalog.pg_proc' AS pg_catalog.regclass)"
  @operator_class "CAST('pg_catalog.pg_operator' AS pg_catalog.regclass)"
  @any_result "CAST('{anyelement,anynonarray,anycompatible,anycompatiblenonarray}' AS pg_catalog.regtype[])"
  @any_argument "CAST('{anyelement,anynonarray,anycompatible,anycompatiblenonarray,anyarray,anycompatiblearray}' AS pg_catalog.regtype[])"

  @quoted_argument "pg_catalog.concat(pg_catalog.quote_ident(CAST(tn.nspname AS pg_catalog.text)), '.', " <>
                     "pg_catalog.quote_ident(CAST(t.typname AS pg_catalog.text)))"

  @quoted_variadic "pg_catalog.concat(pg_catalog.quote_ident(CAST(vn.nspname AS pg_catalog.text)), '.', " <>
                     "pg_catalog.quote_ident(CAST(vt.typname AS pg_catalog.text)))"

  # Whether any extension-owned type exists, the isolation level and every
  # candidate of the functions a statement calls: `$1` the schema a call
  # names ('' for none), `$2` the names. A bare name looks in the pinned
  # schemas only.
  @probe_sql """
  SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_depend d
           WHERE d.classid #{@op} #{@type_class} AND d.deptype #{@op} 'e')
           AS selecto_guard_extension_types,
         pg_catalog.current_setting('transaction_isolation') AS selecto_guard_isolation,
         ARRAY(SELECT pg_catalog.concat_ws(pg_catalog.chr(31), k.sch, k.name,
             CAST(n.nspname AS pg_catalog.text), p.pronargs, p.pronargdefaults,
             pg_catalog.array_to_string(ARRAY(SELECT #{@quoted_argument}
               FROM pg_catalog.unnest(CAST(p.proargtypes AS pg_catalog.oid[])) WITH ORDINALITY AS u(o, i)
               JOIN pg_catalog.pg_type t ON t.oid #{@op} u.o
               JOIN pg_catalog.pg_namespace tn ON tn.oid #{@op} t.typnamespace
               ORDER BY u.i), pg_catalog.chr(30)),
             COALESCE((SELECT #{@quoted_variadic}
               FROM pg_catalog.pg_type vt
               JOIN pg_catalog.pg_namespace vn ON vn.oid #{@op} vt.typnamespace
               WHERE vt.oid #{@op} p.provariadic), ''),
             CASE WHEN EXISTS (SELECT 1 FROM pg_catalog.pg_depend pd
                 WHERE pd.classid #{@op} #{@proc_class} AND pd.objid #{@op} p.oid
                   AND pd.deptype #{@op} 'e'
                   AND EXISTS (SELECT 1 FROM pg_catalog.unnest(CAST(p.proargtypes AS pg_catalog.oid[])) AS ua(t)
                     JOIN pg_catalog.pg_depend td ON td.classid #{@op} #{@type_class}
                       AND td.objid #{@op} ua.t AND td.deptype #{@op} 'e'
                       AND td.refobjid #{@op} pd.refobjid))
               THEN 'member' ELSE '' END,
             COALESCE((SELECT CAST(pg_catalog.min(xd.refobjid) AS pg_catalog.text)
               FROM pg_catalog.pg_depend xd
               WHERE xd.classid #{@op} #{@proc_class} AND xd.objid #{@op} p.oid
                 AND xd.deptype #{@op} 'e'), ''),
             CASE WHEN CAST(p.prorettype AS pg_catalog.regtype) #{@op} ANY (#{@any_result})
                    AND EXISTS (SELECT 1 FROM pg_catalog.unnest(CAST(p.proargtypes AS pg_catalog.oid[])) AS a(t)
                      WHERE CAST(a.t AS pg_catalog.regtype) #{@op} ANY (#{@any_argument}))
               THEN 'polymorphic' ELSE '' END,
             (SELECT #{@quoted_argument} FROM pg_catalog.pg_type t
              JOIN pg_catalog.pg_namespace tn ON tn.oid #{@op} t.typnamespace
              WHERE t.oid #{@op} p.prorettype))
           FROM ROWS FROM (pg_catalog.unnest($1::pg_catalog.text[]), pg_catalog.unnest($2::pg_catalog.text[])) AS k(sch, name)
           JOIN pg_catalog.pg_proc p ON CAST(p.proname AS pg_catalog.text) #{@op} k.name
           JOIN pg_catalog.pg_namespace n ON n.oid #{@op} p.pronamespace
           WHERE p.prokind OPERATOR(pg_catalog.<>) 'p'
             AND n.oid OPERATOR(pg_catalog.<>)
                   COALESCE(pg_catalog.pg_my_temp_schema(), CAST(0 AS pg_catalog.oid))
             AND CASE WHEN k.sch #{@op} ''
                   THEN n.nspname #{@op} ANY (pg_catalog.current_schemas(true))
                   ELSE CAST(n.nspname AS pg_catalog.text) #{@op} k.sch END)
           AS selecto_guard_function_candidates
  """

  # The domain-and-array walk of every column of the named relations (`$1`).
  @columns_ctes """
  a AS (SELECT r.rel, at.attname, at.atttypid
    FROM pg_catalog.unnest($1::pg_catalog.text[]) AS r(rel)
    JOIN pg_catalog.pg_attribute at ON at.attrelid #{@op} pg_catalog.to_regclass(r.rel)
      AND at.attnum OPERATOR(pg_catalog.>) 0 AND NOT at.attisdropped),
  c(rel, att, oid, depth, arr) AS (
    SELECT a.rel, a.attname, a.atttypid, 0, false FROM a
    UNION ALL
    SELECT c.rel, c.att, CASE WHEN t.typtype #{@op} 'd' THEN t.typbasetype ELSE t.typelem END,
      c.depth OPERATOR(pg_catalog.+) 1, c.arr OR t.typtype OPERATOR(pg_catalog.<>) 'd'
    FROM c JOIN pg_catalog.pg_type t ON t.oid #{@op} c.oid
    WHERE c.depth OPERATOR(pg_catalog.<) 16 AND (t.typtype #{@op} 'd' OR (NOT c.arr
      AND t.typtype #{@op} 'b' AND t.typcategory #{@op} 'A'
      AND t.typelem OPERATOR(pg_catalog.<>) CAST(0 AS pg_catalog.oid)
      AND t.typlen OPERATOR(pg_catalog.<) 0))),
  e AS (SELECT c.rel, c.att, c.arr, ty.oid, d.refobjid FROM c
    JOIN pg_catalog.pg_type ty ON ty.oid #{@op} c.oid AND ty.typtype OPERATOR(pg_catalog.<>) 'd'
    JOIN pg_catalog.pg_depend d ON d.classid #{@op} #{@type_class}
      AND d.objid #{@op} ty.oid AND d.deptype #{@op} 'e')
  """

  # Types an extension owns, domains over them and arrays of them, and each
  # function a statement reads rows from (`$2`) with its output types.
  @rowset_ctes """
  , x(oid, depth) AS (
    SELECT d.objid, 0 FROM pg_catalog.pg_depend d
    WHERE d.classid #{@op} #{@type_class} AND d.deptype #{@op} 'e'
    UNION ALL
    SELECT t.oid, x.depth OPERATOR(pg_catalog.+) 1 FROM pg_catalog.pg_type t
    JOIN x ON t.typbasetype #{@op} x.oid OR t.typelem #{@op} x.oid
    WHERE x.depth OPERATOR(pg_catalog.<) 16),
  f AS (SELECT p.oid, p.proname, CASE WHEN p.proallargtypes IS NULL THEN ARRAY[p.prorettype]
      ELSE ARRAY(SELECT u.t FROM ROWS FROM (pg_catalog.unnest(p.proallargtypes), pg_catalog.unnest(p.proargmodes)) AS u(t, m)
        WHERE u.m #{@op} ANY (CAST('{o,b,t}' AS pg_catalog."char"[]))) END AS outs
    FROM pg_catalog.pg_proc p
    WHERE CAST(p.proname AS pg_catalog.text) #{@op} ANY ($2::pg_catalog.text[]))
  """

  @none "CAST(NULL AS pg_catalog.text)"
  @no_args "CAST(NULL AS pg_catalog.text[])"

  # An operator's two argument types, quoted.
  @operator_sides "ARRAY[" <>
                    "(SELECT #{@quoted_argument} FROM pg_catalog.pg_type t " <>
                    "JOIN pg_catalog.pg_namespace tn ON tn.oid #{@op} t.typnamespace WHERE t.oid #{@op} o.oprleft), " <>
                    "(SELECT #{@quoted_argument} FROM pg_catalog.pg_type t " <>
                    "JOIN pg_catalog.pg_namespace tn ON tn.oid #{@op} t.typnamespace WHERE t.oid #{@op} o.oprright)]"

  @columns_rows """
  SELECT 'col' AS selecto_guard_fact, CAST(a.rel AS pg_catalog.text) AS selecto_guard_fact_relation,
    CAST(a.attname AS pg_catalog.text) AS selecto_guard_fact_column,
    CAST(a.atttypid AS pg_catalog.text) AS selecto_guard_fact_name,
    (SELECT #{@quoted_argument} FROM c
     JOIN pg_catalog.pg_type t ON t.oid #{@op} c.oid AND t.typtype OPERATOR(pg_catalog.<>) 'd'
     JOIN pg_catalog.pg_namespace tn ON tn.oid #{@op} t.typnamespace
     WHERE c.rel #{@op} a.rel AND c.att #{@op} a.attname AND NOT c.arr LIMIT 1) AS selecto_guard_fact_schema,
    #{@no_args} AS selecto_guard_fact_arguments FROM a
  UNION ALL
  SELECT 'deep', CAST(c.rel AS pg_catalog.text), CAST(c.att AS pg_catalog.text), #{@none}, #{@none}, #{@no_args}
  FROM c JOIN pg_catalog.pg_type ty ON ty.oid #{@op} c.oid AND ty.typtype #{@op} 'd'
  WHERE c.depth #{@op} 16
  UNION ALL
  SELECT CASE WHEN e.arr THEN 'eop' ELSE 'op' END, CAST(e.rel AS pg_catalog.text), CAST(e.att AS pg_catalog.text),
    CAST(o.oprname AS pg_catalog.text), CAST(n.nspname AS pg_catalog.text), #{@operator_sides}
  FROM e JOIN pg_catalog.pg_operator o ON (o.oprleft #{@op} e.oid OR o.oprright #{@op} e.oid)
    AND o.oprleft OPERATOR(pg_catalog.<>) CAST(0 AS pg_catalog.oid)
    AND o.oprright OPERATOR(pg_catalog.<>) CAST(0 AS pg_catalog.oid)
  JOIN pg_catalog.pg_depend od ON od.classid #{@op} #{@operator_class}
    AND od.objid #{@op} o.oid AND od.deptype #{@op} 'e' AND od.refobjid #{@op} e.refobjid
  JOIN pg_catalog.pg_namespace n ON n.oid #{@op} o.oprnamespace
  WHERE o.oprnamespace #{@op} (SELECT et.typnamespace FROM pg_catalog.pg_type et WHERE et.oid #{@op} e.oid)
  UNION ALL
  SELECT CASE WHEN e.arr THEN 'etyp' ELSE 'typ' END, CAST(e.rel AS pg_catalog.text), CAST(e.att AS pg_catalog.text),
    pg_catalog.concat(pg_catalog.quote_ident(CAST(tn.nspname AS pg_catalog.text)), '.',
      pg_catalog.quote_ident(CAST(ty.typname AS pg_catalog.text))),
    CAST(ty.typcategory AS pg_catalog.text), #{@no_args}
  FROM e JOIN pg_catalog.pg_type ty ON ty.oid #{@op} e.oid
  JOIN pg_catalog.pg_namespace tn ON tn.oid #{@op} ty.typnamespace
  UNION ALL
  SELECT CASE WHEN e.arr THEN 'efn' ELSE 'fn' END, CAST(e.rel AS pg_catalog.text), CAST(e.att AS pg_catalog.text),
    CAST(p.proname AS pg_catalog.text),
    pg_catalog.concat(n.nspname, CASE WHEN p.prorettype #{@op} e.oid THEN '|self' ELSE '' END),
    ARRAY(SELECT #{@quoted_argument}
      FROM pg_catalog.unnest(CAST(p.proargtypes AS pg_catalog.oid[])) WITH ORDINALITY AS u(o, i)
      JOIN pg_catalog.pg_type t ON t.oid #{@op} u.o
      JOIN pg_catalog.pg_namespace tn ON tn.oid #{@op} t.typnamespace
      ORDER BY u.i)
  FROM e JOIN pg_catalog.pg_proc p ON p.pronargs OPERATOR(pg_catalog.>=) CAST(1 AS pg_catalog.int2)
    AND p.proargtypes[0] #{@op} e.oid AND p.prokind OPERATOR(pg_catalog.<>) 'p'
  JOIN pg_catalog.pg_depend pd ON pd.classid #{@op} #{@proc_class}
    AND pd.objid #{@op} p.oid AND pd.deptype #{@op} 'e' AND pd.refobjid #{@op} e.refobjid
  JOIN pg_catalog.pg_namespace n ON n.oid #{@op} p.pronamespace
  WHERE p.pronamespace #{@op} (SELECT et.typnamespace FROM pg_catalog.pg_type et WHERE et.oid #{@op} e.oid)
  """

  @rowset_rows """
  UNION ALL
  SELECT 'fun', #{@none}, CAST(f.proname AS pg_catalog.text), #{@none},
    CASE WHEN pg_catalog.bool_or(EXISTS (SELECT 1 FROM pg_catalog.unnest(f.outs) AS o(t)
      JOIN pg_catalog.pg_type ot ON ot.oid #{@op} o.t
      WHERE o.t #{@op} ANY (ARRAY(SELECT x.oid FROM x))
        OR (ot.typtype #{@op} 'p' AND ot.oid OPERATOR(pg_catalog.<>) CAST('pg_catalog.record' AS pg_catalog.regtype)
          AND EXISTS (SELECT 1 FROM x))
        OR (ot.typtype #{@op} 'c' AND EXISTS (SELECT 1 FROM pg_catalog.pg_attribute ca
          WHERE ca.attrelid #{@op} ot.typrelid AND ca.attnum OPERATOR(pg_catalog.>) 0 AND NOT ca.attisdropped
            AND ca.atttypid #{@op} ANY (ARRAY(SELECT x.oid FROM x))))))
      THEN 'unknown' ELSE 'known' END, #{@no_args}
  FROM f GROUP BY f.proname
  UNION ALL
  SELECT CASE WHEN o.op IS NULL THEN 'fcol' ELSE 'fop' END, CAST(w.pos AS pg_catalog.text),
    CAST(f.proname AS pg_catalog.text), o.op, o.schema, #{@no_args}
  FROM f JOIN LATERAL (SELECT u.pos, u.t FROM pg_catalog.unnest(f.outs) WITH ORDINALITY AS u(t, pos)) AS w ON TRUE
  JOIN pg_catalog.pg_type wt ON wt.oid #{@op} w.t
  JOIN LATERAL (
    SELECT #{@none} AS op, CASE WHEN EXISTS (SELECT 1 FROM pg_catalog.pg_depend d
        WHERE d.classid #{@op} #{@type_class} AND d.deptype #{@op} 'e' AND d.objid #{@op} w.t)
        THEN pg_catalog.concat('ext:', CAST(wt.typcategory AS pg_catalog.text), ':', pg_catalog.quote_ident(CAST((SELECT tn.nspname FROM pg_catalog.pg_namespace tn
          WHERE tn.oid #{@op} wt.typnamespace) AS pg_catalog.text)), '.', pg_catalog.quote_ident(CAST(wt.typname AS pg_catalog.text)))
      WHEN w.t #{@op} ANY (ARRAY(SELECT x.oid FROM x)) OR wt.typtype #{@op} 'p'
        OR wt.typtype #{@op} 'c' THEN 'unknown' ELSE 'known' END AS schema
    UNION ALL
    SELECT CAST(op.oprname AS pg_catalog.text), CAST(n.nspname AS pg_catalog.text)
    FROM pg_catalog.pg_depend d JOIN pg_catalog.pg_operator op ON op.oprleft #{@op} w.t AND op.oprright #{@op} w.t
    JOIN pg_catalog.pg_depend od ON od.classid #{@op} #{@operator_class}
      AND od.objid #{@op} op.oid AND od.deptype #{@op} 'e' AND od.refobjid #{@op} d.refobjid
    JOIN pg_catalog.pg_namespace n ON n.oid #{@op} op.oprnamespace
    WHERE d.classid #{@op} #{@type_class} AND d.deptype #{@op} 'e' AND d.objid #{@op} w.t) AS o ON TRUE
  WHERE (SELECT pg_catalog.count(*) FROM f g WHERE g.proname #{@op} f.proname) #{@op} CAST(1 AS pg_catalog.int8)
  """

  @relations_sql "WITH RECURSIVE\n" <> @columns_ctes <> @columns_rows

  @relations_rowsets_sql "WITH RECURSIVE\n" <>
                           @columns_ctes <> @rowset_ctes <> @columns_rows <> @rowset_rows

  # A cast target's extension type (through any domain chain), its own
  # operators and its qualified name: `$1` the keys, `$2` as spelled.
  @casts_sql """
  WITH RECURSIVE k(name, spelled) AS (
    SELECT * FROM ROWS FROM (pg_catalog.unnest($1::pg_catalog.text[]), pg_catalog.unnest($2::pg_catalog.text[]))),
  c(name, oid, depth) AS (
    SELECT k.name, CAST(pg_catalog.to_regtype(k.spelled) AS pg_catalog.oid), 0 FROM k
    UNION ALL
    SELECT c.name, t.typbasetype, c.depth OPERATOR(pg_catalog.+) 1
    FROM c JOIN pg_catalog.pg_type t ON t.oid #{@op} c.oid
    WHERE t.typtype #{@op} 'd' AND c.depth OPERATOR(pg_catalog.<) 16),
  e AS (SELECT c.name, ty.oid, ty.typnamespace, d.refobjid FROM c
    JOIN pg_catalog.pg_type ty ON ty.oid #{@op} c.oid AND ty.typtype OPERATOR(pg_catalog.<>) 'd'
    JOIN pg_catalog.pg_depend d ON d.classid #{@op} #{@type_class}
      AND d.objid #{@op} ty.oid AND d.deptype #{@op} 'e')
  SELECT 'typ' AS selecto_guard_cast_fact, e.name AS selecto_guard_cast_key,
    pg_catalog.concat(pg_catalog.quote_ident(CAST(tn.nspname AS pg_catalog.text)), '.',
      pg_catalog.quote_ident(CAST(ty.typname AS pg_catalog.text))) AS selecto_guard_cast_name,
    CAST(ty.typcategory AS pg_catalog.text) AS selecto_guard_cast_schema,
    #{@no_args} AS selecto_guard_cast_arguments
  FROM e JOIN pg_catalog.pg_type ty ON ty.oid #{@op} e.oid
  JOIN pg_catalog.pg_namespace tn ON tn.oid #{@op} ty.typnamespace
  UNION ALL
  SELECT 'op', e.name, CAST(o.oprname AS pg_catalog.text), CAST(n.nspname AS pg_catalog.text), #{@operator_sides}
  FROM e JOIN pg_catalog.pg_operator o ON (o.oprleft #{@op} e.oid OR o.oprright #{@op} e.oid)
    AND o.oprleft OPERATOR(pg_catalog.<>) CAST(0 AS pg_catalog.oid)
    AND o.oprright OPERATOR(pg_catalog.<>) CAST(0 AS pg_catalog.oid)
    AND o.oprnamespace #{@op} e.typnamespace
  JOIN pg_catalog.pg_depend od ON od.classid #{@op} #{@operator_class}
    AND od.objid #{@op} o.oid AND od.deptype #{@op} 'e' AND od.refobjid #{@op} e.refobjid
  JOIN pg_catalog.pg_namespace n ON n.oid #{@op} o.oprnamespace
  """

  @implicit_sql """
  SELECT #{"pg_catalog.concat(pg_catalog.quote_ident(CAST(sn.nspname AS pg_catalog.text)), '.', " <> "pg_catalog.quote_ident(CAST(st.typname AS pg_catalog.text)))"} AS selecto_guard_cast_source,
         #{"pg_catalog.concat(pg_catalog.quote_ident(CAST(tn.nspname AS pg_catalog.text)), '.', " <> "pg_catalog.quote_ident(CAST(tt.typname AS pg_catalog.text)))"} AS selecto_guard_cast_target
  FROM pg_catalog.pg_cast c
  JOIN pg_catalog.pg_type st ON st.oid #{@op} c.castsource
  JOIN pg_catalog.pg_namespace sn ON sn.oid #{@op} st.typnamespace
  JOIN pg_catalog.pg_type tt ON tt.oid #{@op} c.casttarget
  JOIN pg_catalog.pg_namespace tn ON tn.oid #{@op} tt.typnamespace
  WHERE c.castcontext #{@op} 'i'
  """

  @doc false
  def lookup_statements, do: [@probe_sql, @relations_sql, @relations_rowsets_sql, @casts_sql]

  @doc """
  `{:ok, sql}` with every operator and function of `sql` explicit (read
  through `execute` when it needs the catalog), or a typed refusal.
  `isolation:` the transaction's level, when the caller knows it (R49a).
  """
  def prepare(execute, sql, opts \\ []) do
    sql = IO.iodata_to_binary(sql)
    verify? = Keyword.get(opts, :isolation) not in [nil, "read committed"]

    try do
      tokens = Qualify.tokenize(sql)

      cond do
        Qualify.rewrite?(tokens) -> rewrite(execute, tokens, opts)
        verify? -> with {:ok, _} <- facts(execute, tokens), do: {:ok, sql}
        true -> {:ok, sql}
      end
    catch
      {:qualify_refuse, reason, message} -> refuse(reason, message)
    end
  end

  defp rewrite(execute, tokens, opts) do
    with {:ok, facts} <- facts(execute, tokens) do
      facts = Map.put(facts, :declared_functions, Keyword.get(opts, :declared_functions, []))

      case Qualify.qualify(tokens, facts) do
        {:ok, sql} ->
          case Qualify.lint(sql) do
            [] ->
              {:ok, sql}

            found ->
              refuse(
                :bare_operator,
                "a statement would resolve an operator or function by name",
                found
              )
          end

        {:error, reason, message} ->
          refuse(reason, message)
      end
    end
  end

  defp facts(execute, tokens) do
    required = Qualify.requirements(tokens)
    keys = Enum.map(required.functions, &{"", &1}) ++ MapSet.to_list(required.named)

    case execute.(@probe_sql, [Enum.map(keys, &elem(&1, 0)), Enum.map(keys, &elem(&1, 1))]) do
      {:ok, %{rows: [[extension_types, isolation, candidates]]}} ->
        functions = functions(candidates)
        required = Map.update!(required, :casts, &Map.merge(&1, result_types(functions)))

        with {:ok, facts} <-
               (if not extension_types and isolation == "read committed" do
                  {:ok, %{functions: functions, extension: false}}
                else
                  relation_facts(execute, tokens, required, functions, extension_types, isolation)
                end) do
          overload_facts(execute, facts, functions)
        end

      _other ->
        unavailable()
    end
  end

  # A function outside pg_catalog is chosen by its arguments' types (R62):
  # the query scopes are needed, and the implicit casts that say what a
  # candidate can take.
  defp overload_facts(execute, facts, functions) do
    outside? =
      Enum.any?(functions, fn {_key, info} ->
        Enum.any?(info.candidates, &(&1.schema != "pg_catalog"))
      end)

    if outside? do
      case execute.(@implicit_sql, []) do
        {:ok, %{rows: rows}} when is_list(rows) ->
          plain = &String.replace(&1, "\"", "")
          implicit = MapSet.new(rows, fn [source, target] -> {plain.(source), plain.(target)} end)
          {:ok, facts |> Map.put(:extension, true) |> Map.put(:implicit, implicit)}

        _other ->
          unavailable()
      end
    else
      {:ok, facts}
    end
  end

  # The non-catalog types candidates return, keyed as a cast is.
  defp result_types(functions) do
    for {_key, info} <- functions,
        %{ret: ret, schema: schema} <- info.candidates,
        schema != "pg_catalog",
        not String.starts_with?(ret, "pg_catalog."),
        into: %{} do
      {ret |> relation_key(), ret}
    end
  end

  defp functions(candidates) do
    Enum.reduce(candidates, %{}, fn row, acc ->
      [sch, name, schema, nargs, defaults, args, variadic, member, extension, polymorphic, ret] =
        String.split(row, <<31>>)

      key = if sch == "", do: name, else: {sch, name}

      candidate = %{
        schema: schema,
        nargs: String.to_integer(nargs),
        defaults: String.to_integer(defaults),
        args: if(args == "", do: [], else: String.split(args, <<30>>)),
        variadic: if(variadic == "", do: nil, else: variadic),
        member: member == "member",
        extension: if(extension == "", do: nil, else: extension),
        ret: ret
      }

      polymorphic? = polymorphic == "polymorphic" and schema == "pg_catalog"

      Map.update(acc, key, %{candidates: [candidate], polymorphic: polymorphic?}, fn info ->
        %{
          candidates: [candidate | info.candidates],
          polymorphic: info.polymorphic or polymorphic?
        }
      end)
    end)
  end

  defp relation_facts(execute, tokens, required, functions, extension_types, isolation) do
    relations = MapSet.to_list(required.relations)
    rowsets = MapSet.to_list(required.rowsets)

    {sql, params} =
      if rowsets == [],
        do: {@relations_sql, [relations]},
        else: {@relations_rowsets_sql, [relations, rowsets]}

    case execute.(sql, params) do
      {:ok, %{rows: rows}} when is_list(rows) ->
        with {:ok, decoded} <- decode(rows),
             :ok <- verify_types(execute, tokens, decoded.types, isolation),
             {:ok, casts} <- cast_facts(execute, required.casts, extension_types) do
          {:ok,
           %{
             functions: functions,
             relations: decoded.relations,
             rowsets: decoded.rowsets,
             casts: casts,
             extension: decoded.extension or map_size(casts) > 0
           }}
        end

      _other ->
        unavailable()
    end
  end

  defp decode(rows) do
    initial = %{relations: %{}, rowsets: %{}, types: %{}, extension: false}

    rows
    |> Enum.reduce_while(initial, fn row, acc ->
      case decode_row(row, acc) do
        :deep -> {:halt, :deep}
        acc -> {:cont, acc}
      end
    end)
    |> case do
      :deep ->
        refuse(
          :domain_chain_too_deep,
          "a column is a domain chain too deep to resolve its base type"
        )

      decoded ->
        {:ok, %{decoded | rowsets: finish_rowsets(decoded.rowsets)}}
    end
  end

  defp decode_row(["deep" | _], _acc), do: :deep

  defp decode_row(["col", relation, column, oid, type, _], acc) do
    acc
    |> Map.update!(:types, fn types ->
      Map.update(types, relation, %{column => oid}, &Map.put(&1, column, oid))
    end)
    |> update_column(relation, column, &Map.put(&1, :type, &1.type || type))
  end

  defp decode_row(["fun", _, name, _, state, _], acc) do
    state = if state == "known", do: :known, else: :unknown

    rowset = acc.rowsets |> Map.get(name, %{cols: %{}}) |> Map.put(:state, state)

    %{
      acc
      | extension: acc.extension or state == :unknown,
        rowsets: Map.put(acc.rowsets, name, rowset)
    }
  end

  defp decode_row(["fcol", position, name, _, state, _], acc) do
    update_rowset_column(acc, name, position, fn entry ->
      case state do
        "unknown" ->
          %{entry | unknown: true}

        <<"ext:", category::binary-size(1), ":", type::binary>> ->
          %{entry | type: type, category: category}

        _ ->
          entry
      end
    end)
  end

  # After its fcol row or before: the type fills in when the rowset finishes.
  defp decode_row(["fop", position, name, op, schema, _], acc) do
    update_rowset_column(acc, name, position, fn entry ->
      %{entry | ops: put_operator(entry.ops, op, schema, :own, :own)}
    end)
  end

  defp decode_row([kind, relation, column, name, schema, args], acc) do
    element? = String.starts_with?(kind, "e")

    update = fn entry ->
      case String.trim_leading(kind, "e") do
        "typ" -> %{entry | type: name, category: schema}
        "op" -> %{entry | ops: put_operator(entry.ops, name, schema, hd(args), List.last(args))}
        "fn" -> %{entry | fns: put_function(entry.fns, name, schema, args)}
      end
    end

    acc
    |> Map.put(:extension, true)
    |> update_column(relation, column, fn entry ->
      if element?,
        do: %{entry | elem: update.(entry.elem || new_entry())},
        else: update.(entry)
    end)
  end

  defp put_operator(ops, name, schema, left, right) do
    operator = %{schema: quote_identifier(schema), left: left, right: right}
    Map.update(ops, name, [operator], &[operator | &1])
  end

  defp own_sides(entry) do
    ops =
      Map.new(entry.ops, fn {name, operators} ->
        {name, Enum.map(operators, &%{&1 | left: entry.type, right: entry.type})}
      end)

    %{entry | ops: ops}
  end

  defp put_function(fns, name, schema, args) do
    {schema, self?} =
      case String.split(schema, "|self") do
        [schema, ""] -> {schema, true}
        _ -> {schema, false}
      end

    function = %{schema: schema, self: self?, args: args, nargs: length(args)}
    Map.update(fns, name, [function], &[function | &1])
  end

  defp new_entry, do: %{ops: %{}, fns: %{}, type: nil, category: nil, elem: nil, unknown: false}

  defp update_column(acc, relation, column, fun) do
    key = relation_key(relation)
    columns = Map.get(acc.relations, key, %{})
    entry = fun.(Map.get(columns, column, new_entry()))
    columns = Map.put(columns, column, entry)
    %{acc | relations: Map.put(acc.relations, key, columns)}
  end

  defp update_rowset_column(acc, name, position, fun) do
    rowset = Map.get(acc.rowsets, name, %{cols: %{}})
    index = String.to_integer(position)
    entry = fun.(Map.get(rowset.cols, index, new_entry()))
    rowset = %{rowset | cols: Map.put(rowset.cols, index, entry)}
    %{acc | rowsets: Map.put(acc.rowsets, name, rowset)}
  end

  defp finish_rowsets(rowsets) do
    Map.new(rowsets, fn {name, rowset} ->
      cols =
        if map_size(rowset.cols) == 0,
          do: nil,
          else: rowset.cols |> Enum.sort() |> Enum.map(&own_sides(elem(&1, 1)))

      {name, %{state: Map.get(rowset, :state, :unknown), cols: cols}}
    end)
  end

  # "schema"."relation" as written => the qualifier's schema.relation key.
  defp relation_key(relation) do
    relation
    |> Qualify.tokenize()
    |> Enum.reject(&(&1 == {:punct, "."}))
    |> Enum.map_join(".", fn
      {:qid, text} -> text |> String.slice(1..-2//1) |> String.replace("\"\"", "\"")
      {_kind, text} -> String.downcase(text)
    end)
  end

  # R49a: what the catalog said at the snapshot against what the parser
  # sees now, for the columns the statement names.
  defp verify_types(_execute, _tokens, _types, "read committed"), do: :ok

  defp verify_types(execute, tokens, types, _isolation) do
    named =
      tokens
      |> Enum.flat_map(fn
        {:qid, text} -> [text |> String.slice(1..-2//1) |> String.replace("\"\"", "\"")]
        {:id, text} -> [String.downcase(text)]
        _ -> []
      end)
      |> MapSet.new()

    pairs =
      for {relation, columns} <- Enum.sort(types),
          {column, oid} <- Enum.sort(columns),
          MapSet.member?(named, column),
          do: {relation, column, oid}

    if pairs == [] do
      :ok
    else
      select =
        Enum.map_join(pairs, ", ", fn {relation, column, _oid} ->
          "CAST(pg_catalog.pg_typeof((CAST(NULL AS #{relation})).#{quote_identifier(column)}) AS pg_catalog.oid)"
        end)

      expected = Enum.map(pairs, fn {_, _, oid} -> String.to_integer(oid) end)

      # A fresh text each time: a cached prepared statement would keep the
      # type it was parsed with (a rowtype cast holds no plan dependency).
      nonce = Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)

      case execute.("SELECT ARRAY[#{select}] AS selecto_guard_column_types /* #{nonce} */", []) do
        {:ok, %{rows: [[^expected]]}} ->
          :ok

        _other ->
          refuse(
            :catalog_changed,
            "a relation's column types changed after the transaction's snapshot; they cannot be read in this transaction"
          )
      end
    end
  end

  defp cast_facts(_execute, casts, _extension_types) when map_size(casts) == 0, do: {:ok, %{}}
  defp cast_facts(_execute, _casts, false), do: {:ok, %{}}

  defp cast_facts(execute, casts, true) do
    keys = casts |> Map.keys() |> Enum.sort()

    case execute.(@casts_sql, [keys, Enum.map(keys, &Map.fetch!(casts, &1))]) do
      {:ok, %{rows: rows}} when is_list(rows) ->
        {:ok,
         Enum.reduce(rows, %{}, fn
           ["typ", key, name, category, _], acc ->
             entry = Map.get(acc, key, new_entry())
             Map.put(acc, key, %{entry | type: name, category: category})

           ["op", key, op, schema, [left, right]], acc ->
             entry = Map.get(acc, key, new_entry())
             Map.put(acc, key, %{entry | ops: put_operator(entry.ops, op, schema, left, right)})
         end)}

      _other ->
        unavailable()
    end
  end

  @doc "Whatever in `sql` would still resolve through the session (R47, R48)."
  def violations(sql), do: sql |> IO.iodata_to_binary() |> Qualify.lint()

  defp quote_identifier(name), do: "\"" <> String.replace(name, "\"", "\"\"") <> "\""

  defp unavailable,
    do: refuse(:function_lookup_unavailable, "cannot read the catalog facts of a statement")

  defp refuse(reason, message, found \\ []) do
    {:error,
     Error.new(:query_enforcement, "PostgreSQL refuses a statement: " <> message,
       details:
         %{code: :query_rule_not_evaluable, reason: reason, adapter: :postgresql}
         |> then(
           &if(found == [], do: &1, else: Map.put(&1, :found, found |> Enum.take(5) |> inspect()))
         )
     )}
  end
end
