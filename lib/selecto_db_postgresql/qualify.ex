defmodule SelectoDBPostgreSQL.Qualify do
  @moduledoc false

  # R46 FINAL-2 / R47 / R49c / R50: the one emitter every generated
  # PostgreSQL statement passes through before it runs. PostgreSQL resolves
  # an unqualified operator or function by argument types across the whole
  # search_path and prefers an exact match over a coercion whatever the
  # schema order, so a user public.=(varchar, varchar) beats pg_catalog's
  # text = text for a varchar column and public.lower(varchar) beats
  # pg_catalog.lower(text). `qualify/2` therefore rewrites the statement so
  # nothing resolves through the session:
  #
  #   - every binary and prefix operator becomes OPERATOR(pg_catalog.op), or
  #     OPERATOR(<ext>.op) when PostgreSQL's own resolution would pick an
  #     extension member type's operator (both operands of that type, or one
  #     of it and the other untyped). Both operands are parenthesised as
  #     PostgreSQL's precedence groups them: OPERATOR() binds like a user
  #     operator, so the grouping is made explicit;
  #   - the operands of an extension-schema operator or function (a schema
  #     that is commonly writable) are CAST to the member's exact argument
  #     type, so nothing planted beside it over a domain can be chosen (R50);
  #   - IN lists, BETWEEN, LIKE/ILIKE, SIMILAR TO, IS [NOT] DISTINCT FROM,
  #     NULLIF and simple CASE, whose operator PostgreSQL looks up by name,
  #     are written with explicit operators;
  #   - every function is schema-qualified: pg_catalog's, or the function an
  #     extension defines for its first argument's type (citext's max),
  #     or the one candidate among the pinned schemas (`candidates`). Anything
  #     else is refused;
  #   - an operand that may have an extension type but cannot be resolved
  #     here is refused (`operand_type_unknown`), never defaulted to
  #     pg_catalog's text operators (R49c).
  #
  # Operators already written OPERATOR(...) pass through as written.
  # `lint/1` reports whatever would still resolve by name. The catalog facts
  # come from SelectoDBPostgreSQL.ExplicitSQL.

  @keywords ~w(AND OR NOT IN IS EXISTS ANY ALL SOME ARRAY ROW VALUES CAST EXTRACT TRIM SUBSTRING
               POSITION OVERLAY COALESCE GREATEST LEAST CASE WHEN THEN ELSE END OVER FILTER WITHIN
               SELECT FROM WHERE JOIN ON USING AS INTO UPDATE SET DELETE INSERT RETURNING WITH
               RECURSIVE LATERAL UNION INTERSECT EXCEPT ORDER BY GROUP HAVING LIMIT OFFSET DISTINCT
               CONFLICT DO NOTHING ROWS RANGE GROUPS PARTITION GROUPING SETS ROLLUP CUBE FOR SHARE
               NOWAIT SKIP LOCKED ONLY TABLE NATURAL LEFT RIGHT INNER OUTER FULL CROSS INTERVAL
               ZONE AT COLLATE VARIADIC ORDINALITY DESC ASC NULLS FIRST LAST FETCH NEXT OPERATOR
               ESCAPE BETWEEN LIKE ILIKE SIMILAR TO NULLIF UNBOUNDED PRECEDING FOLLOWING EXCLUDE
               TIES OTHERS MATERIALIZED NO KEY CHARACTER VARYING DOUBLE PRECISION WITHOUT DECLARE
               CURSOR SCROLL HOLD SAVEPOINT RELEASE ROLLBACK BEGIN COMMIT LOCK MODE EXCLUSIVE
               SYMMETRIC ASYMMETRIC LEADING TRAILING BOTH PLACING NORMALIZE DEFAULT WINDOW MERGE
               MATCHED CURRENT MERGE_ACTION EXPLAIN)
            |> MapSet.new()

  @constants ~w(TRUE FALSE NULL CURRENT_DATE CURRENT_TIMESTAMP CURRENT_TIME LOCALTIMESTAMP
                LOCALTIME CURRENT_USER SESSION_USER CURRENT_ROLE)
             |> MapSet.new()

  @typed_literal ~w(INTERVAL DATE TIME TIMESTAMP TIMESTAMPTZ)

  # Keyword constructs that read as a call: their arguments are qualified
  # and they keep the spelling PostgreSQL's grammar gives them.
  @call_keywords ~w(EXISTS ARRAY ANY ALL SOME EXTRACT COALESCE GREATEST LEAST ROW TRIM SUBSTRING
                    POSITION OVERLAY NORMALIZE GROUPING MERGE_ACTION)

  # These return a type of their own; the others take their arguments'.
  @known_result ~w(EXISTS EXTRACT TRIM SUBSTRING POSITION OVERLAY NORMALIZE GROUPING MERGE_ACTION)

  # Words of @keywords that pg_catalog also defines as functions.
  @function_words ~w(LEFT RIGHT MODE)

  # Words of @keywords PostgreSQL does not reserve.
  @soft_words ~w(KEY MODE FIRST LAST NEXT ZONE SHARE SKIP LOCKED HOLD TIES OTHERS RANGE ROWS GROUPS)

  @comparison ~w(< > = <= >= <> !=)

  @boolean_op ~w(= <> != < > <= >= ~~ ~~* !~~ !~~* ~ ~* !~ !~* @> <@ && ^@ ? ?| ?& @? @@)

  @system_column ~w(tableoid ctid xmin xmax cmin cmax)

  @builtin_type ~w(text varchar character char bpchar name integer int int2 int4 int8 smallint
                   bigint numeric decimal real float4 float8 boolean bool date time timetz
                   timestamp timestamptz interval json jsonb uuid bytea inet cidr macaddr money
                   oid xml)
                |> Enum.flat_map(&[&1, &1 <> "[]"])
                |> Kernel.++([
                  "character varying",
                  "double precision",
                  "timestamp with time zone",
                  "timestamp without time zone",
                  "time with time zone",
                  "time without time zone"
                ])
                |> MapSet.new()

  @op_chars ~c"+-*/<>=~!@#%^&|`?"

  # ---- Tokens ------------------------------------------------------------

  @doc "The significant tokens of `sql`: `{kind, text}`, kind :str, :qid, :param, :num, :id, :op, :punct."
  def tokenize(sql) when is_binary(sql), do: scan(sql, [])
  def tokenize(sql), do: sql |> IO.iodata_to_binary() |> tokenize()

  defp scan(<<>>, acc), do: Enum.reverse(acc)

  # A newline between two string constants continues the first.
  defp scan(<<?\n, rest::binary>>, [{:str, _} = text | _] = acc) do
    case String.trim_leading(rest) do
      <<?', tail::binary>> ->
        {more, tail} = take_quoted(tail, ?', [])
        [_ | previous] = acc
        joined = binary_part(elem(text, 1), 0, byte_size(elem(text, 1)) - 1) <> more
        scan(tail, [{:str, joined} | previous])

      _ ->
        scan(rest, acc)
    end
  end

  defp scan(<<c, rest::binary>>, acc) when c in [?\s, ?\t, ?\n, ?\r, ?\f], do: scan(rest, acc)

  defp scan(<<"--", rest::binary>>, acc) do
    case :binary.match(rest, "\n") do
      {at, 1} -> rest |> binary_part(at, byte_size(rest) - at) |> scan(acc)
      :nomatch -> scan(<<>>, acc)
    end
  end

  defp scan(<<"/*", rest::binary>>, acc), do: rest |> skip_comment(1) |> scan(acc)

  defp scan(<<u, ?&, ?', rest::binary>>, acc) when u in [?U, ?u] do
    {text, tail} = take_quoted(rest, ?', [])
    scan(tail, [{:str, <<u, ?&, ?'>> <> text} | acc])
  end

  defp scan(<<prefix, ?', rest::binary>>, acc) when prefix in [?E, ?e] do
    {text, tail} = take_escape_string(rest, [])
    scan(tail, [{:str, <<prefix, ?'>> <> text} | acc])
  end

  defp scan(<<prefix, ?', rest::binary>>, acc) when prefix in [?B, ?b, ?X, ?x, ?N, ?n] do
    {text, tail} = take_quoted(rest, ?', [])
    scan(tail, [{:str, <<prefix, ?'>> <> text} | acc])
  end

  defp scan(<<?', rest::binary>>, acc) do
    {text, tail} = take_quoted(rest, ?', [])
    scan(tail, [{:str, "'" <> text} | acc])
  end

  defp scan(<<?", rest::binary>>, acc) do
    {text, tail} = take_quoted(rest, ?", [])
    scan(tail, [{:qid, "\"" <> text} | acc])
  end

  defp scan(<<?$, rest::binary>> = sql, acc) do
    case rest do
      <<d, _::binary>> when d in ?0..?9 ->
        {digits, tail} = take_while(rest, &(&1 in ?0..?9))
        scan(tail, [{:param, "$" <> digits} | acc])

      _ ->
        scan_dollar(sql, rest, acc)
    end
  end

  defp scan(<<c, _::binary>> = sql, acc) when c in ?0..?9, do: scan_number(sql, acc)

  defp scan(<<?., d, _::binary>> = sql, acc) when d in ?0..?9, do: scan_number(sql, acc)

  defp scan(<<c, _::binary>> = sql, acc)
       when c in ?a..?z or c in ?A..?Z or c == ?_ or c >= 128 do
    {word, tail} = take_while(sql, &word_char?/1)
    scan(tail, [{:id, word} | acc])
  end

  defp scan(<<"::", rest::binary>>, acc), do: scan(rest, [{:punct, "::"} | acc])
  defp scan(<<"=>", rest::binary>>, acc), do: scan(rest, [{:punct, "=>"} | acc])

  defp scan(<<c, _::binary>> = sql, acc) when c in @op_chars do
    {op, _tail} = take_while(sql, &(&1 in @op_chars))
    op = trim_operator(op)
    scan(binary_part(sql, byte_size(op), byte_size(sql) - byte_size(op)), [{:op, op} | acc])
  end

  defp scan(<<c, rest::binary>>, acc) when c in ~c"()[],.;:",
    do: scan(rest, [{:punct, <<c>>} | acc])

  defp scan(_sql, _acc), do: refuse(:sql_unqualifiable, "generated SQL could not be tokenized")

  defp scan_dollar(sql, rest, acc) do
    {tag, tail} = take_while(rest, &(word_char?(&1) and &1 != ?$))

    with <<?$, body::binary>> <- tail,
         delimiter = "$" <> tag <> "$",
         {at, _} <- :binary.match(body, delimiter) do
      size = byte_size(delimiter) * 2 + at
      text = binary_part(sql, 0, size)
      scan(binary_part(sql, size, byte_size(sql) - size), [{:str, text} | acc])
    else
      _ -> refuse(:sql_unqualifiable, "generated SQL could not be tokenized")
    end
  end

  defp scan_number(sql, acc) do
    case Regex.run(~r/\A(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?/, sql) do
      [number] ->
        size = byte_size(number)
        scan(binary_part(sql, size, byte_size(sql) - size), [{:num, number} | acc])
    end
  end

  defp word_char?(c),
    do: c in ?a..?z or c in ?A..?Z or c in ?0..?9 or c == ?_ or c == ?$ or c >= 128

  defp take_while(binary, fun), do: take_while(binary, fun, 0)

  defp take_while(binary, fun, n) do
    if n < byte_size(binary) and fun.(:binary.at(binary, n)) do
      take_while(binary, fun, n + 1)
    else
      {binary_part(binary, 0, n), binary_part(binary, n, byte_size(binary) - n)}
    end
  end

  # PostgreSQL's lexer: a multi-character operator ends in + or - only if
  # it holds one of ~ ! @ # % ^ & | ` ?. A comment start also ends it.
  defp trim_operator(op) do
    op =
      case {:binary.match(op, "/*"), :binary.match(op, "--")} do
        {{at, _}, _} when at > 0 -> binary_part(op, 0, at)
        {_, {at, _}} when at > 0 -> binary_part(op, 0, at)
        _ -> op
      end

    if byte_size(op) > 1 and String.ends_with?(op, ["+", "-"]) and
         not String.contains?(op, ~w(~ ! @ # % ^ & | ` ?)),
       do: op |> binary_part(0, byte_size(op) - 1) |> trim_operator(),
       else: op
  end

  defp skip_comment(<<>>, _depth), do: <<>>
  defp skip_comment(rest, 0), do: rest
  defp skip_comment(<<"/*", rest::binary>>, depth), do: skip_comment(rest, depth + 1)
  defp skip_comment(<<"*/", rest::binary>>, depth), do: skip_comment(rest, depth - 1)
  defp skip_comment(<<_, rest::binary>>, depth), do: skip_comment(rest, depth)

  defp take_quoted(<<q, q, rest::binary>>, q, acc), do: take_quoted(rest, q, [<<q, q>> | acc])

  defp take_quoted(<<q, rest::binary>>, q, acc),
    do: {IO.iodata_to_binary(Enum.reverse([<<q>> | acc])), rest}

  defp take_quoted(<<c, rest::binary>>, q, acc), do: take_quoted(rest, q, [<<c>> | acc])
  defp take_quoted(<<>>, _q, _acc), do: refuse(:sql_unqualifiable, "unterminated quoted text")

  defp take_escape_string(<<?\\, c, rest::binary>>, acc),
    do: take_escape_string(rest, [<<?\\, c>> | acc])

  defp take_escape_string(<<?', ?', rest::binary>>, acc),
    do: take_escape_string(rest, ["''" | acc])

  defp take_escape_string(<<?', rest::binary>>, acc),
    do: {IO.iodata_to_binary(Enum.reverse(["'" | acc])), rest}

  defp take_escape_string(<<c, rest::binary>>, acc),
    do: take_escape_string(rest, [<<c>> | acc])

  defp take_escape_string(<<>>, _acc), do: refuse(:sql_unqualifiable, "unterminated quoted text")

  # ---- Entry points ------------------------------------------------------

  @doc """
  `{:ok, sql}` with every operator and function of the statement explicit,
  or `{:error, reason, message}`. `facts`:

    * `:functions`: lower-case name => `%{polymorphic: boolean, candidates: [candidate]}`
      for every unqualified function the statement calls (and `{schema, name}`
      for a rowset function it names by schema);
    * `:extension`: whether any operand can have an extension type;
    * `:relations`: lower-case `schema.relation` => column => entry;
    * `:rowsets`: function name => `%{state: :known | :unknown, cols: [entry]}`;
    * `:casts`: lower-case `schema.type` => entry.

  An entry: `%{ops: %{op => schema}, type: name, fns: %{name => [fn]}, elem:
  entry, unknown: boolean}`. A candidate: `%{schema, nargs, defaults, args,
  variadic, member, extension}`.
  """
  def qualify(tokens, facts) when is_list(tokens) do
    toks = List.to_tuple(tokens)

    st = %{
      toks: toks,
      limit: tuple_size(toks),
      pos: 0,
      facts: facts,
      scopes: nil,
      lone: nil,
      refs: [],
      consumed: MapSet.new(),
      next_ref: 0,
      computing: 0
    }

    st = if Map.get(facts, :extension), do: %{st | scopes: scopes(toks)}, else: st
    {sql, st} = sequence(st, nil)

    if st.pos < st.limit,
      do: refuse(:sql_unqualifiable, "generated SQL has unbalanced parentheses"),
      else: {:ok, sql}
  catch
    {:qualify_refuse, reason, message} -> {:error, reason, message}
  end

  def qualify(sql, facts), do: sql |> tokenize() |> qualify(facts)

  # ---- Sequences ---------------------------------------------------------

  defp tok(st, i) when i >= 0 and i < st.limit, do: elem(st.toks, i)
  defp tok(_st, _i), do: nil

  defp upper({:id, text}), do: String.upcase(text)
  defp upper(_token), do: ""

  defp text({_kind, text}), do: text
  defp text(nil), do: ""

  defp punct?(st, i, text), do: tok(st, i) == {:punct, text}

  defp advance(st, n \\ 1), do: %{st | pos: st.pos + n}

  defp expect(st, text) do
    if punct?(st, st.pos, text),
      do: advance(st),
      else: refuse(:sql_unqualifiable, "generated SQL has unbalanced parentheses")
  end

  defp expect_word(st, word) do
    if upper(tok(st, st.pos)) == word,
      do: advance(st),
      else: refuse(:sql_unqualifiable, "generated SQL could not be parsed")
  end

  # A sequence of items up to (not including) the closing token (`)`, `]`)
  # or the limit. Expression runs are qualified; the rest copied.
  defp sequence(st, close) do
    seq(st, close, %{parts: [], run: [], set_list: false, lone: nil})
  end

  defp flush(st, %{run: []} = acc), do: {st, acc}

  defp flush(st, %{run: run, parts: parts} = acc) do
    lone =
      case {run, parts} do
        {[%{kind: :atom} = atom], []} -> atom
        _ -> nil
      end

    {sql, st} = emit_run(st, run)
    {st, %{acc | run: [], parts: [sql | parts], lone: lone}}
  end

  defp finish(st, acc) do
    {st, acc} = flush(st, acc)
    lone = if length(acc.parts) == 1, do: acc.lone, else: nil
    {acc.parts |> Enum.reverse() |> Enum.join(" "), %{st | lone: lone}}
  end

  defp seq(st, close, acc) do
    token = tok(st, st.pos)
    up = upper(token)

    cond do
      token == nil ->
        finish(st, acc)

      close != nil and token == {:punct, close} ->
        finish(st, acc)

      up == "SET" ->
        {st, acc} = flush(st, acc)
        seq(advance(st), close, %{acc | parts: [text(token) | acc.parts], set_list: true})

      acc.set_list and up in ~w(WHERE RETURNING FROM WHEN) ->
        seq(st, close, %{acc | set_list: false})

      # An assignment `col = expr` in an UPDATE ... SET list.
      acc.set_list and token == {:op, "="} and assignment_target?(acc) ->
        {st, acc} = flush(st, acc)
        seq(advance(st), close, %{acc | parts: ["=" | acc.parts]})

      # SELECT *, COUNT(*): a star with nothing to its left is no operator.
      token == {:op, "*"} and star_position?(acc.run) ->
        seq(advance(st), close, %{acc | run: acc.run ++ [%{kind: :atom, sql: "*"}]})

      operator_at?(st, st.pos) ->
        {op, st} = operator(st)
        seq(st, close, %{acc | run: acc.run ++ [op]})

      true ->
        seq_operand(st, close, acc, token)
    end
  end

  defp assignment_target?(%{run: [%{kind: :atom, column: %{}}], parts: parts}) do
    parts == [] or hd(parts) == "," or String.upcase(hd(parts)) == "SET"
  end

  defp assignment_target?(_acc), do: false

  defp star_position?(run), do: run == [] or List.last(run).kind == :op

  defp operator_at?(st, i) do
    case tok(st, i) do
      {:op, _} -> true
      {:id, _} = t -> upper(t) == "OPERATOR" and punct?(st, i + 1, "(")
      _ -> false
    end
  end

  defp seq_operand(st, close, acc, token) do
    construct =
      if acc.run != [] and List.last(acc.run).kind == :atom and match?({:id, _}, token),
        do: construct(st, acc.run),
        else: nil

    case construct do
      {atom, run, st} ->
        seq(st, close, %{acc | run: run ++ [atom]})

      nil ->
        case atom(st) do
          {nil, st} ->
            seq_boundary(st, close, acc, token)

          {atom, st} ->
            # Two atoms in a row (an alias after an expression): a new run.
            {st, acc} =
              if acc.run != [] and List.last(acc.run).kind == :atom,
                do: flush(st, acc),
                else: {st, acc}

            seq(st, close, %{acc | run: acc.run ++ [atom]})
        end
    end
  end

  # A boundary: a keyword, comma, or other punctuation.
  defp seq_boundary(st, close, acc, token) do
    {st, acc} = flush(st, acc)

    case token do
      {:punct, "["} ->
        {inner, st} = sequence(advance(st), "]")
        st = expect(st, "]")
        seq(st, close, %{acc | parts: ["[#{inner}]" | acc.parts]})

      {:punct, p} when p in [")", "]"] ->
        refuse(:sql_unqualifiable, "generated SQL has unbalanced parentheses")

      _ ->
        seq(advance(st), close, %{acc | parts: [text(token) | acc.parts]})
    end
  end

  # ---- Constructs resolved by name ---------------------------------------

  # The operand of a construct that follows a run: its trailing expression
  # above comparison precedence (IN, BETWEEN and LIKE bind tighter than
  # comparisons, looser than every other operator).
  defp take_operand(run) do
    items = List.to_tuple(run)

    j =
      Enum.find(
        (tuple_size(items) - 1)..0//-1,
        -1,
        fn j ->
          item = elem(items, j)

          item.kind == :op and not Map.get(item, :explicit, false) and rank(item.op) == 2 and
            j > 0 and elem(items, j - 1).kind == :atom
        end
      )

    {head, operand} = Enum.split(run, j + 1)
    {tree, _i} = climb(List.to_tuple(operand), 0, 0)
    {tree, head}
  end

  # One expression run from the tokens up to a boundary, or `stop`.
  defp read_run(st, stop \\ nil) do
    {items, st} = run_items(st, stop, [])

    if items == [], do: refuse(:sql_unqualifiable, "generated SQL could not be parsed")
    {tree, _i} = climb(List.to_tuple(items), 0, 0)
    {tree, st}
  end

  defp run_items(st, stop, items) do
    token = tok(st, st.pos)

    cond do
      token == nil ->
        {items, st}

      stop != nil and upper(token) == stop ->
        {items, st}

      token == {:op, "*"} and star_position?(items) ->
        run_items(advance(st), stop, items ++ [%{kind: :atom, sql: "*"}])

      operator_at?(st, st.pos) ->
        {op, st} = operator(st)
        run_items(st, stop, items ++ [op])

      items != [] and List.last(items).kind == :atom ->
        {items, st}

      true ->
        case atom(st) do
          {nil, st} -> {items, st}
          {atom, st} -> run_items(st, stop, items ++ [atom])
        end
    end
  end

  defp construct(st, run) do
    at = st.pos
    up = upper(tok(st, at))

    {negate, at, up} =
      if up == "NOT" and upper(tok(st, at + 1)) in ~w(IN BETWEEN LIKE ILIKE SIMILAR),
        do: {true, at + 1, upper(tok(st, at + 1))},
        else: {false, at, up}

    cond do
      up == "IN" and punct?(st, at + 1, "(") -> construct_in(st, run, at, negate)
      up == "BETWEEN" -> construct_between(st, run, at, negate)
      up in ["LIKE", "ILIKE"] -> construct_like(st, run, at, up, negate)
      up == "SIMILAR" and upper(tok(st, at + 1)) == "TO" -> construct_similar(st, run, at, negate)
      up == "IS" -> construct_distinct(st, run, at)
      true -> nil
    end
  end

  defp construct_in(st, run, at, negate) do
    {operand, head} = take_operand(run)
    {left, st} = render(st, operand)
    inner = upper(tok(st, at + 2))
    st = %{st | pos: at + 2}

    if inner in ~w(SELECT WITH VALUES) do
      open = at + 1
      {query, st} = sequence(st, ")")
      st = expect(st, ")")
      op = %{kind: :op, op: if(negate, do: "<>", else: "=")}
      right = %{atom: %{sql: query, subquery: open}}
      {op_sql, cast, st} = op_sql(st, op, operand, right)

      sql =
        "((#{left_cast(left, cast)}) #{op_sql} #{if negate, do: "ALL", else: "ANY"} " <>
          "(#{cast_subquery(query, cast)}))"

      {%{kind: :atom, sql: sql}, head, st}
    else
      {terms, st} = in_terms(st, left, operand, [])
      st = expect(st, ")")
      sql = "(" <> Enum.join(terms, " OR ") <> ")"
      sql = if negate, do: "(NOT #{sql})", else: sql
      {%{kind: :atom, sql: sql}, head, st}
    end
  end

  defp in_terms(st, left, operand, acc) do
    {element, st} = read_run(st)
    {right, st} = render(st, element)
    {op, cast, st} = op_sql(st, %{kind: :op, op: "="}, operand, element)
    term = "((#{left_cast(left, cast)}) #{op} (#{right_cast(right, cast)}))"

    if punct?(st, st.pos, ","),
      do: in_terms(advance(st), left, operand, [term | acc]),
      else: {Enum.reverse([term | acc]), st}
  end

  defp construct_between(st, run, at, negate) do
    if upper(tok(st, at + 1)) in ~w(SYMMETRIC ASYMMETRIC),
      do: refuse(:sql_unqualifiable, "BETWEEN SYMMETRIC is not generated")

    {operand, head} = take_operand(run)
    {left, st} = render(st, operand)
    st = %{st | pos: at + 1}
    {low, st} = read_run(st, "AND")
    st = expect_word(st, "AND")
    {high, st} = read_run(st)
    {low_sql, st} = render(st, low)
    {high_sql, st} = render(st, high)
    {ge, cast, st} = op_sql(st, %{kind: :op, op: ">="}, operand, low)
    first = "(#{left_cast(left, cast)}) #{ge} (#{right_cast(low_sql, cast)})"
    {le, cast, st} = op_sql(st, %{kind: :op, op: "<="}, operand, high)
    sql = "(#{first} AND (#{left_cast(left, cast)}) #{le} (#{right_cast(high_sql, cast)}))"
    sql = if negate, do: "(NOT #{sql})", else: sql
    {%{kind: :atom, sql: sql}, head, st}
  end

  defp construct_like(st, run, at, up, negate) do
    {operand, head} = take_operand(run)
    {left, st} = render(st, operand)
    st = %{st | pos: at + 1}
    {pattern_tree, st} = read_run(st, "ESCAPE")
    {pattern, st} = render(st, pattern_tree)

    {pattern, right, st} =
      if upper(tok(st, st.pos)) == "ESCAPE" do
        case atom(advance(st)) do
          {nil, _st} ->
            refuse(:sql_unqualifiable, "generated SQL could not be parsed")

          {escape, st} ->
            {"pg_catalog.like_escape(#{pattern}, #{escape.sql})", nil, st}
        end
      else
        {pattern, pattern_tree, st}
      end

    name = if(negate, do: "!", else: "") <> if(up == "LIKE", do: "~~", else: "~~*")
    {op, cast, st} = op_sql(st, %{kind: :op, op: name}, operand, right)

    {%{
       kind: :atom,
       sql: "((#{left_cast(left, cast)}) #{op} (#{right_cast(pattern, cast)}))"
     }, head, st}
  end

  defp construct_similar(st, run, at, negate) do
    {operand, head} = take_operand(run)
    {left, st} = render(st, operand)
    st = %{st | pos: at + 2}
    {pattern_tree, st} = read_run(st, "ESCAPE")
    {pattern, st} = render(st, pattern_tree)

    if upper(tok(st, st.pos)) == "ESCAPE",
      do: refuse(:sql_unqualifiable, "SIMILAR TO ... ESCAPE is not generated")

    op = if negate, do: "!~", else: "~"
    {op, _cast, st} = op_sql(st, %{kind: :op, op: op}, %{known: true}, %{known: true})
    sql = "((#{left}) #{op} (pg_catalog.similar_to_escape(#{pattern})))"
    {%{kind: :atom, sql: sql}, head, st}
  end

  defp construct_distinct(st, run, at) do
    {k, not?} =
      if upper(tok(st, at + 1)) == "NOT", do: {at + 2, true}, else: {at + 1, false}

    if upper(tok(st, k)) == "DISTINCT" and upper(tok(st, k + 1)) == "FROM" do
      {operand, head} = take_operand(run)
      {left, st} = render(st, operand)
      st = %{st | pos: k + 2}
      {right_tree, st} = read_run(st)
      {right, st} = render(st, right_tree)
      {op, cast, st} = op_sql(st, %{kind: :op, op: "="}, operand, right_tree)

      sql =
        "(COALESCE((#{left_cast(left, cast)}) #{op} (#{right_cast(right, cast)}), FALSE) " <>
          "OR ((#{left}) IS NULL AND (#{right}) IS NULL))"

      sql = if not?, do: sql, else: "(NOT #{sql})"
      {%{kind: :atom, sql: sql}, head, st}
    end
  end

  # An operator item: a bare operator token, or an explicit OPERATOR(schema.op).
  defp operator(st) do
    case tok(st, st.pos) do
      {:op, op} ->
        {%{kind: :op, op: op}, advance(st)}

      _word ->
        close = close_index(st, st.pos + 1)

        if close == nil,
          do: refuse(:sql_unqualifiable, "generated SQL has unbalanced parentheses")

        inner =
          (st.pos + 2)..(close - 1)//1
          |> Enum.map_join("", &text(tok(st, &1)))

        {schema, op} =
          case Regex.run(~r/\A(.+)\.([^.]+)\z/s, inner) do
            [_, schema, op] -> {schema, op}
            nil -> {"pg_catalog", inner}
          end

        {%{kind: :op, op: op, schema: schema, explicit: true}, %{st | pos: close + 1}}
    end
  end

  # ---- Atoms -------------------------------------------------------------

  # An atom (literal, parameter, column, function call, parenthesised group,
  # CASE ... END, CAST(...), EXISTS(...), ARRAY[...], ANY/ALL(...), typed
  # literal) with its postfix, or nil at a boundary.
  defp atom(st) do
    token = tok(st, st.pos)
    mark = length(st.refs)
    up = upper(token)

    case token do
      nil ->
        {nil, st}

      {kind, value} when kind in [:num, :str, :param] ->
        postfix(advance(st), %{sql: value}, mark)

      {:op, _} ->
        {nil, st}

      {:punct, "("} ->
        open = st.pos
        row? = row_group?(st, open)
        subquery? = upper(tok(st, open + 1)) in ~w(SELECT WITH VALUES)
        {inner, st} = sequence(advance(st), ")")
        lone = st.lone
        st = expect(st, ")")

        carried =
          cond do
            row? ->
              %{row: true}

            subquery? ->
              %{subquery: open}

            lone ->
              Map.take(lone, [
                :column,
                :known,
                :entry,
                :unknown,
                :untyped,
                :array_entry,
                :row,
                :subquery
              ])

            true ->
              %{}
          end

        postfix(st, Map.put(carried, :sql, "(#{inner})"), mark)

      {:punct, _} ->
        {nil, st}

      {:qid, _} ->
        name_atom(st, mark)

      {:id, value} ->
        cond do
          MapSet.member?(@constants, up) and not punct?(st, st.pos + 1, "(") ->
            postfix(advance(st), %{sql: value}, mark)

          up in @typed_literal and match?({:str, _}, tok(st, st.pos + 1)) ->
            postfix(advance(st, 2), %{sql: "#{value} #{text(tok(st, st.pos + 1))}"}, mark)

          MapSet.member?(@constants, up) ->
            call_atom(st, mark, up, false)

          up == "CASE" ->
            case_atom(st, mark)

          up == "CAST" and punct?(st, st.pos + 1, "(") ->
            cast_atom(st, mark)

          up == "NULLIF" and punct?(st, st.pos + 1, "(") ->
            nullif_atom(st, mark)

          # The comma form of these calls resolves by name (only the
          # keyword form is pg_catalog's by grammar).
          up in ["SUBSTRING", "OVERLAY"] and comma_call?(st) ->
            call_atom(st, mark, up, true)

          up == "POSITION" and punct?(st, st.pos + 1, "(") ->
            position_atom(st, mark)

          # left(x, 2) and mode() are calls; LEFT JOIN is not.
          up in @function_words and punct?(st, st.pos + 1, "(") ->
            name_atom(st, mark)

          up in @call_keywords and call_open?(st, up) ->
            call_atom(st, mark, up, false)

          # A non-reserved word as a column: key = 'a'.
          up in @soft_words and
              (operator_at?(st, st.pos + 1) or match?({:op, _}, tok(st, st.pos - 1))) ->
            name_atom(st, mark)

          MapSet.member?(@keywords, up) ->
            {nil, st}

          true ->
            name_atom(st, mark)
        end
    end
  end

  # (a, b): a row, its comparison resolved per column.
  defp row_group?(st, open) do
    case close_index(st, open) do
      nil ->
        false

      close ->
        Enum.any?(
          (open + 1)..(close - 1)//1,
          &(tok(st, &1) == {:punct, ","} and depth_zero?(st, open, &1))
        )
    end
  end

  defp call_open?(st, "ARRAY"),
    do: punct?(st, st.pos + 1, "(") or punct?(st, st.pos + 1, "[")

  defp call_open?(st, _up), do: punct?(st, st.pos + 1, "(")

  defp comma_call?(st) do
    with true <- punct?(st, st.pos + 1, "("),
         close when is_integer(close) <- close_index(st, st.pos + 1) do
      not Enum.any?((st.pos + 2)..(close - 1)//1, fn i ->
        depth_zero?(st, st.pos + 1, i) and upper(tok(st, i)) in ~w(FROM FOR PLACING SIMILAR)
      end)
    else
      _ -> false
    end
  end

  defp depth_zero?(st, open, i) do
    depth =
      Enum.reduce((open + 1)..(i - 1)//1, 0, fn k, depth ->
        case tok(st, k) do
          {:punct, p} when p in ["(", "["] -> depth + 1
          {:punct, p} when p in [")", "]"] -> depth - 1
          _ -> depth
        end
      end)

    depth == 0
  end

  # A keyword construct read as a call. A quantifier keeps what it ranges
  # over: its operator is chosen from the element type (see class_of/2).
  defp call_atom(st, mark, up, comma?) do
    open_at = st.pos + 1
    open = text(tok(st, open_at))
    close = if open == "(", do: ")", else: "]"
    word = text(tok(st, st.pos))
    subquery? = upper(tok(st, open_at + 1)) in ~w(SELECT WITH VALUES)
    {inner, st} = sequence(advance(st, 2), close)
    lone = st.lone
    st = expect(st, close)
    word = if comma?, do: "pg_catalog.#{String.downcase(word)}", else: word

    extra =
      cond do
        up in ~w(ANY ALL SOME) ->
          %{quantified: %{word: word, inner: inner, subquery: subquery? && open_at, lone: lone}}

        up == "ROW" ->
          %{row: true}

        true ->
          %{}
      end

    atom = Map.merge(extra, %{sql: "#{word}#{open}#{inner}#{close}", known: up in @known_result})
    postfix(st, atom, mark)
  end

  # POSITION(a IN b): IN here is syntax, whatever follows it.
  defp position_atom(st, mark) do
    open = st.pos + 1
    close = close_index(st, open)
    if close == nil, do: refuse(:sql_unqualifiable, "generated SQL has unbalanced parentheses")

    in_at =
      Enum.find((open + 1)..(close - 1)//1, fn i ->
        upper(tok(st, i)) == "IN" and depth_zero?(st, open, i)
      end)

    if in_at == nil, do: refuse(:sql_unqualifiable, "generated SQL could not be parsed")
    {[left, right], st} = arguments(st, [{open + 1, in_at}, {in_at + 1, close}])

    # PostgreSQL's grammar turns POSITION(a IN b) into position(b, a).
    postfix(
      %{st | pos: close + 1},
      %{sql: "pg_catalog.position(#{right.sql}, #{left.sql})", known: true},
      mark
    )
  end

  # CAST(expression AS type): a cast to an extension type (or a domain over
  # one) has that type, and so its operators.
  defp cast_atom(st, mark) do
    open = st.pos + 1
    close = close_index(st, open)
    if close == nil, do: refuse(:sql_unqualifiable, "generated SQL has unbalanced parentheses")

    as_at =
      Enum.find((close - 1)..(open + 1)//-1, fn i ->
        upper(tok(st, i)) == "AS" and depth_zero?(st, open, i)
      end)

    if as_at == nil, do: refuse(:sql_unqualifiable, "generated SQL could not be parsed")

    saved = st.limit
    {inner, st} = sequence(%{st | pos: open + 1, limit: as_at}, nil)
    st = %{st | limit: saved}
    type_tokens = Enum.map((as_at + 1)..(close - 1)//1, &tok(st, &1))
    type = render_type(type_tokens)
    entry = cast_entry(st, type_tokens)
    st = %{st | pos: close + 1}
    word = text(tok(st, open - 1))

    postfix(
      st,
      Map.put(type_facts(st, type_tokens, entry), :sql, "#{word}(#{inner} AS #{type})"),
      mark
    )
  end

  # What a cast's target says of its result: an extension type's entry, an
  # array of one, a type no extension defines, or (a bare name that is no
  # built-in, while an extension type can reach the statement) nothing known.
  defp type_facts(_st, tokens, %{} = entry) do
    if List.last(tokens) == {:punct, "]"}, do: %{array_entry: entry}, else: %{entry: entry}
  end

  defp type_facts(st, tokens, nil) do
    if st.scopes != nil and open_type?(tokens), do: %{unknown: true}, else: %{known: true}
  end

  defp open_type?(tokens) do
    tokens
    |> render_type()
    |> String.downcase()
    |> String.replace(~r/\A(?:"?pg_catalog"?\.)/, "")
    |> String.replace("\"", "")
    |> String.replace(~r/\(.*?\)/, "")
    |> String.trim()
    |> then(&(not MapSet.member?(@builtin_type, &1) and not String.starts_with?(&1, "reg")))
  end

  defp render_type(tokens) do
    tokens
    |> Enum.reduce("", fn
      {:punct, p}, acc when p in [".", "(", ")", "[", "]", ","] ->
        acc <> p

      token, "" ->
        text(token)

      token, acc ->
        if String.ends_with?(acc, [".", "("]),
          do: acc <> text(token),
          else: acc <> " " <> text(token)
    end)
  end

  defp cast_entry(st, [schema, {:punct, "."}, name | array])
       when array in [[], [{:punct, "["}, {:punct, "]"}]] do
    case Map.get(st.facts, :casts) do
      %{} = casts ->
        case Map.get(casts, String.downcase(unquote_name(schema)) <> "." <> unquote_name(name)) do
          %{type: _} = entry -> entry
          _ -> nil
        end

      _ ->
        nil
    end
  end

  defp cast_entry(_st, _tokens), do: nil

  defp nullif_atom(st, mark) do
    word_at = st.pos
    close = close_index(st, word_at + 1)
    if close == nil, do: refuse(:sql_unqualifiable, "generated SQL has unbalanced parentheses")

    st = %{st | pos: word_at + 2}
    {a, st} = read_run(st)
    st = expect(st, ",")
    {b, st} = read_run(st)
    st = expect(st, ")")
    {left, st} = render(st, a)
    {right, st} = render(st, b)
    {op, cast, st} = op_sql(st, %{kind: :op, op: "="}, a, b)

    sql =
      "(CASE WHEN (#{left_cast(left, cast)}) #{op} (#{right_cast(right, cast)}) " <>
        "THEN NULL ELSE (#{left}) END)"

    postfix(st, %{sql: sql}, mark)
  end

  defp case_atom(st, mark) do
    st = advance(st)

    if upper(tok(st, st.pos)) == "WHEN" do
      {inner, st} = searched_case(st, [])
      postfix(st, %{sql: "CASE #{inner} END"}, mark)
    else
      simple_case(st, mark)
    end
  end

  # A simple CASE x WHEN v ...: searched, with explicit operators.
  defp simple_case(st, mark) do
    {subject, st} = read_run(st, "WHEN")
    {left, st} = render(st, subject)
    {parts, st} = simple_branches(st, subject, left, [])

    {parts, st} =
      if upper(tok(st, st.pos)) == "ELSE" do
        {sql, st} = case_branch(advance(st))
        {["ELSE #{sql}" | parts], st}
      else
        {parts, st}
      end

    st = expect_word(st, "END")
    postfix(st, %{sql: "CASE #{parts |> Enum.reverse() |> Enum.join(" ")} END"}, mark)
  end

  defp simple_branches(st, subject, left, parts) do
    if upper(tok(st, st.pos)) == "WHEN" do
      {value, st} = read_run(advance(st), "THEN")
      st = expect_word(st, "THEN")
      {result, st} = case_branch(st)
      {value_sql, st} = render(st, value)
      {op, cast, st} = op_sql(st, %{kind: :op, op: "="}, subject, value)

      part =
        "WHEN (#{left_cast(left, cast)}) #{op} (#{right_cast(value_sql, cast)}) THEN #{result}"

      simple_branches(st, subject, left, [part | parts])
    else
      {parts, st}
    end
  end

  # One CASE branch's expression, up to the next WHEN/ELSE/END of this CASE.
  defp case_branch(st) do
    stop = case_stop(st, st.pos, 0, 0)
    saved = st.limit
    {sql, st} = sequence(%{st | limit: stop}, nil)
    {sql, %{st | limit: saved, pos: stop}}
  end

  defp case_stop(st, i, depth, nested) do
    token = tok(st, i)
    up = upper(token)

    cond do
      token == nil ->
        refuse(:sql_unqualifiable, "generated SQL could not be parsed")

      match?({:punct, p} when p in ["(", "["], token) ->
        case_stop(st, i + 1, depth + 1, nested)

      match?({:punct, p} when p in [")", "]"], token) ->
        case_stop(st, i + 1, depth - 1, nested)

      depth > 0 ->
        case_stop(st, i + 1, depth, nested)

      up == "CASE" ->
        case_stop(st, i + 1, depth, nested + 1)

      up == "END" and nested > 0 ->
        case_stop(st, i + 1, depth, nested - 1)

      nested == 0 and up in ~w(WHEN THEN ELSE END) ->
        i

      true ->
        case_stop(st, i + 1, depth, nested)
    end
  end

  defp searched_case(st, parts) do
    token = tok(st, st.pos)
    up = upper(token)

    cond do
      token == nil ->
        refuse(:sql_unqualifiable, "generated SQL could not be parsed")

      up == "END" ->
        {parts |> Enum.reverse() |> Enum.join(" "), advance(st)}

      up in ~w(WHEN THEN ELSE) ->
        searched_case(advance(st), [text(token) | parts])

      true ->
        {sql, st} = case_branch(st)
        searched_case(st, [sql | parts])
    end
  end

  # A name chain a.b.c, possibly a function call.
  defp name_atom(st, mark) do
    here = st.pos
    {names, st} = name_chain(%{st | pos: here + 1}, [tok(st, here)])

    cond do
      punct?(st, st.pos, "(") and column_list?(st, here, st.pos) ->
        {inner, st} = sequence(advance(st), ")")
        st = expect(st, ")")
        postfix(st, %{sql: "#{join_names(names)}(#{inner})", known: true}, mark)

      punct?(st, st.pos, "(") ->
        function_atom(st, names, here, mark)

      true ->
        {at, values} = {here, Enum.map(names, &unquote_name/1)}
        postfix(st, %{sql: join_names(names), column: %{names: values, at: at}}, mark)
    end
  end

  defp name_chain(st, names) do
    next = tok(st, st.pos + 1)

    if punct?(st, st.pos, ".") and
         (match?({kind, _} when kind in [:id, :qid], next) or next == {:op, "*"}) do
      name_chain(advance(st, 2), names ++ [next])
    else
      {names, st}
    end
  end

  defp join_names(names), do: Enum.map_join(names, ".", &text/1)

  # `name (...)` that is no call: a relation or alias's column list.
  defp column_list?(st, here, open) do
    previous = tok(st, here - 1)

    alias_position =
      case previous do
        {:id, _} = t -> upper(t) in ~w(AS INTO TABLE UPDATE) or not keyword_token?(t)
        {:qid, _} -> true
        {:punct, ")"} -> true
        _ -> false
      end

    alias_position or cte_columns?(st, open)
  end

  defp keyword_token?(token) do
    up = upper(token)
    MapSet.member?(@keywords, up) or MapSet.member?(@constants, up)
  end

  # name ( ... ) AS [NOT MATERIALIZED] ( : a CTE's column list.
  defp cte_columns?(st, open) do
    case close_index(st, open) do
      nil ->
        false

      close ->
        upper(tok(st, close + 1)) == "AS" and
          (punct?(st, close + 2, "(") or upper(tok(st, close + 2)) in ~w(NOT MATERIALIZED))
    end
  end

  # The matching close of the bracket at `open`, within the limit.
  defp close_index(st, open), do: close_index(st, open + 1, 1)

  defp close_index(st, i, depth) do
    case tok(st, i) do
      nil -> nil
      {:punct, "("} -> close_index(st, i + 1, depth + 1)
      {:punct, ")"} when depth == 1 -> i
      {:punct, ")"} -> close_index(st, i + 1, depth - 1)
      _ -> close_index(st, i + 1, depth)
    end
  end

  # A call's argument ranges, split at its top-level commas.
  defp argument_ranges(st, open, close) do
    {ranges, start, _depth} =
      Enum.reduce((open + 1)..(close - 1)//1, {[], open + 1, 0}, fn i, {ranges, start, depth} ->
        case tok(st, i) do
          {:punct, p} when p in ["(", "["] -> {ranges, start, depth + 1}
          {:punct, p} when p in [")", "]"] -> {ranges, start, depth - 1}
          {:punct, ","} when depth == 0 -> {[{start, i} | ranges], i + 1, depth}
          _ -> {ranges, start, depth}
        end
      end)

    if close == open + 1, do: [], else: Enum.reverse([{start, close} | ranges])
  end

  # Each argument qualified: `%{sql, lone}` (its lone atom, if it is one).
  defp arguments(st, ranges) do
    saved = st.limit

    {args, st} =
      Enum.map_reduce(ranges, st, fn {from, to}, st ->
        # DISTINCT or ALL before an aggregate's argument is syntax.
        {prefix, from} =
          if upper(tok(st, from)) in ~w(DISTINCT ALL),
            do: {text(tok(st, from)) <> " ", from + 1},
            else: {"", from}

        {sql, st} = sequence(%{st | pos: from, limit: to}, nil)
        {%{sql: sql, prefix: prefix, lone: st.lone}, st}
      end)

    {args, %{st | limit: saved}}
  end

  defp function_atom(st, names, here, mark) do
    open = st.pos
    close = close_index(st, open)
    if close == nil, do: refuse(:sql_unqualifiable, "generated SQL has unbalanced parentheses")

    {args, st} = arguments(st, argument_ranges(st, open, close))
    st = %{st | pos: close + 1}
    previous = tok(st, here - 1)
    refuse_open_extension_call(st, names, args, mark)
    from? = upper(previous) in ~w(FROM JOIN LATERAL) or previous == {:punct, ","}

    {call, st} =
      case names do
        [name] ->
          bare_call(st, name, args)

        [schema, name] when from? ->
          if String.downcase(unquote_name(schema)) == "pg_catalog",
            do: {plain_call(names, args), st},
            else: {schema_call(st, schema, name, args), st}

        _ ->
          {plain_call(names, args), st}
      end

    {sql, st} = call_clauses(st, call.sql)
    postfix(st, Map.put(call, :sql, sql), mark)
  end

  # A call an extension defines for a type read inside its first argument,
  # when that argument is an expression this emitter cannot type: PostgreSQL
  # may choose the extension's function. Refused, never pg_catalog's (R49c).
  defp refuse_open_extension_call(%{scopes: nil}, _names, _args, _mark), do: :ok

  defp refuse_open_extension_call(st, [name], [%{lone: lone} | _], mark)
       when is_nil(lone) or is_map_key(lone, :refs) do
    key = String.downcase(unquote_name(name))
    typed? = lone != nil and match?(%{entry: _}, class_of(st, lone))

    open? =
      st.refs
      |> Enum.drop(mark)
      |> Enum.any?(fn ref ->
        case ref_class(st, ref.column) do
          %{entry: entry} -> Map.has_key?(entry.fns, key)
          _ -> false
        end
      end)

    if open? and not typed?,
      do:
        refuse(
          :operand_type_unknown,
          "a function an extension defines over an expression that could not be typed"
        )

    :ok
  end

  defp refuse_open_extension_call(_st, _names, _args, _mark), do: :ok

  defp plain_call(names, args),
    do: %{sql: "#{join_names(names)}(#{join_args(args)})"}

  defp join_args(args), do: Enum.map_join(args, ", ", &(&1.prefix <> &1.sql))

  defp bare_call(st, name, args) do
    key = String.downcase(unquote_name(name))
    info = Map.get(Map.get(st.facts, :functions, %{}), key)
    candidates = if info, do: info.candidates, else: []
    catalog = Enum.filter(candidates, &(&1.schema == "pg_catalog"))
    other = Enum.reject(candidates, &(&1.schema == "pg_catalog"))
    count = length(args)

    extension = extension_function(st, args, key)

    cond do
      extension != nil ->
        {extension_call(st, name, args, extension), st}

      catalog != [] and (reach(catalog, count) != [] or reach(other, count) == []) ->
        known = not info.polymorphic
        {%{sql: "pg_catalog.#{text(name)}(#{join_args(args)})", known: known}, st}

      true ->
        {candidate_call(st, key, candidates, args), st}
    end
  end

  defp schema_call(st, schema, name, args) do
    key = {unquote_name(schema), unquote_name(name)}
    info = Map.get(Map.get(st.facts, :functions, %{}), key)
    candidate_call(st, elem(key, 1), (info && info.candidates) || [], args)
  end

  defp reach(candidates, count) do
    Enum.filter(candidates, fn c ->
      count >= c.nargs - c.defaults and (c.variadic != nil or count <= c.nargs)
    end)
  end

  # The candidates this call's argument count can reach (as PostgreSQL
  # filters them first), narrowed by the arguments of an extension type. An
  # installed extension's own function over one of its own types is dropped
  # while another candidate remains (untyped arguments resolve to that one:
  # citext's regexp_split_to_table); alone, it is the candidate (hstore's
  # each). Every remaining candidate pg_catalog's: the call is
  # pg_catalog-qualified. Exactly one other: its schema, with each argument
  # cast to its declared type (R50). Overloads of one extension (digest,
  # hstore, ST_DWithin): that extension's schema, every argument its
  # overloads agree on cast, PostgreSQL choosing among members only; anything
  # else beside them refuses. Else refused.
  defp candidate_call(st, name, candidates, args) do
    count = length(args)
    reached = candidates |> reach(count) |> narrow_by_arguments(st, args)

    if reached == [],
      do:
        refuse(
          :function_not_found,
          "a function the statement calls (#{name}) was not found in the pinned schemas"
        )

    plain = Enum.reject(reached, & &1.member)
    reached = narrow_members(name, reached, plain)
    catalog? = Enum.all?(reached, &(&1.schema == "pg_catalog"))

    cond do
      catalog? ->
        %{sql: "pg_catalog.#{quote_name(name)}(#{join_args(args)})", known: true}

      length(reached) == 1 ->
        [info] = reached
        cast = cast_arguments(info, args)

        Map.put(
          result_facts(st, reached),
          :sql,
          "#{quote_name(info.schema)}.#{quote_name(name)}(#{Enum.join(cast, ", ")})"
        )

      one_extension?(reached) ->
        [info | _] = reached

        Map.put(
          result_facts(st, reached),
          :sql,
          "#{quote_name(info.schema)}.#{quote_name(name)}(#{Enum.join(agreed_casts(reached, args), ", ")})"
        )

      true ->
        ambiguous()
    end
  end

  defp narrow_by_arguments(candidates, st, args) do
    args
    |> Enum.with_index()
    |> Enum.reduce(candidates, fn {arg, index}, kept ->
      type =
        case arg.lone && class_of(st, arg.lone) do
          %{entry: %{type: type}} -> type
          _ -> nil
        end

      narrowed = if type, do: Enum.filter(kept, &(Enum.at(&1.args, index) == type)), else: []
      if narrowed == [], do: kept, else: narrowed
    end)
  end

  defp one_extension?(candidates) do
    extensions = candidates |> Enum.map(&{&1.extension, &1.schema}) |> Enum.uniq()
    match?([{extension, _schema}] when extension != nil, extensions)
  end

  defp agreed_casts(candidates, args) do
    args
    |> Enum.with_index()
    |> Enum.map(fn {arg, index} ->
      case candidates |> Enum.map(&Enum.at(&1.args, index)) |> Enum.uniq() do
        [type] when is_binary(type) -> "#{arg.prefix}CAST((#{arg.sql}) AS #{type})"
        _ -> arg.prefix <> arg.sql
      end
    end)
  end

  # What the candidates return says of the call's type.
  defp result_facts(st, candidates) do
    case candidates |> Enum.map(&Map.get(&1, :ret)) |> Enum.uniq() do
      [nil] ->
        %{known: true}

      ["pg_catalog." <> _] ->
        %{known: true}

      [type] when is_binary(type) ->
        case cast_entry(st, type_tokens(type)) do
          %{} = entry -> %{entry: entry}
          nil -> %{unknown: st.scopes != nil, known: st.scopes == nil}
        end

      _several ->
        %{unknown: st.scopes != nil, known: st.scopes == nil}
    end
  end

  defp type_tokens(type), do: tokenize(type)

  defp narrow_members(_name, reached, plain) when plain == [] or length(plain) == length(reached),
    do: reached

  # The extension's own functions are dropped only beside pg_catalog's or
  # other members of that same extension: beside anything else, which could
  # be a plant in a writable pinned schema, the call is ambiguous.
  defp narrow_members(_name, reached, plain) do
    own = reached |> Enum.filter(& &1.member) |> MapSet.new(& &1.extension)

    if Enum.any?(plain, &(&1.schema != "pg_catalog" and not MapSet.member?(own, &1.extension))),
      do: ambiguous()

    plain
  end

  defp ambiguous,
    do:
      refuse(
        :function_ambiguous,
        "a function the statement calls must resolve to exactly one function in the pinned schemas"
      )

  defp cast_arguments(%{variadic: variadic} = info, args) when is_binary(variadic) do
    if Regex.match?(~r/(?:\A|\.)"?any"?\z/, variadic),
      do: refuse(:function_not_castable, ~s(a function the statement calls takes VARIADIC "any"))

    fixed = length(info.args) - 1
    {head, tail} = Enum.split(args, fixed)

    casts =
      head
      |> Enum.zip(info.args)
      |> Enum.map(fn {arg, type} -> "CAST((#{arg.sql}) AS #{type})" end)

    if tail == [] do
      casts
    else
      elements = Enum.map_join(tail, ", ", &"CAST((#{&1.sql}) AS #{variadic})")
      casts ++ ["VARIADIC ARRAY[#{elements}]"]
    end
  end

  defp cast_arguments(info, args) do
    args
    |> Enum.zip(info.args)
    |> Enum.map(fn {arg, type} -> "CAST((#{arg.sql}) AS #{type})" end)
  end

  # R49c: a function the extension of its first argument's type defines for
  # that type (citext's max and min, its replace and regexp_* overloads)
  # resolves in the extension's schema, as PostgreSQL resolves it: when the
  # other arguments are untyped or of that type too. Its result keeps the
  # type when it returns it.
  defp extension_function(_st, [], _name), do: nil

  defp extension_function(%{scopes: nil}, _args, _name), do: nil

  defp extension_function(st, [%{lone: first} | rest] = args, name) when is_map(first) do
    with %{entry: entry} <- class_of(st, first),
         overloads when is_list(overloads) <- Map.get(entry.fns, name),
         %{} = function <-
           Enum.find(overloads, fn function ->
             function.nargs == length(args) and
               rest
               |> Enum.zip(tl(function.args))
               |> Enum.all?(fn {arg, declared} ->
                 extension_argument?(st, arg, declared, entry)
               end)
           end) do
      %{function: function, entry: entry}
    else
      _ -> nil
    end
  end

  defp extension_function(_st, _args, _name), do: nil

  # A parameter of the member type takes an untyped value or that type; any
  # other parameter whatever is not an extension's.
  defp extension_argument?(st, %{lone: atom}, declared, entry) do
    class = if atom, do: class_of(st, atom), else: %{}

    if declared == entry.type do
      class[:untyped] == true or match?(%{entry: %{type: type}} when type == entry.type, class)
    else
      not Map.has_key?(class, :entry) and not Map.has_key?(class, :unknown)
    end
  end

  defp extension_call(_st, name, args, %{function: function, entry: entry}) do
    # R50: each argument cast to the declared type, so no aggregate or
    # function planted beside it over a domain can be chosen.
    cast =
      args
      |> Enum.zip(function.args)
      |> Enum.map_join(", ", fn {arg, type} -> "#{arg.prefix}CAST((#{arg.sql}) AS #{type})" end)

    sql = "#{quote_name(function.schema)}.#{text(name)}(#{cast})"
    if function.self, do: %{sql: sql, entry: entry}, else: %{sql: sql, known: true}
  end

  # FILTER (WHERE ...) / OVER (...) / WITHIN GROUP (...) follow a call.
  defp call_clauses(st, sql) do
    token = tok(st, st.pos)

    if upper(token) in ~w(FILTER OVER WITHIN) do
      word = text(token)
      st = advance(st)

      {word, st} =
        if upper(tok(st, st.pos)) == "GROUP",
          do: {"#{word} #{text(tok(st, st.pos))}", advance(st)},
          else: {word, st}

      if punct?(st, st.pos, "(") do
        {clause, st} = sequence(advance(st), ")")
        st = expect(st, ")")
        call_clauses(st, "#{sql} #{word} (#{clause})")
      else
        call_clauses(advance(st), "#{sql} #{word} #{text(tok(st, st.pos))}")
      end
    else
      {sql, st}
    end
  end

  @multiword_types [
    ~w(CHARACTER VARYING),
    ~w(DOUBLE PRECISION),
    ~w(TIMESTAMP WITH TIME ZONE),
    ~w(TIMESTAMP WITHOUT TIME ZONE),
    ~w(TIME WITH TIME ZONE),
    ~w(TIME WITHOUT TIME ZONE)
  ]

  # ::type, [subscript], AT TIME ZONE x, COLLATE x after an atom.
  defp postfix(st, atom, mark) do
    atom =
      Map.merge(
        %{
          column: nil,
          known: false,
          entry: nil,
          unknown: false,
          untyped: false,
          array_entry: nil,
          row: false,
          subquery: nil,
          quantified: nil
        },
        atom
      )

    postfix_loop(st, atom, mark)
  end

  defp postfix_loop(st, atom, mark) do
    token = tok(st, st.pos)

    cond do
      token == {:punct, "::"} ->
        from = st.pos + 1
        {type, st} = postfix_type(advance(st))
        tokens = Enum.map(from..(st.pos - 1)//1, &tok(st, &1))
        facts = type_facts(st, tokens, cast_entry(st, tokens))

        atom =
          atom
          |> Map.merge(%{
            sql: "#{atom.sql}::#{type}",
            column: nil,
            entry: nil,
            known: false,
            row: false,
            subquery: nil,
            quantified: nil
          })
          |> Map.merge(facts)

        postfix_loop(st, atom, mark)

      token == {:punct, "["} ->
        {inner, st} = sequence(advance(st), "]")
        st = expect(st, "]")

        postfix_loop(
          st,
          %{atom | sql: "#{atom.sql}[#{inner}]", column: nil, entry: nil, known: false},
          mark
        )

      upper(token) == "AT" and upper(tok(st, st.pos + 1)) == "TIME" and
          upper(tok(st, st.pos + 2)) == "ZONE" ->
        case atom(advance(st, 3)) do
          {nil, _} ->
            refuse(:sql_unqualifiable, "generated SQL could not be parsed")

          {zone, st} ->
            sql = "(#{atom.sql} AT TIME ZONE #{zone.sql})"
            postfix_loop(st, %{atom | sql: sql, column: nil, entry: nil, known: true}, mark)
        end

      upper(token) == "COLLATE" ->
        {names, st} = name_chain(advance(st, 2), [tok(st, st.pos + 1)])
        # A collation keeps the operand's type (and column).
        postfix_loop(st, %{atom | sql: "(#{atom.sql} COLLATE #{join_names(names)})"}, mark)

      true ->
        finish_atom(st, atom, mark)
    end
  end

  defp postfix_type(st) do
    words =
      Enum.find(@multiword_types, fn words ->
        words
        |> Enum.with_index()
        |> Enum.all?(fn {word, i} -> upper(tok(st, st.pos + i)) == word end)
      end)

    {type, st} =
      if words do
        count = length(words)
        {Enum.map_join(0..(count - 1), " ", &text(tok(st, st.pos + &1))), advance(st, count)}
      else
        {names, st} = name_chain(advance(st), [tok(st, st.pos)])
        {join_names(names), st}
      end

    {type, st} =
      if punct?(st, st.pos, "(") do
        close = close_index(st, st.pos)
        args = Enum.map_join((st.pos + 1)..(close - 1)//1, "", &text(tok(st, &1)))
        {"#{type}(#{args})", %{st | pos: close + 1}}
      else
        {type, st}
      end

    array_suffix(st, type)
  end

  defp array_suffix(st, type) do
    if punct?(st, st.pos, "[") and punct?(st, st.pos + 1, "]"),
      do: array_suffix(advance(st, 2), type <> "[]"),
      else: {type, st}
  end

  # The column references this atom's type depends on: itself for a column;
  # none for a known or extension-typed result; else those read inside it.
  defp finish_atom(st, atom, mark) do
    {kept, own} = Enum.split(st.refs, mark)

    {refs, st} =
      cond do
        atom.known or atom.entry != nil or atom.unknown or atom.array_entry != nil ->
          {[], st}

        atom.column != nil ->
          ref = %{id: st.next_ref, column: atom.column}
          {[ref], %{st | next_ref: st.next_ref + 1}}

        true ->
          {own, st}
      end

    st = %{st | refs: kept ++ refs}
    {atom |> Map.put(:kind, :atom) |> Map.put(:refs, refs), st}
  end

  # ---- Precedence --------------------------------------------------------

  # PostgreSQL's, highest first: ^; * / %; + -; any other operator;
  # comparison. Prefix + and - bind tightest.
  defp rank("^"), do: 6
  defp rank(op) when op in ["*", "/", "%"], do: 5
  defp rank(op) when op in ["+", "-"], do: 4
  defp rank(op) when op in @comparison, do: 2
  defp rank(_op), do: 3

  defp emit_run(st, run) do
    items = List.to_tuple(run)
    {tree, i} = climb(items, 0, 0)
    {sql, st} = render(st, tree)

    {rest, st} =
      Enum.map_reduce(i..(tuple_size(items) - 1)//1, st, fn k, st ->
        case elem(items, k) do
          %{kind: :atom, sql: atom_sql} ->
            {atom_sql, st}

          op ->
            {op_text, _cast, st} = op_sql(st, op, nil, nil)
            {op_text, st}
        end
      end)

    {Enum.join([sql | rest], " "), st}
  end

  defp climb(items, i, min) do
    {left, i} = primary(items, i)
    climb_loop(items, i, min, left)
  end

  defp climb_loop(items, i, min, left) when i < tuple_size(items) do
    case elem(items, i) do
      %{kind: :op} = op ->
        rank = if Map.get(op, :explicit), do: 3, else: rank(op.op)

        # A trailing operator with no right operand: left in place.
        if rank < min or i + 1 >= tuple_size(items) do
          {left, i}
        else
          {right, i} = climb(items, i + 1, rank + 1)
          climb_loop(items, i, min, %{binary: op, left: left, right: right})
        end

      _atom ->
        {left, i}
    end
  end

  defp climb_loop(_items, i, _min, left), do: {left, i}

  defp primary(items, i) when i >= tuple_size(items), do: {nil, i}

  defp primary(items, i) do
    case elem(items, i) do
      %{kind: :op} = op ->
        {operand, i} = climb(items, i + 1, 7)
        {%{prefix: op, operand: operand}, i}

      atom ->
        {%{atom: atom}, i + 1}
    end
  end

  defp render(st, nil), do: {"", st}
  defp render(st, %{atom: atom}), do: {atom.sql, st}

  defp render(st, %{prefix: op, operand: operand}) do
    {sql, st} = render(st, operand)
    {op_text, _cast, st} = op_sql(st, Map.put(op, :prefix, true), operand, nil)
    {"(#{op_text} (#{sql}))", st}
  end

  defp render(st, %{binary: op, left: left, right: right}) do
    {left_sql, st} = render(st, left)
    {right_sql, st} = render(st, right)
    {op_text, cast, st} = op_sql(st, op, left, right)
    quantified = right |> Map.get(:atom, %{}) |> Map.get(:quantified)

    # x op ANY/ALL/SOME (array or subquery): the quantifier belongs to the
    # operator's syntax and takes no parentheses of its own.
    right_text =
      cond do
        quantified == nil -> " (#{right_cast(right_sql, cast)}))"
        cast == nil -> " #{right_sql})"
        true -> " #{quantified_cast(quantified, cast)})"
      end

    {"((#{left_cast(left_sql, cast)}) #{op_text}" <> right_text, st}
  end

  defp left_cast(sql, nil), do: sql
  defp left_cast(sql, {type, _right}), do: "CAST((#{sql}) AS #{type})"

  defp right_cast(sql, nil), do: sql
  defp right_cast(sql, {_left, type}), do: "CAST((#{sql}) AS #{type})"

  defp cast_subquery(query, nil), do: query

  defp cast_subquery(query, {_left, type}),
    do: "SELECT CAST((q.v) AS #{type}) FROM (#{query}) AS q(v)"

  defp quantified_cast(%{subquery: open} = quantified, cast) when is_integer(open),
    do: "#{quantified.word} (#{cast_subquery(quantified.inner, cast)})"

  defp quantified_cast(quantified, {_left, type}),
    do: "#{quantified.word} (CAST((#{quantified.inner}) AS #{type}[]))"

  # OPERATOR(schema.op) and the types its operands are cast to (R50), or nil.
  # An operator outside pg_catalog is named only when it is a member of the
  # extension of an operand's type for those operand types (H1): the
  # extension's schema is commonly writable, and a user operator planted
  # there is exactly as resolvable as a member. PostgreSQL's own resolution
  # picks a member when both operands have its types, or one has and the
  # other is an untyped parameter or literal; a citext against text resolves
  # to pg_catalog's text operator (R48). The same choice is made here,
  # whatever the operand order. Each operator decides its own casts: none
  # carries over to the next.
  defp op_sql(st, %{explicit: true, schema: "pg_catalog"} = op, _left, _right),
    do: {"OPERATOR(pg_catalog.#{op.op})", nil, st}

  # An explicit operator elsewhere is verified, not trusted.
  defp op_sql(st, %{explicit: true} = op, left, right) do
    {member, st} = resolve_operator(st, op.op, left, right)

    if member == nil or member.schema != quoted_schema(op.schema),
      do:
        refuse(
          :operator_not_member,
          "an operator outside pg_catalog is no member of its operands' extension"
        )

    {"OPERATOR(#{member.schema}.#{member.op})", {member.left, member.right}, st}
  end

  defp op_sql(st, %{prefix: true} = op, _left, _right),
    do: {"OPERATOR(pg_catalog.#{op.op})", nil, st}

  defp op_sql(st, op, left, right) do
    {member, st} = resolve_operator(st, op.op, left, right)

    case member do
      nil ->
        name = if op.op == "!=", do: "<>", else: op.op
        {"OPERATOR(pg_catalog.#{name})", nil, st}

      member ->
        {"OPERATOR(#{member.schema}.#{member.op})", {member.left, member.right}, st}
    end
  end

  defp quoted_schema(schema) do
    case tokenize(schema) do
      [{:qid, _} = name] -> quote_name(unquote_name(name))
      [{:id, _} = name] -> quote_name(unquote_name(name))
      _ -> schema
    end
  end

  defp resolve_operator(st, op, left, right) do
    name = if op == "!=", do: "<>", else: op
    l = operand_class(st, left)
    r = operand_class(st, right)

    if l[:row] == :extension or r[:row] == :extension,
      do:
        refuse(
          :operand_type_unknown,
          "a row comparison over an extension type cannot be written with explicit operators"
        )

    for {unknown, other} <- [{l, r}, {r, l}] do
      if unknown[:unknown] && (other[:untyped] || other[:entry] || other[:unknown]) do
        refuse(
          :operand_type_unknown,
          "an operand that may have an extension type could not be resolved for operator qualification"
        )
      end
    end

    member = member_operator(name, l, r)

    # A comparison's operands do not type an expression around it (a CASE
    # WHEN tier = 'gold' THEN 'G' ...): their references are consumed.
    st =
      if name in @boolean_op,
        do: Enum.reduce([left, right], st, &consume(&2, &1)),
        else: st

    {member, st}
  end

  defp member_operator(name, %{entry: l}, %{entry: r}) do
    (Map.get(l.ops, name, []) ++ Map.get(r.ops, name, []))
    |> Enum.find(&(&1.left == l.type and &1.right == r.type))
    |> named(name)
  end

  defp member_operator(name, %{entry: l}, %{untyped: true}),
    do: untyped_side(Map.get(l.ops, name, []), :left, :right, l.type) |> named(name)

  defp member_operator(name, %{untyped: true}, %{entry: r}),
    do: untyped_side(Map.get(r.ops, name, []), :right, :left, r.type) |> named(name)

  defp member_operator(name, %{entry: l}, r),
    do: typed_side(Map.get(l.ops, name, []), :left, :right, l.type, r[:type]) |> named(name)

  defp member_operator(name, l, %{entry: r}),
    do: typed_side(Map.get(r.ops, name, []), :right, :left, r.type, l[:type]) |> named(name)

  defp member_operator(_name, _l, _r), do: nil

  defp named(nil, _name), do: nil
  defp named(member, name), do: Map.put(member, :op, name)

  # One side of the member's type, the other untyped: PostgreSQL assumes the
  # same type when that operator exists, else the one candidate, preferring
  # text among several.
  defp untyped_side(operators, side, other, type) do
    candidates = Enum.filter(operators, &(Map.get(&1, side) == type))

    Enum.find(candidates, &(Map.get(&1, other) == type)) ||
      case candidates do
        [] ->
          nil

        [one] ->
          one

        many ->
          Enum.find(many, &(Map.get(&1, other) == "pg_catalog.text")) ||
            refuse(:operand_type_unknown, "an untyped operand fits several extension operators")
      end
  end

  # One side of the member's type, the other of a type no extension owns: a
  # member for exactly that pair, else pg_catalog's by implicit cast; with a
  # member for some other type and the operand's type not known, the choice
  # cannot be made here.
  defp typed_side(operators, side, other, type, other_type) do
    candidates =
      Enum.filter(operators, &(Map.get(&1, side) == type and Map.get(&1, other) != type))

    cond do
      other_type != nil ->
        Enum.find(candidates, &(Map.get(&1, other) == other_type))

      candidates == [] ->
        nil

      true ->
        refuse(
          :operand_type_unknown,
          "an operand's type could not be named for an extension operator"
        )
    end
  end

  defp consume(st, %{atom: atom}) do
    ids = atom |> Map.get(:refs, []) |> Enum.map(& &1.id)
    %{st | consumed: Enum.into(ids, st.consumed)}
  end

  defp consume(st, %{} = node) do
    [:left, :right, :operand]
    |> Enum.reduce(st, fn key, st ->
      case Map.get(node, key) do
        nil -> st
        child -> consume(st, child)
      end
    end)
  end

  defp consume(st, _node), do: st

  defp operand_class(_st, nil), do: %{untyped: true}
  defp operand_class(_st, %{known: true}), do: %{}
  defp operand_class(st, %{atom: atom}), do: class_of(st, atom)
  defp operand_class(_st, _node), do: %{}

  # What is known of an atom's type: %{untyped: true} (a bare parameter or
  # string literal), %{entry: e} (its extension type's operators and
  # functions), %{unknown: true} (it may have an extension type that cannot
  # be resolved here), or %{} (a known type that is no extension's).
  defp class_of(st, atom) do
    cond do
      Map.get(atom, :quantified) != nil ->
        quantified_class(st, atom, atom.quantified)

      is_integer(Map.get(atom, :subquery)) ->
        subquery_class(st, atom.subquery)

      Map.get(atom, :row) == true ->
        row_class(st, atom)

      Map.get(atom, :untyped) == true ->
        %{untyped: true}

      Map.get(atom, :unknown) == true ->
        %{unknown: true}

      Map.get(atom, :column) == nil and
          Regex.match?(~r/\A(?:\$\d+|[Ee]?'(?:[^']|'')*')\z/, atom.sql) ->
        %{untyped: true}

      Map.get(atom, :entry) != nil ->
        %{entry: atom.entry}

      Map.get(atom, :known) == true or st.scopes == nil ->
        %{}

      is_map(Map.get(atom, :column)) ->
        ref_class(st, atom.column)

      true ->
        expression_class(st, atom)
    end
  end

  # ANY/ALL/SOME take the element type of what they range over.
  defp quantified_class(_st, %{unknown: true}, _quantified), do: %{unknown: true}

  defp quantified_class(st, _atom, %{subquery: open}) when is_integer(open),
    do: subquery_class(st, open)

  defp quantified_class(st, atom, %{lone: nil}), do: expression_class(st, atom)

  defp quantified_class(st, atom, %{lone: lone}) do
    cond do
      Map.get(lone, :array_entry) != nil ->
        %{entry: lone.array_entry}

      st.scopes != nil and is_map(Map.get(lone, :column)) ->
        case resolve_column(st, lone.column.at, lone.column.names) do
          %{elem: %{} = element} -> %{entry: element}
          %{unknown: true} -> %{unknown: true}
          _ -> %{}
        end

      true ->
        case class_of(st, lone) do
          %{untyped: true} -> %{untyped: true}
          %{unknown: true} -> %{unknown: true}
          _ -> expression_class(st, atom)
        end
    end
  end

  # A scalar or IN subquery: the type of its one output column.
  defp subquery_class(%{scopes: nil}, _open), do: %{}

  defp subquery_class(st, open) do
    with scope when is_integer(scope) <- Map.get(st.scopes.opens, open),
         [item] when item != [] <- Map.fetch!(st.scopes.scopes, scope).items do
      case item_entry(st, scope, item) do
        %{unknown: true} -> %{unknown: true}
        %{} = entry -> if extension?(entry), do: %{entry: entry}, else: %{}
      end
    else
      _ -> %{unknown: true}
    end
  end

  # A row over an extension-typed column compares it by that type's operator:
  # no single OPERATOR() can say so.
  defp row_class(%{scopes: nil}, _atom), do: %{}

  defp row_class(st, atom) do
    case expression_class(st, atom) do
      %{unknown: true} -> %{row: :extension}
      _ -> %{}
    end
  end

  # An expression: its result type follows its operands. Unknown when one
  # has (or may have) an extension type.
  defp expression_class(st, atom) do
    atom
    |> Map.get(:refs, [])
    |> Enum.reject(&MapSet.member?(st.consumed, &1.id))
    |> Enum.find_value(%{}, fn ref ->
      case ref_class(st, ref.column) do
        %{entry: _} -> %{unknown: true}
        %{unknown: true} = class -> class
        _ -> nil
      end
    end)
  end

  defp ref_class(st, column) do
    case resolve_column(st, column.at, column.names) do
      %{unknown: true} ->
        %{unknown: true}

      %{} = entry ->
        if extension?(entry), do: %{entry: entry}, else: Map.take(entry, [:type])

      nil ->
        # No source found: unknown only when an extension-typed column of a
        # relation the statement names has its name.
        if names_extension_column?(st, List.last(column.names)), do: %{unknown: true}, else: %{}
    end
  end

  defp extension?(entry), do: map_size(entry[:ops] || %{}) > 0 or map_size(entry[:fns] || %{}) > 0

  defp names_extension_column?(st, name) do
    name = String.downcase(name)

    st.facts
    |> Map.get(:relations, %{})
    |> Enum.any?(fn {_relation, columns} ->
      case Map.get(columns, name) do
        %{} = entry -> extension?(entry) or entry[:elem] != nil
        _ -> false
      end
    end)
  end

  # ---- Column sources by query scope -------------------------------------
  #
  # A column reference resolves through the scope it appears in: its alias
  # names a relation, a CTE or a derived table of that scope or an enclosing
  # one. A CTE's or derived table's output passes its source column's type
  # on when its select item is that column, so citext keeps its operators
  # through member CTEs and laterals, and a reused alias resolves to its own
  # scope's relation.

  defp scopes(toks) do
    count = tuple_size(toks)

    initial = %{
      scope_of: %{},
      depth_of: %{},
      opens: %{},
      closes: %{},
      scopes: %{0 => %{parent: nil, aliases: %{}, items: [], select: nil}},
      next: 1,
      stack: [%{scope: 0, open: nil}]
    }

    walk =
      Enum.reduce(0..(count - 1)//1, initial, fn i, acc ->
        token = elem(toks, i)

        acc =
          cond do
            token == {:punct, "("} ->
              top = hd(acc.stack)
              %{acc | stack: [%{scope: top.scope, open: i} | acc.stack]}

            token == {:punct, ")"} ->
              [frame | stack] = acc.stack
              closes = if frame.open, do: Map.put(acc.closes, frame.open, i), else: acc.closes
              stack = if stack == [], do: [%{scope: 0, open: nil}], else: stack
              %{acc | stack: stack, closes: closes}

            upper(token) == "SELECT" ->
              [top | rest] = acc.stack
              id = acc.next
              scope = %{parent: top.scope, aliases: %{}, items: [], select: i}

              opens =
                if top.open, do: Map.put_new(acc.opens, top.open, id), else: acc.opens

              %{
                acc
                | scopes: Map.put(acc.scopes, id, scope),
                  next: id + 1,
                  opens: opens,
                  stack: [%{top | scope: id} | rest]
              }

            true ->
              acc
          end

        top = hd(acc.stack)

        %{
          acc
          | scope_of: Map.put(acc.scope_of, i, top.scope),
            depth_of: Map.put(acc.depth_of, i, length(acc.stack))
        }
      end)

    info = %{
      toks: toks,
      scope_of: walk.scope_of,
      depth_of: walk.depth_of,
      opens: walk.opens,
      closes: walk.closes,
      scopes: walk.scopes,
      cte: %{}
    }

    info |> collect_ctes() |> collect_items() |> collect_sources()
  end

  defp stok(info, i) when i >= 0 and i < tuple_size(info.toks), do: elem(info.toks, i)
  defp stok(_info, _i), do: nil

  defp name_token?({kind, _}) when kind in [:id, :qid], do: true
  defp name_token?(_token), do: false

  defp name_at(info, k) do
    token = stok(info, k)
    if name_token?(token), do: name_more(info, k + 1, [unquote_name(token)]), else: {nil, k}
  end

  defp name_more(info, k, names) do
    if stok(info, k) == {:punct, "."} and name_token?(stok(info, k + 1)),
      do: name_more(info, k + 2, names ++ [unquote_name(stok(info, k + 1))]),
      else: {names, k}
  end

  defp cols_at(info, k) do
    with {:punct, "("} <- stok(info, k),
         close when is_integer(close) <- Map.get(info.closes, k) do
      cols =
        for i <- (k + 1)..(close - 1)//1,
            name_token?(stok(info, i)),
            do: unquote_name(stok(info, i))

      {cols, close + 1}
    else
      _ -> {nil, k}
    end
  end

  # WITH [RECURSIVE] name [(cols)] AS [NOT] [MATERIALIZED] ( body ), ...
  defp collect_ctes(info) do
    count = tuple_size(info.toks)

    Enum.reduce(0..(count - 1)//1, info, fn i, info ->
      if upper(stok(info, i)) == "WITH" do
        k = if upper(stok(info, i + 1)) == "RECURSIVE", do: i + 2, else: i + 1
        cte_items(info, k)
      else
        info
      end
    end)
  end

  defp cte_items(info, k) do
    with {[name], after_name} <- name_at(info, k),
         {cols, next} <- cols_at(info, after_name),
         "AS" <- upper(stok(info, next)) do
      k = next + 1
      k = if upper(stok(info, k)) == "NOT", do: k + 1, else: k
      k = if upper(stok(info, k)) == "MATERIALIZED", do: k + 1, else: k

      with {:punct, "("} <- stok(info, k),
           close when is_integer(close) <- Map.get(info.closes, k) do
        entry = %{scope: Map.get(info.opens, k), cols: cols}
        info = %{info | cte: Map.put(info.cte, String.downcase(name), entry)}

        if stok(info, close + 1) == {:punct, ","},
          do: cte_items(info, close + 2),
          else: info
      else
        _ -> info
      end
    else
      _ -> info
    end
  end

  @item_stop ~w(FROM WHERE GROUP ORDER LIMIT OFFSET UNION INTERSECT EXCEPT HAVING WINDOW FOR INTO RETURNING)

  # Each scope's select items.
  defp collect_items(info) do
    scopes =
      Map.new(info.scopes, fn
        {0, scope} ->
          {0, scope}

        {id, scope} ->
          depth = Map.fetch!(info.depth_of, scope.select)
          k = scope.select + 1
          k = if upper(stok(info, k)) == "DISTINCT", do: k + 1, else: k
          {id, %{scope | items: select_items(info, id, depth, k, [], [])}}
      end)

    %{info | scopes: scopes}
  end

  defp select_items(info, id, depth, k, item, items) do
    token = stok(info, k)
    token_depth = Map.get(info.depth_of, k, 0)
    here? = token_depth == depth and Map.get(info.scope_of, k) == id

    cond do
      token == nil or token_depth < depth ->
        close_items(item, items)

      here? and upper(token) in @item_stop ->
        close_items(item, items)

      here? and token == {:punct, ","} ->
        select_items(info, id, depth, k + 1, [], [Enum.reverse(item) | items])

      true ->
        select_items(info, id, depth, k + 1, [k | item], items)
    end
  end

  defp close_items([], items), do: Enum.reverse(items)
  defp close_items(item, items), do: Enum.reverse([Enum.reverse(item) | items])

  defp collect_sources(info) do
    Enum.reduce(0..(tuple_size(info.toks) - 1)//1, info, fn k, info ->
      up = upper(stok(info, k))

      if up in ~w(FROM JOIN UPDATE) or (up == "INTO" and upper(stok(info, k - 1)) == "INSERT") do
        source_items(info, k, up, Map.get(info.scope_of, k, 0), k + 1)
      else
        info
      end
    end)
  end

  defp source_items(info, k, up, scope, j) do
    j = skip_words(info, j, ~w(LATERAL ONLY))
    token = stok(info, j)

    cond do
      # A parenthesised join group, FROM (a JOIN b ON ...): its first item
      # is registered here, the others by their own JOIN.
      token == {:punct, "("} and not Map.has_key?(info.opens, j) ->
        source_items(info, k, up, scope, j + 1)

      token == {:punct, "("} ->
        case Map.get(info.closes, j) do
          nil -> info
          close -> source_alias(info, k, up, scope, close + 1, %{derived: info.opens[j]}, nil)
        end

      true ->
        source_name(info, k, up, scope, j)
    end
  end

  defp skip_words(info, j, words) do
    if upper(stok(info, j)) in words, do: skip_words(info, j + 1, words), else: j
  end

  defp source_name(info, k, up, scope, j) do
    case name_at(info, j) do
      {nil, _} ->
        info

      {names, after_name} ->
        if stok(info, after_name) == {:punct, "("} and up != "INTO" do
          # A function rowset, FROM fn(args) [WITH ORDINALITY] [AS] alias
          # [(col [type], ...)].
          case Map.get(info.closes, after_name) do
            nil ->
              info

            close ->
              source = %{
                function: String.downcase(List.last(names)),
                open: after_name,
                close: close
              }

              j = close + 1

              {source, j} =
                if upper(stok(info, j)) == "WITH" and upper(stok(info, j + 1)) == "ORDINALITY",
                  do: {Map.put(source, :ordinality, true), j + 2},
                  else: {source, j}

              source_alias(info, k, up, scope, j, source, List.last(names))
          end
        else
          source =
            if length(names) == 1 and Map.has_key?(info.cte, String.downcase(hd(names))),
              do: %{cte: String.downcase(hd(names))},
              else: %{relation: names |> Enum.join(".") |> String.downcase()}

          source_alias(info, k, up, scope, after_name, source, List.last(names))
        end
    end
  end

  defp source_alias(info, k, up, scope, j, source, default) do
    j = if upper(stok(info, j)) == "AS", do: j + 1, else: j
    token = stok(info, j)

    {alias_name, j} =
      if name_token?(token) and not (match?({:id, _}, token) and keyword_token?(token)),
        do: {unquote_name(token), j + 1},
        else: {default, j}

    {source, j} =
      if Map.has_key?(source, :function) do
        function_columns(info, source, alias_name, j)
      else
        case cols_at(info, j) do
          {cols, next} when is_list(cols) ->
            if Map.has_key?(source, :derived) or Map.has_key?(source, :cte),
              do: {Map.put(source, :cols, cols), next},
              else: {source, j}

          _ ->
            {source, j}
        end
      end

    info =
      if alias_name do
        scopes =
          Map.update!(info.scopes, scope, fn entry ->
            %{entry | aliases: Map.put(entry.aliases, String.downcase(alias_name), source)}
          end)

        %{info | scopes: scopes}
      else
        info
      end

    if up == "FROM" and stok(info, j) == {:punct, ","},
      do: source_items(info, k, up, scope, j + 1),
      else: info
  end

  # A rowset's column list, with declared types (a record function's).
  defp function_columns(info, source, alias_name, j) do
    with {:punct, "("} <- stok(info, j),
         close when is_integer(close) <- Map.get(info.closes, j) do
      depth = Map.get(info.depth_of, j + 1)

      {groups, current} =
        Enum.reduce((j + 1)..(close - 1)//1, {[], []}, fn i, {groups, current} ->
          if stok(info, i) == {:punct, ","} and Map.get(info.depth_of, i) == depth,
            do: {[Enum.reverse(current) | groups], []},
            else: {groups, [i | current]}
        end)

      groups = Enum.reverse([Enum.reverse(current) | groups]) |> Enum.reject(&(&1 == []))
      cols = Enum.map(groups, &unquote_name(stok(info, hd(&1))))

      types =
        Enum.map(groups, fn
          [_name] -> nil
          [_name | type] -> type |> Enum.map_join(" ", &text(stok(info, &1))) |> String.downcase()
        end)

      {source |> Map.put(:cols, cols) |> Map.put(:types, types), close + 1}
    else
      _ -> {Map.put(source, :cols, [alias_name]), j}
    end
  end

  defp resolve_column(%{scopes: nil}, _at, _names), do: nil

  defp resolve_column(_st, _at, names) when length(names) > 3 or names == [], do: nil

  defp resolve_column(st, at, names) do
    scope = Map.get(st.scopes.scope_of, at, 0)
    qualifier = if length(names) > 1, do: names |> Enum.at(-2) |> String.downcase()
    resolve_in(st, scope, qualifier, names |> List.last() |> String.downcase(), 1)
  end

  defp resolve_in(_st, _scope, _qualifier, _column, guard) when guard > 16, do: nil
  defp resolve_in(_st, nil, _qualifier, _column, _guard), do: nil

  defp resolve_in(st, scope, qualifier, column, guard) do
    entry = Map.fetch!(st.scopes.scopes, scope)

    source =
      cond do
        qualifier != nil -> Map.get(entry.aliases, qualifier)
        map_size(entry.aliases) == 1 -> entry.aliases |> Map.values() |> hd()
        true -> nil
      end

    cond do
      source != nil ->
        source_column(st, source, column, guard + 1)

      qualifier == nil and map_size(entry.aliases) > 1 ->
        case unqualified_sources(st, entry.aliases, column, guard) do
          [] -> resolve_in(st, entry.parent, qualifier, column, guard)
          [found] -> found
          _several -> %{unknown: true}
        end

      true ->
        resolve_in(st, entry.parent, qualifier, column, guard)
    end
  end

  # A bare column of a many-source FROM: the sources that have it.
  defp unqualified_sources(st, aliases, column, guard) do
    aliases
    |> Map.values()
    |> Enum.flat_map(fn
      %{relation: relation} = source ->
        columns = Map.get(Map.get(st.facts, :relations, %{}), relation, %{})

        if Map.has_key?(columns, column),
          do: [source_column(st, source, column, guard + 1)],
          else: []

      source ->
        case source_column(st, source, column, guard + 1) do
          nil -> []
          found -> [found]
        end
    end)
  end

  defp source_column(st, %{relation: relation}, column, _guard) do
    # A relation with no visible columns is unknown; its columns are all
    # listed, so one that is not was not there when the types were read.
    case Map.get(Map.get(st.facts, :relations, %{}), relation) do
      nil ->
        %{unknown: true}

      columns ->
        case Map.get(columns, column) do
          nil -> if column in @system_column, do: %{}, else: %{unknown: true}
          entry -> entry
        end
    end
  end

  defp source_column(st, %{function: _} = source, column, guard),
    do: function_column(st, source, column, guard)

  defp source_column(st, %{cte: name} = source, column, guard) do
    case Map.get(st.scopes.cte, name) do
      nil -> nil
      cte -> output_column(st, cte.scope, source[:cols] || cte.cols, column, guard)
    end
  end

  defp source_column(st, %{derived: scope} = source, column, guard),
    do: output_column(st, scope, source[:cols], column, guard)

  defp output_column(_st, nil, _cols, _column, _guard), do: nil

  defp output_column(st, scope, cols, column, guard) do
    items = Map.fetch!(st.scopes.scopes, scope).items

    items
    |> Enum.with_index()
    |> Enum.find_value(fn {item, index} ->
      output_item(st, scope, item, index, cols, column, guard)
    end)
  end

  defp output_item(_st, _scope, [], _index, _cols, _column, _guard), do: nil

  defp output_item(st, scope, item, index, cols, column, guard) do
    {alias_name, chain, passthrough?} = item_parts(st, item)

    name =
      cond do
        alias_name != nil -> alias_name
        passthrough? -> List.last(chain)
        true -> nil
      end

    output = if is_list(cols) and index < length(cols), do: Enum.at(cols, index), else: name

    if output == nil or String.downcase(output) != column,
      do: nil,
      else: item_entry(st, scope, item, guard)
  end

  defp item_parts(st, item) do
    {alias_name, body} = item_body(st, item)
    {chain, passthrough?} = passthrough_chain(body)
    {alias_name, chain, passthrough?}
  end

  defp item_body(st, item) do
    tokens = Enum.map(item, &stok(st.scopes, &1))
    count = length(tokens)
    last = List.last(tokens)
    before = if count >= 2, do: Enum.at(tokens, -2)

    alias_name =
      if count >= 2 and name_token?(last) and alias_before?(before), do: unquote_name(last)

    body =
      cond do
        alias_name == nil -> tokens
        upper(before) == "AS" -> Enum.drop(tokens, -2)
        true -> Enum.drop(tokens, -1)
      end

    {alias_name, body}
  end

  # A select item's type: its source column's, or as an operand is typed.
  defp item_entry(st, scope, item, guard \\ 1) do
    {_alias, body} = item_body(st, item)
    {chain, passthrough?} = passthrough_chain(body)

    if passthrough? do
      qualifier = if length(chain) > 1, do: chain |> Enum.at(-2) |> String.downcase()

      resolve_in(st, scope, qualifier, chain |> List.last() |> String.downcase(), guard + 1) ||
        %{unknown: true}
    else
      computed_entry(st, hd(item), hd(item) + length(body) - 1)
    end
  end

  # `expr AS alias` and the bare `expr alias`.
  defp alias_before?({:punct, p}), do: p in [")", "]"]
  defp alias_before?({kind, _}) when kind in [:num, :str, :param, :qid], do: true
  defp alias_before?({:id, _} = token), do: upper(token) == "AS" or not keyword_token?(token)
  defp alias_before?(_token), do: false

  # A passthrough item: a [b.]c, in any redundant parentheses.
  defp passthrough_chain(body) do
    wrap = wrap_depth(body, 0)
    inner = body |> Enum.drop(wrap) |> Enum.drop(-wrap)
    chain_of(inner, [])
  end

  defp wrap_depth(body, n) do
    if length(body) > 2 * n and Enum.at(body, n) == {:punct, "("} and
         Enum.at(body, -1 - n) == {:punct, ")"},
       do: wrap_depth(body, n + 1),
       else: n
  end

  defp chain_of([token], acc) do
    if name_token?(token) do
      chain = Enum.reverse([unquote_name(token) | acc])
      {chain, length(chain) <= 3}
    else
      {[], false}
    end
  end

  defp chain_of([token, {:punct, "."} | rest], acc) do
    if name_token?(token), do: chain_of(rest, [unquote_name(token) | acc]), else: {[], false}
  end

  defp chain_of(_tokens, _acc), do: {[], false}

  defp computed_entry(st, first, last) do
    if st.computing > 8 do
      %{unknown: true}
    else
      try do
        inner = %{st | pos: first, limit: last + 1, refs: [], computing: st.computing + 1}
        {tree, after_run} = read_run(inner)

        if after_run.pos == last + 1,
          do: tree_entry(after_run, tree),
          else: %{unknown: true}
      catch
        {:qualify_refuse, _reason, _message} -> %{unknown: true}
      end
    end
  end

  defp tree_entry(st, %{atom: atom}) do
    case class_of(st, atom) do
      %{entry: entry} -> entry
      %{unknown: true} -> %{unknown: true}
      _ -> %{}
    end
  end

  defp tree_entry(_st, _tree), do: %{}

  # A function rowset's output column: a declared column type, the element
  # type of an array for unnest, or what the catalog says of the function's
  # outputs. Unknown types are never taken for text.
  defp function_column(st, source, column, guard) do
    cols = source[:cols] || []
    index = Enum.find_index(cols, &(String.downcase(&1) == column))

    cond do
      index == nil ->
        nil

      is_binary(Enum.at(source[:types] || [], index)) ->
        declared_column(Enum.at(source.types, index))

      source[:ordinality] && index == length(cols) - 1 && length(cols) > 1 ->
        %{}

      source.function == "unnest" ->
        unnest_column(st, source, index, guard)

      true ->
        rowset_column(st, source, cols, index)
    end
  end

  defp declared_column(type) do
    type =
      type
      |> String.replace("\"", "")
      |> String.replace(~r/\A\s*pg_catalog\s*\.\s*/, "")
      |> String.replace(~r/\s*\(.*\z/, "")
      |> String.replace(~r/\s*\[\s*\]\z/, "[]")

    if MapSet.member?(@builtin_type, type), do: %{}, else: %{unknown: true}
  end

  # unnest(array column): the column's element type.
  defp unnest_column(st, source, index, guard) do
    tokens =
      for i <- (source.open + 1)..(source.close - 1)//1,
          token = stok(st.scopes, i),
          token not in [{:punct, "("}, {:punct, ")"}],
          do: token

    case {chain_of(tokens, []), index} do
      {{chain, true}, 0} ->
        qualifier = if length(chain) > 1, do: chain |> Enum.at(-2) |> String.downcase()
        scope = Map.get(st.scopes.scope_of, source.open, 0)
        column = chain |> List.last() |> String.downcase()

        case resolve_in(st, scope, qualifier, column, guard + 1) do
          %{elem: %{} = element} -> element
          %{unknown: true} -> %{unknown: true}
          %{} = entry -> if extension?(entry), do: %{unknown: true}, else: %{}
          nil -> %{unknown: true}
        end

      {_chain, _index} ->
        if Enum.all?(tokens, &literal_token?/1), do: %{}, else: %{unknown: true}
    end
  end

  # unnest(ARRAY['a', 'b']): text, numbers: no extension type.
  defp literal_token?({kind, _}) when kind in [:str, :num], do: true
  defp literal_token?({:punct, p}), do: p in ["[", "]", ","]
  defp literal_token?({:id, _} = token), do: upper(token) == "ARRAY"
  defp literal_token?(_token), do: false

  defp rowset_column(st, source, cols, index) do
    case Map.get(Map.get(st.facts, :rowsets, %{}), source.function) do
      %{} = rowset ->
        declared = rowset[:cols]
        width = length(cols) - if(source[:ordinality], do: 1, else: 0)

        cond do
          is_list(declared) and length(declared) == width ->
            Enum.at(declared, index) || %{unknown: true}

          rowset[:state] == :known ->
            %{}

          true ->
            %{unknown: true}
        end

      _ ->
        %{unknown: true}
    end
  end

  defp unquote_name({:qid, text}), do: unquote_text(text)
  defp unquote_name({:id, text}), do: String.downcase(text)
  defp unquote_name({:op, text}), do: text

  defp unquote_text(text) do
    text
    |> binary_part(1, byte_size(text) - 2)
    |> String.replace("\"\"", "\"")
  end

  defp quote_name(name), do: "\"" <> String.replace(name, "\"", "\"\"") <> "\""

  defp refuse(reason, message), do: throw({:qualify_refuse, reason, message})

  # ---- What the catalog must be asked ------------------------------------

  @doc """
  What a statement needs from the catalog: its unqualified function names,
  the `{schema, name}` of rowset functions it names by schema, the
  schema-qualified relations it reads (spelled as written), the functions it
  reads rows from and its casts to schema-qualified non-catalog types.
  """
  def requirements(tokens) do
    toks = List.to_tuple(tokens)
    count = tuple_size(toks)
    at = fn i -> if i >= 0 and i < count, do: elem(toks, i) end

    Enum.reduce(
      0..(count - 1)//1,
      %{
        functions: MapSet.new(),
        named: MapSet.new(),
        relations: MapSet.new(),
        rowsets: MapSet.new(),
        casts: %{}
      },
      fn k, acc ->
        token = at.(k)

        if name_token?(token) do
          acc |> require_name(at, k) |> require_cast(at, k)
        else
          acc
        end
      end
    )
  end

  defp require_name(acc, at, k) do
    previous = at.(k - 1)
    previous_up = upper(previous)
    dotted? = previous == {:punct, "."}
    chain_end = chain_end(at, k)
    next = at.(chain_end + 1)

    cond do
      dotted? ->
        acc

      next == {:punct, "("} ->
        require_call(acc, at, k, chain_end, previous, previous_up)

      chain_end == k + 2 and relation_position?(previous, previous_up) ->
        %{acc | relations: MapSet.put(acc.relations, "#{text(at.(k))}.#{text(at.(k + 2))}")}

      true ->
        acc
    end
  end

  defp relation_position?(previous, up),
    do: up in ~w(FROM JOIN INTO UPDATE ONLY LATERAL) or previous in [{:punct, "("}, {:punct, ","}]

  defp chain_end(at, k) do
    if at.(k + 1) == {:punct, "."} and name_token?(at.(k + 2)),
      do: chain_end(at, k + 2),
      else: k
  end

  defp require_call(acc, at, k, chain_end, previous, up) do
    from? = up in ~w(FROM JOIN LATERAL) or previous in [{:punct, ","}, {:punct, "("}]
    last = at.(chain_end)
    name = String.downcase(unquote_name(last))
    acc = if from?, do: %{acc | rowsets: MapSet.put(acc.rowsets, name)}, else: acc

    cond do
      # INSERT INTO "s"."t" (cols)
      up == "INTO" ->
        if chain_end == k + 2,
          do: %{acc | relations: MapSet.put(acc.relations, "#{text(at.(k))}.#{text(at.(k + 2))}")},
          else: acc

      chain_end == k and call_name?(last, at, k, previous) ->
        %{acc | functions: MapSet.put(acc.functions, name)}

      chain_end == k + 2 and from? and
          String.downcase(unquote_name(at.(k))) != "pg_catalog" ->
        %{acc | named: MapSet.put(acc.named, {unquote_name(at.(k)), unquote_name(last)})}

      true ->
        acc
    end
  end

  defp call_name?(token, at, k, previous) do
    up = upper(token)

    keyword? =
      (MapSet.member?(@keywords, up) or MapSet.member?(@constants, up)) and
        up not in @function_words

    not keyword? and not alias_position?(previous) and not cte_column_list?(at, k)
  end

  defp alias_position?({:id, _} = t),
    do: upper(t) in ~w(AS INTO TABLE UPDATE) or not keyword_token?(t)

  defp alias_position?({:qid, _}), do: true
  defp alias_position?({:punct, p}) when p in [")", "::"], do: true
  defp alias_position?(_token), do: false

  defp cte_column_list?(at, k) do
    case at_close(at, k + 2, 1) do
      nil ->
        false

      close ->
        upper(at.(close + 1)) == "AS" and
          (at.(close + 2) == {:punct, "("} or upper(at.(close + 2)) in ~w(NOT MATERIALIZED))
    end
  end

  defp at_close(at, i, depth) do
    case at.(i) do
      nil -> nil
      {:punct, "("} -> at_close(at, i + 1, depth + 1)
      {:punct, ")"} when depth == 1 -> i
      {:punct, ")"} -> at_close(at, i + 1, depth - 1)
      _ -> at_close(at, i + 1, depth)
    end
  end

  # CAST(x AS "schema"."type") to a type outside pg_catalog.
  defp require_cast(acc, at, k) do
    with true <- upper(at.(k - 1)) == "AS" or at.(k - 1) == {:punct, "::"},
         {:punct, "."} <- at.(k + 1),
         true <- name_token?(at.(k + 2)),
         schema = unquote_name(at.(k)),
         false <- String.downcase(schema) == "pg_catalog" do
      key = String.downcase(schema) <> "." <> unquote_name(at.(k + 2))
      %{acc | casts: Map.put(acc.casts, key, "#{text(at.(k))}.#{text(at.(k + 2))}")}
    else
      _ -> acc
    end
  end

  # ---- Lint --------------------------------------------------------------

  @doc """
  Whether a statement must pass through `qualify/2`: it holds something
  `lint/1` finds, or an operator named outside pg_catalog, which is verified
  rather than trusted.
  """
  def rewrite?(tokens), do: lint(tokens) != [] or foreign_operator?(tokens)

  defp foreign_operator?([{:id, word}, {:punct, "("}, schema, {:punct, "."} | rest])
       when word in ["OPERATOR", "operator"] do
    schema not in [{:id, "pg_catalog"}, {:qid, "\"pg_catalog\""}] or foreign_operator?(rest)
  end

  defp foreign_operator?([_token | rest]), do: foreign_operator?(rest)
  defp foreign_operator?([]), do: false

  @doc """
  What in `sql` would still resolve through the session: bare operators,
  constructs whose operator PostgreSQL looks up by name, unqualified
  function calls (`{:operator, op}`, `{:construct, name}`, `{:function, name}`).
  """
  def lint(sql) when is_binary(sql), do: sql |> tokenize() |> lint()

  def lint(tokens) when is_list(tokens) do
    toks = List.to_tuple(tokens)
    count = tuple_size(toks)
    at = fn i -> if i >= 0 and i < count, do: elem(toks, i) end

    {found, _skip, _frames} =
      Enum.reduce(0..(count - 1)//1, {[], -1, [nil]}, fn k, {found, skip, frames} ->
        if k <= skip do
          {found, skip, frames}
        else
          token = at.(k)
          # OPERATOR(schema.op) is explicit: skipped whole.
          if upper(token) == "OPERATOR" and at.(k + 1) == {:punct, "("} do
            {found, at_close(at, k + 2, 1) || k, frames}
          else
            {lint_token(found, at, k, token, hd(frames) == :set), skip,
             lint_frames(frames, token)}
          end
        end
      end)

    Enum.reverse(found)
  end

  # Whether each parenthesis level is in an UPDATE ... SET list: only there
  # is `name =` an assignment.
  defp lint_frames(frames, {:punct, p}) when p in ["(", "["], do: [nil | frames]
  defp lint_frames([_, next | rest], {:punct, p}) when p in [")", "]"], do: [next | rest]

  defp lint_frames([frame | rest], {:id, _} = token) do
    case upper(token) do
      "SET" -> [:set | rest]
      up when up in ~w(WHERE FROM RETURNING WHEN) -> [nil | rest]
      _ -> [frame | rest]
    end
  end

  defp lint_frames(frames, _token), do: frames

  defp lint_token(found, at, k, {:op, op}, set?) do
    previous = at.(k - 1)

    cond do
      op == "*" and
          (previous in [{:punct, "("}, {:punct, ","}, {:punct, "."}] or
             upper(previous) in ~w(SELECT DISTINCT RETURNING) or previous == nil) ->
        found

      op == "=" and set? and name_token?(previous) and
          (at.(k - 2) == {:punct, ","} or upper(at.(k - 2)) == "SET") ->
        found

      true ->
        [{:operator, op} | found]
    end
  end

  defp lint_token(found, at, k, {kind, _} = token, _set?) when kind in [:id, :qid] do
    construct = if kind == :id, do: implicit_construct(at, k, token)
    previous = at.(k - 1)

    cond do
      construct != nil ->
        [{:construct, construct} | found]

      at.(k + 1) == {:punct, "("} and previous != {:punct, "."} and
          call_name?(token, at, k, previous) ->
        [{:function, unquote_name(token)} | found]

      kind == :id and upper(token) in ["SUBSTRING", "OVERLAY"] and at.(k + 1) == {:punct, "("} and
        previous != {:punct, "."} and comma_form_tokens?(at, k + 1) ->
        [{:function, unquote_name(token)} | found]

      true ->
        found
    end
  end

  defp lint_token(found, _at, _k, _token, _set?), do: found

  defp comma_form_tokens?(at, open) do
    close = at_close(at, open + 1, 1)

    close != nil and
      not Enum.any?((open + 1)..(close - 1)//1, fn i ->
        upper(at.(i)) in ~w(FROM FOR PLACING SIMILAR)
      end)
  end

  defp implicit_construct(at, k, token) do
    up = upper(token)
    next = at.(k + 1)
    previous_up = upper(at.(k - 1))

    cond do
      up == "IN" and next == {:punct, "("} ->
        "IN"

      up == "BETWEEN" and previous_up not in ~w(ROWS RANGE GROUPS) ->
        "BETWEEN"

      up == "DISTINCT" and upper(next) == "FROM" and previous_up in ~w(IS NOT) ->
        "IS DISTINCT FROM"

      up in ~w(LIKE ILIKE SIMILAR) ->
        up

      up == "NULLIF" and next == {:punct, "("} ->
        "NULLIF"

      up == "CASE" and upper(next) != "WHEN" ->
        "simple CASE"

      true ->
        nil
    end
  end
end
