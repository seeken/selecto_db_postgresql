defmodule SelectoDBPostgreSQL.HandBuilt do
  @moduledoc false

  # Hand-built read text: SQL a host passed to a Selecto read path, wholly the
  # host's (R68, R71). Selecto never rewrites it; it checks only that it is a
  # read by its first word.

  alias Selecto.Write.Error

  @reads ~w(SELECT WITH VALUES TABLE SHOW EXPLAIN)

  @doc """
  `:ok` when hand-built text is a read by its first word, past comments and
  opening parentheses, or the typed refusal (`hand_built_not_a_read`).

  The word is what stops COMMIT or ROLLBACK ending the read's own transaction;
  the driver's extended protocol takes one statement, so nothing can follow
  it. Comments nest, as they do on the server. This stops honest mistakes,
  not evasion.
  """
  def read(sql) do
    word = sql |> IO.iodata_to_binary() |> first_word()

    if word != nil and String.upcase(word) in @reads,
      do: :ok,
      else: refuse(:hand_built_not_a_read, "hand-built SQL on a read path must be a read")
  end

  @doc """
  `:ok` when hand-built read text may run inside the host's transaction, or the
  typed refusal (`hand_built_in_host_transaction`).

  In the host's transaction nothing would stop a write, so text whose words
  could write refuses before anything is sent: INSERT, UPDATE, DELETE, MERGE,
  TRUNCATE or INTO, as a data-modifying WITH or SELECT INTO uses, and EXPLAIN
  ANALYZE of one. A row lock (FOR UPDATE, FOR NO KEY UPDATE, FOR SHARE) is
  allowed, as are sequences and the host's own functions and views: they are
  the host's. The check reads words, not meaning; it stops honest mistakes,
  not evasion.
  """
  def host_transaction(sql) do
    if may_write?(sql),
      do:
        refuse(
          :hand_built_in_host_transaction,
          "a hand-built statement that may write cannot run inside the host's transaction"
        ),
      else: :ok
  end

  defp may_write?(sql) do
    text = words(sql)
    start = text |> first_word() |> Kernel.||("") |> String.upcase()

    cond do
      start == "SHOW" ->
        false

      start == "EXPLAIN" and not Regex.match?(~r/\bANALY[SZ]E\b/i, text) ->
        false

      true ->
        text = Regex.replace(~r/\bFOR\s+(?:NO\s+KEY\s+)?UPDATE\b/i, text, " ")
        Regex.match?(~r/\b(?:INSERT|UPDATE|DELETE|MERGE|TRUNCATE|INTO)\b/i, text)
    end
  end

  @doc """
  Whether a statement's words lock rows (FOR UPDATE, FOR NO KEY UPDATE, FOR
  SHARE, FOR KEY SHARE): such a read cannot run READ ONLY. Read from words, with
  string literals, quoted identifiers and comments blanked.
  """
  def row_lock?(sql),
    do: Regex.match?(~r/\bFOR\s+(?:NO\s+KEY\s+UPDATE|UPDATE|KEY\s+SHARE|SHARE)\b/i, words(sql))

  # The statement with its string literals, quoted identifiers, dollar quotes
  # and comments blanked. An E'' string with a backslash-escaped quote can end
  # early, leaving words of its own in: they can only count against the text.
  defp words(sql) do
    Regex.replace(
      ~r/'(?:[^']|'')*'|"(?:[^"]|"")*"|\$([A-Za-z_][A-Za-z0-9_]*)?\$.*?\$\1\$|--[^\n\r]*|\/\*.*?\*\//s,
      IO.iodata_to_binary(sql),
      " "
    )
  end

  defp first_word(<<c, rest::binary>>) when c in [?\s, ?\t, ?\n, ?\r, ?\f, ?\v, ?(],
    do: first_word(rest)

  defp first_word(<<"--", rest::binary>>) do
    case :binary.match(rest, ["\n", "\r"]) do
      {at, 1} -> rest |> binary_part(at, byte_size(rest) - at) |> first_word()
      :nomatch -> nil
    end
  end

  defp first_word(<<"/*", rest::binary>>) do
    case past_comment(rest, 1) do
      nil -> nil
      tail -> first_word(tail)
    end
  end

  defp first_word(text) do
    case Regex.run(~r/\A[A-Za-z]+(?![A-Za-z0-9_$])/, text) do
      [word] -> word
      nil -> nil
    end
  end

  defp past_comment(rest, 0), do: rest
  defp past_comment(<<>>, _depth), do: nil
  defp past_comment(<<"/*", rest::binary>>, depth), do: past_comment(rest, depth + 1)
  defp past_comment(<<"*/", rest::binary>>, depth), do: past_comment(rest, depth - 1)
  defp past_comment(<<_c, rest::binary>>, depth), do: past_comment(rest, depth)

  defp refuse(reason, message) do
    {:error,
     Error.new(:query_enforcement, "PostgreSQL refuses a statement: " <> message,
       details: %{code: :query_rule_not_evaluable, reason: reason, adapter: :postgresql}
     )}
  end
end
