defmodule SelectoDBPostgreSQL.PostImage do
  @moduledoc false
  alias Selecto.Write.{Command, Error}

  # Evaluate the scope in the write's own RETURNING, after storage coercion,
  # defaults and BEFORE triggers. Host triggers, rules and owner DDL are
  # outside the governed-write boundary.
  # Keep the private physical-key receipt convention: a written table OID is
  # returned only when its tuple satisfies the scope. No key is re-read.
  @truth "selecto_post_image_key_0"

  def prepare(%Command{metadata: %{post_image: %{predicate: _}}, operation: operation} = command)
      when operation in [:insert, :update, :upsert], do: {:post_image, command}

  def prepare(command), do: {:plain, command}

  def returning(statement, command, compile_predicate) do
    with {:ok, scope} <-
           compile_predicate.(
             untyped_text(command.metadata.post_image.predicate),
             length(statement.params)
           ) do
      prefix = if command.returning == :none, do: " RETURNING ", else: ", "

      {:ok,
       %{
         statement
         | text:
             statement.text <>
               prefix <> "CASE WHEN (#{scope.text}) THEN tableoid ELSE NULL END AS #{@truth}",
           params: statement.params ++ scope.params
       }}
    end
  end

  def verify(%{columns: columns, rows: rows} = result) when is_list(columns) and is_list(rows) do
    index = length(columns) - 1

    cond do
      List.last(columns) != @truth ->
        refused(:query_rule_not_evaluable, "cannot read the written rows' scope")

      Enum.any?(rows, &(not is_integer(Enum.at(&1, index)) or Enum.at(&1, index) <= 0)) ->
        refused(:query_rule_violation, "the written rows left the trusted write scope")

      true ->
        kept = List.delete_at(columns, index)

        {:ok,
         %{
           result
           | columns: kept,
             rows: if(kept == [], do: [], else: Enum.map(rows, &List.delete_at(&1, index)))
         }}
    end
  end

  def verify(_), do: refused(:query_rule_not_evaluable, "cannot read the written rows' scope")

  # Text literals of the scope compare as untyped SQL literals, as the host's
  # own read compares them (WriteCompiler.compile_value/3 `:untyped_text`).
  defp untyped_text({:literal, value}) when is_binary(value), do: {:untyped_text, value}

  defp untyped_text(tuple) when is_tuple(tuple),
    do: tuple |> Tuple.to_list() |> Enum.map(&untyped_text/1) |> List.to_tuple()

  defp untyped_text(list) when is_list(list), do: Enum.map(list, &untyped_text/1)
  defp untyped_text(value), do: value

  defp refused(code, message),
    do:
      {:error,
       Error.new(:query_enforcement, "PostgreSQL " <> message,
         details: %{code: code, adapter: :postgresql}
       )}
end
