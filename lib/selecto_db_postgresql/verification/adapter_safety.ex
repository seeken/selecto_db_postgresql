defmodule SelectoDBPostgreSQL.Verification.AdapterSafety do
  @moduledoc """
  Bounded verification of fail-closed PostgreSQL adapter boundaries.

  This model checks portable command validation, normalized identifier
  collisions, publication-name safety, and named-connection rejection without
  opening a database connection or dispatching SQL.
  """

  alias Selecto.Verification.BoundedModel
  alias Selecto.Write.{Command, Error}
  alias SelectoDBPostgreSQL.Adapter

  @model "selecto_db_postgresql.adapter_safety.v1"

  @spec check() :: BoundedModel.report()
  def check do
    BoundedModel.check(@model, states(), invariants())
  end

  defp states do
    for command_shape <- [:valid, :malformed, :duplicate_assignment, :duplicate_returning],
        publication_name <- [:safe, :malicious],
        connection_shape <- [:unregistered_name, :unsupported_url_option] do
      %{
        command_shape: command_shape,
        publication_name: publication_name,
        connection_shape: connection_shape
      }
    end
  end

  defp invariants do
    [
      {"command boundary returns structured results", &command_boundary/1},
      {"publication boundary dispatches only safe quoted SQL", &publication_boundary/1},
      {"unregistered named connections fail before Postgrex dispatch", &connection_boundary/1}
    ]
  end

  defp command_boundary(state) do
    result = Adapter.preview_write(:preview_only, command(state.command_shape))

    case {state.command_shape, result} do
      {:valid, {:ok, %{statements: [_statement]}}} ->
        :ok

      {:malformed, {:error, %Error{type: :invalid_command}}} ->
        :ok

      {:duplicate_assignment,
       {:error,
        %Error{type: :invalid_command, details: %{code: :duplicate_assignment_identifier}}}} ->
        :ok

      {:duplicate_returning,
       {:error, %Error{type: :invalid_command, details: %{code: :duplicate_returning_identifier}}}} ->
        :ok

      other ->
        {:error, {:unexpected_command_result, other}}
    end
  end

  defp publication_boundary(state) do
    capture = capture_connection()
    result = Adapter.refresh_materialized_view(capture, publication_name(state.publication_name))

    case {state.publication_name, result, captured_query()} do
      {:safe, {:ok, _result}, ~s(REFRESH MATERIALIZED VIEW "reporting"."daily_rollup";)} ->
        :ok

      {:malicious, {:error, %{code: :invalid_sql_identifier}}, nil} ->
        :ok

      other ->
        {:error, {:unexpected_publication_result, other}}
    end
  end

  defp connection_boundary(%{connection_shape: :unregistered_name}) do
    name = :selecto_db_postgresql_unregistered_verification_connection

    case Adapter.execute(name, "select 1", [], []) do
      {:error, {:invalid_connection, ^name}} -> :ok
      other -> {:error, {:unexpected_connection_result, other}}
    end
  end

  defp connection_boundary(%{connection_shape: :unsupported_url_option}) do
    case Adapter.connect(url: "postgres://verification.invalid/database") do
      {:error, {:invalid_connection_options, :url_option_not_supported}} -> :ok
      other -> {:error, {:unexpected_connection_result, other}}
    end
  end

  defp command(:valid) do
    %Command{
      operation: :insert,
      relation: :items,
      assignments: [%{field: :name, value: {:literal, "item"}}]
    }
  end

  defp command(:malformed) do
    %Command{operation: :insert, relation: :items, assignments: [:malformed]}
  end

  defp command(:duplicate_assignment) do
    %Command{
      operation: :insert,
      relation: :items,
      assignments: [
        %{field: :name, value: {:literal, "first"}},
        %{field: "name", value: {:literal, "second"}}
      ]
    }
  end

  defp command(:duplicate_returning) do
    %{command(:valid) | returning: [:id, "id"]}
  end

  defp publication_name(:safe), do: "reporting.daily_rollup"
  defp publication_name(:malicious), do: "reporting.safe; DROP TABLE accounts; --"

  defp capture_connection do
    owner = self()

    %{
      query_fun: fn query, _params, _opts ->
        send(owner, {__MODULE__, :query, query})
        {:ok, %{rows: [], columns: []}}
      end
    }
  end

  defp captured_query do
    receive do
      {__MODULE__, :query, query} -> query
    after
      0 -> nil
    end
  end
end
