defmodule SelectoDBPostgreSQL.EctoTransactionIntegrationTest do
  # The transaction Selecto joins or opens is the connection's real one, on an
  # Ecto repository too: Repo.in_transaction?/0 is false under the SQL Sandbox
  # and after a raw BEGIN, and Selecto must never COMMIT what it did not open.
  use ExUnit.Case, async: false

  alias Ecto.Adapters.SQL
  alias Ecto.Adapters.SQL.Sandbox
  alias Selecto.Write.Command
  alias SelectoDBPostgreSQL.Adapter
  alias SelectoDBPostgreSQL.Verification.ConnectionOptions

  @moduletag :postgres

  defmodule PlainRepo do
    use Ecto.Repo, otp_app: :selecto_db_postgresql, adapter: Ecto.Adapters.Postgres
  end

  defmodule SandboxRepo do
    use Ecto.Repo, otp_app: :selecto_db_postgresql, adapter: Ecto.Adapters.Postgres
  end

  setup_all do
    {:ok, _} = Application.ensure_all_started(:ecto_sql)
    options = ConnectionOptions.options()
    Application.put_env(:selecto_db_postgresql, PlainRepo, Keyword.put(options, :pool_size, 1))

    Application.put_env(
      :selecto_db_postgresql,
      SandboxRepo,
      Keyword.merge(options, pool_size: 2, pool: Sandbox)
    )

    {:ok, plain} = PlainRepo.start_link()
    {:ok, sandbox} = SandboxRepo.start_link()
    Process.unlink(plain)
    Process.unlink(sandbox)
    Sandbox.mode(SandboxRepo, :manual)
    :ok
  end

  setup do
    table = "selecto_ecto_#{System.unique_integer([:positive])}"

    SQL.query!(
      PlainRepo,
      "CREATE TABLE public.#{table} (id int PRIMARY KEY, org text, note text)"
    )

    SQL.query!(PlainRepo, "INSERT INTO public.#{table} VALUES (1, 't1', 'n')")
    on_exit(fn -> SQL.query!(PlainRepo, "DROP TABLE public.#{table}") end)
    %{table: table}
  end

  defp domain(table) do
    %{
      name: "E",
      source: %{
        source_table: table,
        primary_key: :id,
        fields: [:id, :org, :note],
        redact_fields: [],
        columns: %{id: %{type: :integer}, org: %{type: :string}, note: %{type: :string}},
        associations: %{}
      },
      schemas: %{},
      joins: %{}
    }
  end

  defp read(repo, table) do
    domain(table)
    |> Selecto.configure(repo, adapter: Adapter, validate: false)
    |> Selecto.select(["id"])
    |> Selecto.order_by(["id"])
    |> Selecto.execute()
  end

  defp write(repo, table, id) do
    scope = {:eq, {:field, "org"}, {:literal, "t1"}}

    {:ok, command} =
      Command.new(%{
        operation: :update,
        relation: table,
        predicate: {:and, [{:eq, {:field, "id"}, {:literal, id}}, scope]},
        assignments: [%{field: "note", value: {:literal, "new"}}],
        expected_cardinality: :many,
        metadata: %{post_image: %{predicate: scope, key: ["id"]}}
      })

    Adapter.execute_write_unsafe(repo, command, [])
  end

  defp count(table, id) do
    %{rows: [[n]]} =
      SQL.query!(PlainRepo, "SELECT count(*) FROM public.#{table} WHERE id = #{id}")

    n
  end

  defp note(table) do
    %{rows: [[note]]} = SQL.query!(PlainRepo, "SELECT note FROM public.#{table} WHERE id = 1")
    note
  end

  test "a read under the Sandbox sees the test's rows, which end with it", %{table: table} do
    Sandbox.checkout(SandboxRepo)
    SQL.query!(SandboxRepo, "INSERT INTO public.#{table} VALUES (555, 't1', 's')")

    assert {:ok, {[[1], [555]], _, _}} = read(SandboxRepo, table)
    assert %{rows: [[2]]} = SQL.query!(SandboxRepo, "SELECT count(*) FROM public.#{table}")
    Sandbox.checkin(SandboxRepo)

    assert count(table, 555) == 0
  end

  test "a write under the Sandbox applies, and ends with the test", %{table: table} do
    Sandbox.checkout(SandboxRepo)
    assert {:ok, %{affected_rows: 1}} = write(SandboxRepo, table, 1)
    assert %{rows: [["new"]]} = SQL.query!(SandboxRepo, "SELECT note FROM public.#{table}")
    assert {:ok, {[[1]], _, _}} = read(SandboxRepo, table)
    Sandbox.checkin(SandboxRepo)

    assert note(table) == "n"
  end

  test "a read after a raw BEGIN joins the host's transaction", %{table: table} do
    {result, status} =
      PlainRepo.checkout(fn ->
        SQL.query!(PlainRepo, "BEGIN")
        SQL.query!(PlainRepo, "INSERT INTO public.#{table} VALUES (777, 't1', 'h')")
        result = read(PlainRepo, table)
        status = SQL.query(PlainRepo, "ROLLBACK")
        {result, status}
      end)

    assert {:ok, {[[1], [777]], _, _}} = result
    assert {:ok, _} = status
    assert count(table, 777) == 0
  end

  test "a write after a raw BEGIN joins the host's transaction", %{table: table} do
    PlainRepo.checkout(fn ->
      SQL.query!(PlainRepo, "BEGIN")
      assert {:ok, %{affected_rows: 1}} = write(PlainRepo, table, 1)
      SQL.query!(PlainRepo, "ROLLBACK")
    end)

    assert note(table) == "n"
  end

  test "outside any transaction a read and a write take their own", %{table: table} do
    assert {:ok, {[[1]], _, _}} = read(PlainRepo, table)
    assert {:ok, %{affected_rows: 1}} = write(PlainRepo, table, 1)
    assert note(table) == "new"
  end

  test "inside Repo.transaction a read joins, and a rollback takes the row", %{table: table} do
    PlainRepo.transaction(fn ->
      SQL.query!(PlainRepo, "INSERT INTO public.#{table} VALUES (888, 't1', 'h')")
      assert {:ok, {[[1], [888]], _, _}} = read(PlainRepo, table)
      PlainRepo.rollback(:undo)
    end)

    assert count(table, 888) == 0
  end
end
