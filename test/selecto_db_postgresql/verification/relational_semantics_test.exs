defmodule SelectoDBPostgreSQL.Verification.RelationalSemanticsTest do
  use ExUnit.Case, async: false

  alias SelectoDBPostgreSQL.{Adapter, Verification.RelationalSemantics}

  @moduletag :postgres

  setup do
    {:ok, connection} =
      Adapter.connect(SelectoDBPostgreSQL.Verification.ConnectionOptions.options())

    Process.unlink(connection)

    on_exit(fn ->
      if Process.alive?(connection), do: GenServer.stop(connection)
    end)

    %{connection: connection}
  end

  test "generated Selecto SQL matches the complete bounded relational model", %{
    connection: connection
  } do
    report = RelationalSemantics.check(connection)

    assert report.proof_level == :bounded_live_differential
    assert report.state_count == 232
    assert report.check_count == 232
    assert report.proved?, inspect(report.counterexamples, pretty: true, limit: :infinity)
  end
end
