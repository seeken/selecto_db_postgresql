defmodule SelectoDBPostgreSQL.Verification.ProtocolModelsTest do
  use ExUnit.Case, async: true

  alias SelectoDBPostgreSQL.Verification.{PoolProtocol, StreamProtocol, TransactionProtocol}

  for {model, checker, state_count, invariant_count, check_count} <- [
        {"selecto_db_postgresql.transaction_protocol.v1", TransactionProtocol, 38, 3, 114},
        {"selecto_db_postgresql.stream_protocol.v2", StreamProtocol, 120, 7, 840},
        {"selecto_db_postgresql.pool_protocol.v1", PoolProtocol, 26, 3, 78}
      ] do
    test "proves #{model}" do
      report = unquote(checker).check()

      assert report.model == unquote(model)
      assert report.proof_level == :bounded_exhaustive
      assert report.state_count == unquote(state_count)
      assert report.invariant_count == unquote(invariant_count)
      assert report.check_count == unquote(check_count)
      assert report.proved?
      assert report.counterexamples == []
    end
  end
end
