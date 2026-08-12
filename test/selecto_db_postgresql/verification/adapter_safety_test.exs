defmodule SelectoDBPostgreSQL.Verification.AdapterSafetyTest do
  use ExUnit.Case, async: true

  alias SelectoDBPostgreSQL.Verification.AdapterSafety

  test "proves bounded adapter safety invariants" do
    report = AdapterSafety.check()

    assert report.proof_level == :bounded_exhaustive
    assert report.state_count == 16
    assert report.invariant_count == 3
    assert report.check_count == 48
    assert report.proved?
    assert report.counterexamples == []
  end
end
