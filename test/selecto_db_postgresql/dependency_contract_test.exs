defmodule SelectoDBPostgreSQL.DependencyContractTest do
  use ExUnit.Case, async: true

  test "published Postgrex requirement admits the locked release and bounds the supported series" do
    {:postgrex, requirement} =
      Enum.find(Mix.Project.config()[:deps], &(elem(&1, 0) == :postgrex))

    {lock, _} = Code.eval_file("mix.lock")
    {:hex, :postgrex, locked_version, _, _, _, _, _} = lock[:postgrex]
    assert Version.match?(locked_version, requirement)
    assert Version.match?("0.22.0", requirement)
    refute Version.match?("0.21.1", requirement)
    refute Version.match?("0.23.0", requirement)
    refute Version.match?("1.0.0", requirement)
  end
end
