defmodule Mix.Tasks.SelectoDbPostgresql.Verify do
  use Mix.Task

  @shortdoc "Runs the PostgreSQL adapter bounded verification suite"

  @moduledoc """
  Runs the PostgreSQL adapter bounded verification suite.

      mix selecto_db_postgresql.verify
      mix selecto_db_postgresql.verify --output tmp/postgresql-verification.json

  Exit status is non-zero if the model produces a counterexample.
  """

  @impl Mix.Task
  def run(args) do
    {opts, rest, invalid} = OptionParser.parse(args, strict: [output: :string])

    if rest != [] or invalid != [] do
      Mix.raise("usage: mix selecto_db_postgresql.verify [--output PATH]")
    end

    report = SelectoDBPostgreSQL.Verification.AdapterSafety.check()
    status = if report.proved?, do: "PROVED", else: "FAILED"

    Mix.shell().info(
      "#{status} #{report.model}: #{report.check_count} checks " <>
        "(#{report.state_count} states x #{report.invariant_count} invariants, " <>
        "proof=#{report.proof_level})"
    )

    Enum.each(report.counterexamples, fn counterexample ->
      Mix.shell().error("counterexample: #{inspect(counterexample, pretty: true)}")
    end)

    maybe_write(report, opts[:output])

    unless report.proved? do
      Mix.raise("PostgreSQL adapter formal verification found counterexamples")
    end
  end

  defp maybe_write(_report, nil), do: :ok

  defp maybe_write(report, path) do
    artifact = %{
      format: "selecto.formal_verification_suite",
      format_version: 1,
      generated_at: DateTime.utc_now() |> DateTime.to_iso8601(),
      proved?: report.proved?,
      reports: [report]
    }

    path = Path.expand(path)
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, Jason.encode_to_iodata!(artifact, pretty: true))
    Mix.shell().info("Wrote verification artifact to #{path}")
  end
end
