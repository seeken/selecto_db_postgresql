defmodule Mix.Tasks.SelectoDbPostgresql.Verify do
  use Mix.Task

  @shortdoc "Runs the PostgreSQL adapter bounded verification suite"

  @moduledoc """
  Runs the PostgreSQL adapter bounded verification suite.

      mix selecto_db_postgresql.verify
      mix selecto_db_postgresql.verify --output tmp/postgresql-verification.json

  Exit status is non-zero if any model produces a counterexample.
  """

  @impl Mix.Task
  def run(args) do
    {opts, rest, invalid} = OptionParser.parse(args, strict: [output: :string])

    if rest != [] or invalid != [] do
      Mix.raise("usage: mix selecto_db_postgresql.verify [--output PATH]")
    end

    reports = [
      SelectoDBPostgreSQL.Verification.AdapterSafety.check(),
      SelectoDBPostgreSQL.Verification.TransactionProtocol.check(),
      SelectoDBPostgreSQL.Verification.StreamProtocol.check(),
      SelectoDBPostgreSQL.Verification.PoolProtocol.check()
    ]

    Enum.each(reports, &print_report/1)
    maybe_write(reports, opts[:output])

    unless Enum.all?(reports, & &1.proved?) do
      Mix.raise("PostgreSQL adapter formal verification found counterexamples")
    end
  end

  defp print_report(report) do
    status = if report.proved?, do: "PROVED", else: "FAILED"

    Mix.shell().info(
      "#{status} #{report.model}: #{report.check_count} checks " <>
        "(#{report.state_count} states x #{report.invariant_count} invariants, " <>
        "proof=#{report.proof_level})"
    )

    Enum.each(report.counterexamples, fn counterexample ->
      Mix.shell().error("counterexample: #{inspect(counterexample, pretty: true)}")
    end)
  end

  defp maybe_write(_reports, nil), do: :ok

  defp maybe_write(reports, path) do
    artifact = %{
      format: "selecto.formal_verification_suite",
      format_version: 1,
      generated_at: DateTime.utc_now() |> DateTime.to_iso8601(),
      proved?: Enum.all?(reports, & &1.proved?),
      reports: reports
    }

    path = Path.expand(path)
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, Jason.encode_to_iodata!(artifact, pretty: true))
    Mix.shell().info("Wrote verification artifact to #{path}")
  end
end
