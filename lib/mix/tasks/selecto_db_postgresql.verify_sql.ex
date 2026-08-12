defmodule Mix.Tasks.SelectoDbPostgresql.VerifySql do
  use Mix.Task

  @shortdoc "Runs bounded live Selecto/PostgreSQL semantic differential checks"
  @requirements ["app.start"]

  @moduledoc """
  Runs the bounded live relational differential verifier.

      mix selecto_db_postgresql.verify_sql
      mix selecto_db_postgresql.verify_sql --output tmp/postgresql-semantics.json

  Connection settings use `SELECTO_POSTGRES_TEST_URL` or the existing
  `SELECTO_POSTGRES_TEST_*`/libpq environment variables.
  """

  @impl Mix.Task
  def run(args) do
    {opts, rest, invalid} = OptionParser.parse(args, strict: [output: :string])

    if rest != [] or invalid != [] do
      Mix.raise("usage: mix selecto_db_postgresql.verify_sql [--output PATH]")
    end

    connection_options = SelectoDBPostgreSQL.Verification.ConnectionOptions.options()

    case SelectoDBPostgreSQL.Adapter.connect(connection_options) do
      {:ok, connection} ->
        Process.unlink(connection)

        try do
          report = SelectoDBPostgreSQL.Verification.RelationalSemantics.check(connection)
          print_report(report)
          maybe_write(report, opts[:output])

          unless report.proved? do
            Mix.raise("PostgreSQL relational semantic differential verification failed")
          end
        after
          if Process.alive?(connection), do: GenServer.stop(connection)
        end

      {:error, reason} ->
        Mix.raise("could not connect for live relational verification: #{inspect(reason)}")
    end
  end

  defp print_report(report) do
    status = if report.proved?, do: "PROVED", else: "FAILED"

    Mix.shell().info(
      "#{status} #{report.model}: #{report.check_count} live differential checks " <>
        "(proof=#{report.proof_level})"
    )

    Enum.each(report.counterexamples, fn counterexample ->
      Mix.shell().error("counterexample: #{inspect(counterexample, pretty: true)}")
    end)
  end

  defp maybe_write(_report, nil), do: :ok

  defp maybe_write(report, path) do
    path = Path.expand(path)
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, Jason.encode_to_iodata!(report, pretty: true))
    Mix.shell().info("Wrote verification artifact to #{path}")
  end
end
