Code.require_file("source_refs.exs", __DIR__)
alias SelectoDBPostgreSQL.CI.SourceRefs

root = Path.expand("..", __DIR__)
core = SourceRefs.core!(root)
output = System.fetch_env!("CI_DIAGNOSTICS_PATH")
otp_release = to_string(:erlang.system_info(:otp_release))

otp_version =
  :code.root_dir()
  |> to_string()
  |> Path.join("releases/#{otp_release}/OTP_VERSION")
  |> File.read!()
  |> String.trim()

true = Regex.match?(~r/\A\d+(?:\.\d+)*\z/, otp_version)

metadata = %{
  schema: "selecto.postgresql-ci-stage.v1",
  stage: "diagnostics",
  status: "passed",
  exit_code: 0,
  core_ref: core,
  elixir: System.version(),
  otp: otp_version,
  postgrex: to_string(Application.spec(:postgrex, :vsn)),
  sources: [
    SourceRefs.source!(
      SelectoDBPostgreSQL.Adapter,
      "seeken/selecto_db_postgresql",
      System.get_env("GITHUB_SHA"),
      root
    ),
    SourceRefs.source!(Selecto, "seeken/selecto", core, Path.join(root, "selecto"))
  ]
}

metadata =
  case System.get_env("CI_POSTGRES_MAJOR") do
    nil ->
      metadata

    major when major in ~w(13 14 15 16 17 18) ->
      {:ok, connection} =
        Postgrex.start_link(SelectoDBPostgreSQL.Verification.ConnectionOptions.options())

      try do
        {:ok, %{rows: [[version, number]]}} =
          Postgrex.query(
            connection,
            "SELECT current_setting('server_version'), current_setting('server_version_num')",
            []
          )

        true = Regex.match?(~r/\A\d+(?:\.\d+)*(?: \([A-Za-z0-9 .+~_-]{1,120}\))?\z/, version)
        true = div(String.to_integer(number), 10_000) == String.to_integer(major)
        Map.put(metadata, :postgresql, %{major: major, version: version, version_num: number})
      after
        if Process.alive?(connection), do: GenServer.stop(connection)
      end
  end

File.write!(output, Jason.encode_to_iodata!(metadata, pretty: true))
IO.puts("Actual runtime, clean loaded sources and selected server identity verified.")
