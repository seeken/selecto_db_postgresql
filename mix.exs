defmodule SelectoDBPostgreSQL.MixProject do
  use Mix.Project

  @version "0.4.10"
  @source_url "https://github.com/seeken/selecto_db_postgresql"

  def project do
    [
      app: :selecto_db_postgresql,
      version: @version,
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases(),
      cli: cli(),
      name: "SelectoDBPostgreSQL",
      description: "PostgreSQL adapter package for Selecto",
      source_url: @source_url,
      docs: docs(),
      package: package(),
      dialyzer: [
        plt_add_apps: [:mix],
        plt_core_path: "priv/plts",
        plt_file: {:no_warn, "priv/plts/dialyzer.plt"}
      ]
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      selecto_dep(),
      {:postgrex, ">= 0.0.0"},
      {:ex_doc, "~> 0.29", only: :dev, runtime: false},
      {:dialyxir, "~> 1.4", only: :dev, runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false}
    ]
  end

  defp selecto_dep do
    if use_local_ecosystem?() do
      {:selecto, path: local_selecto_path()}
    else
      {:selecto, ">= 0.4.12 and < 0.6.0"}
    end
  end

  defp local_selecto_path do
    "SELECTO_ECOSYSTEM_SELECTO_PATH"
    |> System.get_env("../selecto")
    |> Path.expand(__DIR__)
  end

  defp use_local_ecosystem? do
    case System.get_env("SELECTO_ECOSYSTEM_USE_LOCAL") do
      value when value in ["1", "true", "TRUE", "yes", "YES", "on", "ON"] -> true
      value when value in ["0", "false", "FALSE", "no", "NO", "off", "OFF"] -> false
      _ -> File.dir?(Path.expand("../selecto", __DIR__))
    end
  end

  def cli do
    [preferred_envs: [precommit: :test]]
  end

  defp aliases do
    [
      "credo.atom_audit": ["credo -C atom_audit --all-priorities --strict"],
      precommit: [
        "compile --force --warnings-as-errors",
        "format --check-formatted",
        "credo --strict",
        "credo.atom_audit",
        "test",
        "selecto_db_postgresql.verify",
        "xref graph --format cycles --label compile-connected --fail-above 0"
      ]
    ]
  end

  defp package do
    [
      files:
        ~w(lib mix.exs README.md CHANGELOG.md LICENSE docs/formal_verification.md .formatter.exs),
      licenses: ["MIT"],
      links: %{
        "Changelog" => "#{@source_url}/blob/main/CHANGELOG.md",
        "GitHub" => @source_url,
        "Selecto" => "https://github.com/seeken/selecto"
      }
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md", "CHANGELOG.md", "LICENSE", "docs/formal_verification.md"]
    ]
  end
end
