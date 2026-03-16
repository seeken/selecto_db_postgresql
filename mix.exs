defmodule SelectoDBPostgreSQL.MixProject do
  use Mix.Project

  @version "0.4.0"
  @source_url "https://github.com/seeken/selecto_db_postgresql"

  def project do
    [
      app: :selecto_db_postgresql,
      version: @version,
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      name: "SelectoDBPostgreSQL",
      description: "PostgreSQL adapter package for Selecto",
      source_url: @source_url,
      package: package()
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
      selecto_components_dep(),
      {:postgrex, ">= 0.0.0"},
      {:ex_doc, "~> 0.29", only: :dev, runtime: false}
    ]
  end

  defp selecto_dep do
    if use_local_ecosystem?() do
      {:selecto, path: "../selecto"}
    else
      {:selecto, ">= 0.4.0 and < 0.5.0"}
    end
  end

  defp use_local_ecosystem? do
    case System.get_env("SELECTO_ECOSYSTEM_USE_LOCAL") do
      value when value in ["1", "true", "TRUE", "yes", "YES", "on", "ON"] -> true
      _ -> false
    end
  end

  defp selecto_components_dep do
    if use_local_ecosystem?() do
      {:selecto_components, path: "../selecto_components", only: :test}
    else
      {:selecto_components, github: "seeken/selecto_components", branch: "main", only: :test}
    end
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{
        "GitHub" => @source_url,
        "Selecto" => "https://github.com/seeken/selecto"
      }
    ]
  end
end
