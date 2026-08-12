ExUnit.start()
ExUnit.configure(exclude: [postgres: true])

defmodule SelectoDBPostgreSQL.PostgresTestConnection do
  @moduledoc false

  @url_env "SELECTO_POSTGRES_TEST_URL"

  def options do
    case System.get_env(@url_env) do
      nil -> environment_options()
      url -> from_url!(url)
    end
  end

  def from_url!(url) when is_binary(url) do
    uri = URI.parse(url)

    with true <- uri.scheme in ["postgres", "postgresql"],
         host when is_binary(host) and host != "" <- uri.host,
         {:ok, port} <- port(uri.port),
         {:ok, username, password} <- credentials(uri.userinfo),
         {:ok, database} <- database(uri.path) do
      [
        hostname: host,
        port: port,
        username: username,
        password: password,
        database: database
      ]
      |> maybe_add_ssl(uri.query)
    else
      _ ->
        invalid_url!()
    end
  rescue
    _exception -> invalid_url!()
  end

  def from_url!(_url), do: raise(ArgumentError, "#{@url_env} must be a PostgreSQL URL")

  defp environment_options do
    [
      hostname: env("SELECTO_POSTGRES_TEST_HOST", "PGHOST", "localhost"),
      port: port_from_env(),
      username: env("SELECTO_POSTGRES_TEST_USER", "PGUSER", "postgres"),
      password: env("SELECTO_POSTGRES_TEST_PASSWORD", "PGPASSWORD", "postgres"),
      database: env("SELECTO_POSTGRES_TEST_DATABASE", "PGDATABASE", "postgres")
    ]
  end

  defp credentials(userinfo) when is_binary(userinfo) and userinfo != "" do
    case String.split(userinfo, ":", parts: 2) do
      [username] when username != "" ->
        {:ok, URI.decode(username), nil}

      [username, password] when username != "" ->
        {:ok, URI.decode(username), URI.decode(password)}

      _ ->
        :error
    end
  end

  defp credentials(_userinfo), do: :error

  defp database("/" <> encoded_database) when encoded_database != "" do
    database = URI.decode(encoded_database)

    if String.contains?(database, "/"), do: :error, else: {:ok, database}
  end

  defp database(_path), do: :error

  defp port(nil), do: {:ok, 5432}
  defp port(port) when port in 1..65_535, do: {:ok, port}
  defp port(_port), do: :error

  defp maybe_add_ssl(options, query) do
    case URI.decode_query(query || "") do
      %{"sslmode" => sslmode} when sslmode in ["require", "verify-ca", "verify-full"] ->
        Keyword.put(options, :ssl, true)

      %{"sslmode" => "disable"} ->
        Keyword.put(options, :ssl, false)

      _ ->
        options
    end
  end

  defp port_from_env do
    case Integer.parse(env("SELECTO_POSTGRES_TEST_PORT", "PGPORT", "5432")) do
      {port, ""} when port in 1..65_535 -> port
      _ -> raise ArgumentError, "PostgreSQL test port must be an integer from 1 to 65535"
    end
  end

  defp env(primary, fallback, default) do
    System.get_env(primary) || System.get_env(fallback) || default
  end

  defp invalid_url! do
    raise ArgumentError,
          "#{@url_env} must be a postgres:// or postgresql:// URL with host, username, and database"
  end
end
