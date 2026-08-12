defmodule SelectoDBPostgreSQL.Verification.ConnectionOptions do
  @moduledoc false

  @url_env "SELECTO_POSTGRES_TEST_URL"
  @default_port 5432
  @ssl_modes ["require", "verify-ca", "verify-full"]

  @spec options() :: keyword()
  def options do
    case System.get_env(@url_env) do
      nil -> environment_options()
      url -> from_url!(url)
    end
  end

  @doc "Parses a PostgreSQL URL into validated Postgrex connection options."
  @spec from_url!(String.t()) :: keyword()
  def from_url!(url) when is_binary(url) do
    with {:ok, uri} <- URI.new(url),
         true <- uri.scheme in ["postgres", "postgresql"],
         host when is_binary(host) and host != "" <- uri.host,
         true <- is_nil(uri.fragment),
         {:ok, port} <- port(uri.port),
         {:ok, username, password} <- credentials(uri.userinfo),
         {:ok, database} <- database(uri.path),
         {:ok, ssl} <- ssl(uri.query) do
      [
        hostname: host,
        port: port,
        username: username,
        password: password,
        database: database
      ]
      |> maybe_put_ssl(ssl)
    else
      _ -> invalid_url!()
    end
  rescue
    _exception -> invalid_url!()
  end

  def from_url!(_url), do: invalid_url!()

  defp environment_options do
    hostname = env("SELECTO_POSTGRES_TEST_HOST", "PGHOST", "localhost")
    username = env("SELECTO_POSTGRES_TEST_USER", "PGUSER", "postgres")
    password = env("SELECTO_POSTGRES_TEST_PASSWORD", "PGPASSWORD", "postgres")
    database = env("SELECTO_POSTGRES_TEST_DATABASE", "PGDATABASE", "postgres")

    unless Enum.all?([hostname, username, password, database], &non_empty?/1) do
      raise ArgumentError, "PostgreSQL test connection settings cannot be empty"
    end

    [
      hostname: hostname,
      port: port_from_environment!(),
      username: username,
      password: password,
      database: database
    ]
  end

  defp credentials(userinfo) when is_binary(userinfo) and userinfo != "" do
    case String.split(userinfo, ":", parts: 2) do
      [encoded_username] ->
        with {:ok, username} <- decode_non_empty(encoded_username) do
          {:ok, username, nil}
        end

      [encoded_username, encoded_password] ->
        with {:ok, username} <- decode_non_empty(encoded_username),
             {:ok, password} <- decode(encoded_password) do
          {:ok, username, password}
        end

      _ ->
        :error
    end
  end

  defp credentials(_userinfo), do: :error

  defp database("/" <> encoded_database) when encoded_database != "" do
    with false <- String.contains?(encoded_database, "/"),
         {:ok, database} <- decode_non_empty(encoded_database),
         false <- String.contains?(database, "/") do
      {:ok, database}
    else
      _ -> :error
    end
  end

  defp database(_path), do: :error

  defp port(nil), do: {:ok, @default_port}
  defp port(value) when value in 1..65_535, do: {:ok, value}
  defp port(_value), do: :error

  defp ssl(nil), do: {:ok, nil}

  defp ssl(query) when is_binary(query) do
    case URI.query_decoder(query) |> Enum.to_list() do
      [] -> {:ok, nil}
      [{"sslmode", "disable"}] -> {:ok, false}
      [{"sslmode", mode}] when mode in @ssl_modes -> {:ok, true}
      _ -> :error
    end
  rescue
    _exception -> :error
  end

  defp maybe_put_ssl(options, nil), do: options
  defp maybe_put_ssl(options, value), do: Keyword.put(options, :ssl, value)

  defp decode_non_empty(encoded) do
    case decode(encoded) do
      {:ok, value} when value != "" -> {:ok, value}
      _other -> :error
    end
  end

  defp decode(encoded) do
    if valid_percent_encoding?(encoded), do: {:ok, URI.decode(encoded)}, else: :error
  end

  defp valid_percent_encoding?(<<>>), do: true

  defp valid_percent_encoding?(<<"%", high, low, rest::binary>>)
       when high in ?0..?9 or high in ?A..?F or high in ?a..?f do
    if low in ?0..?9 or low in ?A..?F or low in ?a..?f do
      valid_percent_encoding?(rest)
    else
      false
    end
  end

  defp valid_percent_encoding?(<<"%", _rest::binary>>), do: false
  defp valid_percent_encoding?(<<_byte, rest::binary>>), do: valid_percent_encoding?(rest)

  defp port_from_environment! do
    value = env("SELECTO_POSTGRES_TEST_PORT", "PGPORT", Integer.to_string(@default_port))

    case Integer.parse(value) do
      {port, ""} when port in 1..65_535 -> port
      _ -> raise ArgumentError, "PostgreSQL test port must be an integer from 1 to 65535"
    end
  end

  defp non_empty?(value), do: is_binary(value) and value != ""

  defp env(primary, fallback, default),
    do: System.get_env(primary) || System.get_env(fallback) || default

  defp invalid_url! do
    raise ArgumentError,
          "#{@url_env} must be a postgres:// or postgresql:// URL with a valid host, port, username, and database"
  end
end
