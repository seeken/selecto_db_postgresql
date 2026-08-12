defmodule SelectoDBPostgreSQL.Verification.ConnectionOptionsTest do
  use ExUnit.Case, async: false

  alias SelectoDBPostgreSQL.Verification.ConnectionOptions

  test "parses PostgreSQL URLs into current Postgrex options" do
    options =
      ConnectionOptions.from_url!(
        "postgresql://test%40user:p%3Ass@db.example.test:55432/selecto%5Ftest?sslmode=require"
      )

    assert options[:hostname] == "db.example.test"
    assert options[:port] == 55_432
    assert options[:username] == "test@user"
    assert options[:password] == "p:ss"
    assert options[:database] == "selecto_test"
    assert options[:ssl]
    refute Keyword.has_key?(options, :url)
  end

  test "accepts a nonempty username without a password" do
    options = ConnectionOptions.from_url!("postgres://test%40user@db.example.test/selecto")

    assert options[:username] == "test@user"
    assert options[:password] == nil

    empty_password =
      ConnectionOptions.from_url!("postgres://test%40user:@db.example.test/selecto")

    assert empty_password[:password] == ""
  end

  test "preserves explicit TLS disable and rejects unsupported or ambiguous SSL modes" do
    assert ConnectionOptions.from_url!(
             "postgres://user:password@example.test/database?sslmode=disable"
           )[:ssl] == false

    for query <- ["sslmode=prefer", "sslmode=require&sslmode=disable", "application_name=test"] do
      assert_invalid_url("postgres://user:password@example.test/database?#{query}")
    end
  end

  test "rejects malformed scheme, credentials, database, fragments, and ports" do
    for url <- [
          "https://user:password@example.test/database",
          "postgres://example.test/database",
          "postgres://:password@example.test/database",
          "postgres://%ZZ:password@example.test/database",
          "postgres://user:%ZZ@example.test/database",
          "postgres://user:password@example.test",
          "postgres://user:password@example.test/database%2Fextra",
          "postgres://user:password@example.test/%ZZ",
          "postgres://user:password@example.test:0/database",
          "postgres://user:password@example.test:65536/database",
          "postgres://user:password@example.test:not-a-port/database",
          "postgres://user:password@example.test/database#fragment"
        ] do
      assert_invalid_url(url)
    end
  end

  test "environment ports are validated before Postgrex receives them" do
    previous_url = System.get_env("SELECTO_POSTGRES_TEST_URL")
    previous_port = System.get_env("SELECTO_POSTGRES_TEST_PORT")

    on_exit(fn ->
      restore_env("SELECTO_POSTGRES_TEST_URL", previous_url)
      restore_env("SELECTO_POSTGRES_TEST_PORT", previous_port)
    end)

    System.delete_env("SELECTO_POSTGRES_TEST_URL")
    System.put_env("SELECTO_POSTGRES_TEST_PORT", "65536")

    assert_raise ArgumentError, ~r/integer from 1 to 65535/, fn ->
      ConnectionOptions.options()
    end
  end

  defp assert_invalid_url(url) do
    error = assert_raise ArgumentError, fn -> ConnectionOptions.from_url!(url) end

    assert error.message =~ "SELECTO_POSTGRES_TEST_URL"
    refute error.message =~ url
  end

  defp restore_env(key, nil), do: System.delete_env(key)
  defp restore_env(key, value), do: System.put_env(key, value)
end
