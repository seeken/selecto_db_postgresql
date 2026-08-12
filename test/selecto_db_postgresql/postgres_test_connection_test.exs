defmodule SelectoDBPostgreSQL.PostgresTestConnectionTest do
  use ExUnit.Case, async: true

  alias SelectoDBPostgreSQL.PostgresTestConnection

  test "parses PostgreSQL URLs into current Postgrex options" do
    options =
      PostgresTestConnection.from_url!(
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

  test "rejects incomplete and non-PostgreSQL URLs without exposing their contents" do
    assert_raise ArgumentError, ~r/SELECTO_POSTGRES_TEST_URL/, fn ->
      PostgresTestConnection.from_url!("https://example.test/database")
    end

    assert_raise ArgumentError, ~r/SELECTO_POSTGRES_TEST_URL/, fn ->
      PostgresTestConnection.from_url!("postgres://user@example.test")
    end

    assert_raise ArgumentError, ~r/SELECTO_POSTGRES_TEST_URL/, fn ->
      PostgresTestConnection.from_url!("postgres://user:password@example.test/database%2Fextra")
    end

    assert_raise ArgumentError, ~r/SELECTO_POSTGRES_TEST_URL/, fn ->
      PostgresTestConnection.from_url!("postgres://user:password@example.test:0/database")
    end
  end
end
