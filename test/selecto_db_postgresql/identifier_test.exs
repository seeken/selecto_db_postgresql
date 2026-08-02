defmodule SelectoDBPostgreSQL.IdentifierTest do
  use ExUnit.Case, async: true

  alias SelectoDBPostgreSQL.Identifier

  test "interns valid PostgreSQL identifiers consistently" do
    identifier = "runtime_column_#{System.unique_integer([:positive])}"

    assert atom = Identifier.to_atom!(identifier)
    assert Atom.to_string(atom) == identifier
    assert Identifier.to_atom!(identifier) == atom
  end

  test "rejects identifiers outside PostgreSQL's identifier boundary" do
    assert_raise ArgumentError, fn -> Identifier.to_atom!("") end
    assert_raise ArgumentError, fn -> Identifier.to_atom!(String.duplicate("x", 64)) end
  end
end
