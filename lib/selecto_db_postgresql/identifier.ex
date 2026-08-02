defmodule SelectoDBPostgreSQL.Identifier do
  @moduledoc false

  @counter_key {__MODULE__, :dynamic_identifier_counter}
  @max_dynamic_identifiers 10_000
  @max_identifier_bytes 63

  def to_atom!(value) when is_atom(value) and not is_nil(value), do: value

  def to_atom!(value) when is_binary(value) do
    validate!(value)

    try do
      String.to_existing_atom(value)
    rescue
      ArgumentError -> intern_new_atom!(value)
    end
  end

  def to_atom!(value) do
    raise ArgumentError, "cannot convert #{inspect(value)} to a PostgreSQL identifier atom"
  end

  defp validate!(value) do
    cond do
      value == "" ->
        raise ArgumentError, "PostgreSQL identifier cannot be empty"

      byte_size(value) > @max_identifier_bytes ->
        raise ArgumentError, "PostgreSQL identifier exceeds #{@max_identifier_bytes} bytes"

      not String.valid?(value) ->
        raise ArgumentError, "PostgreSQL identifier must be valid UTF-8"

      true ->
        :ok
    end
  end

  defp intern_new_atom!(value) do
    :global.trans({__MODULE__, :atom_interning}, fn ->
      try do
        String.to_existing_atom(value)
      rescue
        ArgumentError -> create_bounded_atom!(value)
      end
    end)
  end

  defp create_bounded_atom!(value) do
    counter = ensure_counter!()
    count = :atomics.add_get(counter, 1, 1)

    if count <= @max_dynamic_identifiers do
      String.to_atom(value)
    else
      :atomics.sub_get(counter, 1, 1)

      raise ArgumentError,
            "PostgreSQL runtime identifier atom limit of #{@max_dynamic_identifiers} has been reached"
    end
  end

  defp ensure_counter! do
    case :persistent_term.get(@counter_key, nil) do
      nil ->
        counter = :atomics.new(1, signed: false)
        :persistent_term.put(@counter_key, counter)
        counter

      counter ->
        counter
    end
  end
end
