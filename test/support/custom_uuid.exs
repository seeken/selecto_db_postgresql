defmodule SelectoDBPostgreSQL.Verification.CustomUUID do
  @moduledoc false
  @behaviour Postgrex.Extension

  def init(opts), do: Postgrex.Extensions.UUID.init(Keyword.put_new(opts, :decode_binary, :copy))
  defdelegate matching(state), to: Postgrex.Extensions.UUID
  defdelegate format(state), to: Postgrex.Extensions.UUID
  defdelegate encode(state), to: Postgrex.Extensions.UUID
  defdelegate decode(state), to: Postgrex.Extensions.UUID
end

Postgrex.Types.define(
  SelectoDBPostgreSQL.Verification.CustomUUIDTypes,
  [SelectoDBPostgreSQL.Verification.CustomUUID],
  []
)
