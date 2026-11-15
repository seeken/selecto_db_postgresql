defmodule SelectoDBPostgreSQL.RawJSONB do
  @moduledoc false

  # PostgreSQL validated this JSON. A fragment preserves that text for Jason
  # output without decoding it into maps. Copying in the extension avoids
  # retaining the driver's whole receive buffer for a small JSON value.
  def decode!(json), do: Jason.Fragment.new(json)
  def encode_to_iodata!(value), do: Jason.encode_to_iodata!(value)
end

defmodule SelectoDBPostgreSQL.Extensions.RawJSONB do
  @moduledoc """
  Postgrex jsonb extension returning `Jason.Fragment` values.

  For a host's custom Postgrex types module, prepend this extension to its
  extensions. Ordinary `json` keeps the host's JSON decoder; jsonb parameter
  encoding uses Jason. SQL NULL remains nil, including within jsonb arrays.
  """

  @behaviour Postgrex.Extension

  def init(opts),
    do: Postgrex.Extensions.JSONB.init(Keyword.put(opts, :json, SelectoDBPostgreSQL.RawJSONB))

  defdelegate matching(state), to: Postgrex.Extensions.JSONB
  defdelegate format(state), to: Postgrex.Extensions.JSONB
  defdelegate encode(state), to: Postgrex.Extensions.JSONB
  defdelegate decode(state), to: Postgrex.Extensions.JSONB
end

Postgrex.Types.define(
  SelectoDBPostgreSQL.RawJSONBTypes,
  [SelectoDBPostgreSQL.Extensions.RawJSONB],
  []
)
