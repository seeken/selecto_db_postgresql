defmodule SelectoDBPostgreSQL.UpdatoAdapter do
  @moduledoc false

  @behaviour SelectoUpdato.WriteAdapter

  defdelegate name(), to: SelectoUpdato.WriteAdapters.Generic
  defdelegate validate_operation(op), to: SelectoUpdato.WriteAdapters.Generic

  defdelegate merge_upsert_opts(op, conflict_opts, on_conflict_opts, base_opts),
    to: SelectoUpdato.WriteAdapters.Generic

  defdelegate maybe_load_upsert_result(record, op, repo), to: SelectoUpdato.WriteAdapters.Generic
  defdelegate upsert_preview_style(), to: SelectoUpdato.WriteAdapters.Generic
end
