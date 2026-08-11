defmodule SelectoDBPostgreSQL.WriteAdapterConformanceTest do
  use ExUnit.Case, async: true

  alias Selecto.Write.AdapterConformance
  alias SelectoDBPostgreSQL.Adapter

  test "previews every portable operation and an atomic batch" do
    selecto = %Selecto{adapter: Adapter, connection: :preview_only}

    assert {:ok, report} = AdapterConformance.check(selecto)
    assert report.capabilities.dialect == :postgresql
    assert report.operations == [:insert, :update, :upsert, :delete]
    assert length(report.batch_preview.statements) == 4
  end
end
