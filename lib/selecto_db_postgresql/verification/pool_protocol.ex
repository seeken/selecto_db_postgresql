defmodule SelectoDBPostgreSQL.Verification.PoolProtocol do
  @moduledoc """
  Bounded transition model for managed PostgreSQL pool generations.

  It explores start, reuse, pool death, stale-manager retirement, stop, and
  restart. References are generation tagged in the model so a successful use
  must point at the one live registered generation.
  """

  alias Selecto.Verification.BoundedTraceModel

  @model "selecto_db_postgresql.pool_protocol.v1"

  @spec check() :: map()
  def check do
    BoundedTraceModel.check(@model, [initial_state()], events(), invariants(), max_depth: 7)
  end

  defp initial_state do
    %{
      generation: 0,
      registered: nil,
      manager_alive?: false,
      pool_alive?: false,
      ref_generation: nil,
      last_result: nil
    }
  end

  defp events do
    [
      {:start, &start/1},
      {:reuse, &reuse/1},
      {:select_previous_reference, &select_previous_reference/1},
      {:execute, &execute/1},
      {:kill_pool, &kill_pool/1},
      {:retire_stale_manager, &retire_stale_manager/1},
      {:stop, &stop/1}
    ]
  end

  defp invariants do
    [
      {"successful references target the live registered generation", &live_reference/1},
      {"dead pools cannot execute successfully", &dead_pool_fails_closed/1},
      {"retired and stopped generations are not registered", &registration_tracks_liveness/1}
    ]
  end

  defp start(%{registered: nil, manager_alive?: false, generation: generation} = state)
       when generation < 2 do
    generation = state.generation + 1

    {:next,
     %{
       state
       | generation: generation,
         registered: generation,
         manager_alive?: true,
         pool_alive?: true,
         ref_generation: generation,
         last_result: :started
     }, %{generation: generation}}
  end

  defp start(_state), do: :disabled

  defp reuse(%{registered: generation, manager_alive?: true, pool_alive?: true} = state)
       when is_integer(generation) do
    {:next, %{state | ref_generation: generation, last_result: :reused}}
  end

  defp reuse(_state), do: :disabled

  defp select_previous_reference(%{generation: generation} = state) when generation > 1 do
    {:next, %{state | ref_generation: generation - 1, last_result: :stale_reference_selected}}
  end

  defp select_previous_reference(_state), do: :disabled

  defp execute(%{ref_generation: ref, registered: generation} = state)
       when is_integer(ref) do
    result =
      if state.manager_alive? and state.pool_alive? and ref == generation,
        do: :ok,
        else: :structured_error

    {:next, %{state | last_result: result}}
  end

  defp execute(_state), do: :disabled

  defp kill_pool(%{pool_alive?: true} = state) do
    {:next, %{state | pool_alive?: false, last_result: :pool_killed}}
  end

  defp kill_pool(_state), do: :disabled

  defp retire_stale_manager(%{manager_alive?: true, pool_alive?: false} = state) do
    {:next, %{state | registered: nil, manager_alive?: false, last_result: :retired}}
  end

  defp retire_stale_manager(_state), do: :disabled

  defp stop(%{manager_alive?: true} = state) do
    {:next,
     %{state | registered: nil, manager_alive?: false, pool_alive?: false, last_result: :stopped}}
  end

  defp stop(_state), do: :disabled

  defp live_reference(%{last_result: result} = state) when result in [:started, :reused, :ok] do
    if state.ref_generation == state.registered and state.manager_alive? and state.pool_alive?,
      do: :ok,
      else: {:error, {:stale_successful_reference, state.ref_generation, state.registered}}
  end

  defp live_reference(_state), do: :ok

  defp dead_pool_fails_closed(%{pool_alive?: false, last_result: :ok}),
    do: {:error, :dead_pool_executed_successfully}

  defp dead_pool_fails_closed(_state), do: :ok

  defp registration_tracks_liveness(%{registered: nil}), do: :ok

  defp registration_tracks_liveness(%{registered: generation} = state) do
    if generation == state.generation and state.manager_alive?,
      do: :ok,
      else: {:error, {:invalid_registration, generation}}
  end
end
