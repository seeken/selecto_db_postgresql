defmodule SelectoDBPostgreSQL.Verification.TransactionProtocol do
  @moduledoc """
  Bounded transition model for atomic PostgreSQL write execution.

  The model covers two- and three-step batch and graph plans. Every compiled
  step can succeed, fail during execution, or fail its cardinality check. The
  transaction may commit only after every step succeeds; every other terminal
  path rolls the abstract database back to its initial value.
  """

  alias Selecto.Verification.BoundedTraceModel

  @model "selecto_db_postgresql.transaction_protocol.v1"

  @spec check() :: map()
  def check do
    BoundedTraceModel.check(@model, initial_states(), events(), invariants(), max_depth: 5)
  end

  defp initial_states do
    for kind <- [:batch, :graph], length <- [2, 3] do
      %{
        kind: kind,
        length: length,
        phase: :open,
        next_step: 0,
        initial_rows: 0,
        working_rows: 0,
        durable_rows: 0,
        outcome: nil,
        failure_position: nil
      }
    end
  end

  defp events do
    [
      {:step_succeeds, &step_succeeds/1},
      {:step_execution_fails, &step_fails(&1, :execution_failed)},
      {:step_cardinality_fails, &step_fails(&1, :cardinality_mismatch)},
      {:commit, &commit/1}
    ]
  end

  defp invariants do
    [
      {"durable state changes only after a complete commit", &durable_only_after_commit/1},
      {"every failure rolls back the complete plan", &failure_rolls_back/1},
      {"commit contains every bounded plan step exactly once", &complete_commit/1}
    ]
  end

  defp step_succeeds(%{phase: :open, next_step: step, length: length} = state)
       when step < length do
    {:next, %{state | next_step: step + 1, working_rows: state.working_rows + 1}, %{step: step}}
  end

  defp step_succeeds(_state), do: :disabled

  defp step_fails(%{phase: :open, next_step: step, length: length} = state, reason)
       when step < length do
    position = failure_position(step, length)

    {:next,
     %{
       state
       | phase: :rolled_back,
         durable_rows: state.initial_rows,
         working_rows: state.initial_rows,
         outcome: reason,
         failure_position: position
     }, %{step: step, position: position, reason: reason}}
  end

  defp step_fails(_state, _reason), do: :disabled

  defp commit(%{phase: :open, next_step: length, length: length} = state) do
    {:next, %{state | phase: :committed, durable_rows: state.working_rows, outcome: :ok},
     %{steps: length}}
  end

  defp commit(_state), do: :disabled

  defp durable_only_after_commit(%{phase: :open} = state) do
    if state.durable_rows == state.initial_rows,
      do: :ok,
      else: {:error, {:uncommitted_rows_became_durable, state.durable_rows}}
  end

  defp durable_only_after_commit(_state), do: :ok

  defp failure_rolls_back(%{phase: :rolled_back} = state) do
    cond do
      state.outcome not in [:execution_failed, :cardinality_mismatch] ->
        {:error, {:unexpected_failure_outcome, state.outcome}}

      state.failure_position not in [:first, :middle, :last] ->
        {:error, {:missing_failure_position, state.failure_position}}

      state.durable_rows != state.initial_rows or state.working_rows != state.initial_rows ->
        {:error, :partial_mutation_survived_rollback}

      true ->
        :ok
    end
  end

  defp failure_rolls_back(_state), do: :ok

  defp complete_commit(%{phase: :committed} = state) do
    if state.next_step == state.length and
         state.durable_rows == state.initial_rows + state.length and state.outcome == :ok,
       do: :ok,
       else: {:error, :incomplete_plan_committed}
  end

  defp complete_commit(_state), do: :ok

  defp failure_position(0, _length), do: :first
  defp failure_position(step, length) when step == length - 1, do: :last
  defp failure_position(_step, _length), do: :middle
end
