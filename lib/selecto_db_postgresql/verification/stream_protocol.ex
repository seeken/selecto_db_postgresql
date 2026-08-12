defmodule SelectoDBPostgreSQL.Verification.StreamProtocol do
  @moduledoc """
  Bounded transition model for the adapter cursor producer/consumer protocol.

  It explores queued delivery, demand acknowledgements, producer/transaction
  failures, producer exits, consumer cancellation, and receive timeouts. The
  consumer identity is bound when enumeration begins, at most one chunk may be
  in flight, and every terminal state owns neither a producer nor queued data.
  """

  alias Selecto.Verification.BoundedTraceModel

  @model "selecto_db_postgresql.stream_protocol.v2"

  @spec check() :: map()
  def check do
    BoundedTraceModel.check(@model, initial_states(), events(), invariants(), max_depth: 8)
  end

  defp initial_states do
    for constructor <- [:caller_a, :caller_b], enumerator <- [:caller_a, :caller_b] do
      %{
        constructor: constructor,
        enumerator: enumerator,
        owner: nil,
        phase: :created,
        producer_alive?: false,
        chunks_sent: 0,
        chunks_delivered: 0,
        chunks_settled: 0,
        chunks_drained: 0,
        queued_chunks: 0,
        in_flight?: false,
        outcome: nil
      }
    end
  end

  defp events do
    [
      {:enumerate, &enumerate/1},
      {:send_chunk, &send_chunk/1},
      {:deliver_chunk, &deliver_chunk/1},
      {:acknowledge_chunk, &acknowledge_chunk/1},
      {:complete, &finish_idle(&1, :complete)},
      {:transaction_error, &finish_idle(&1, :transaction_error)},
      {:producer_error, &finish_idle(&1, :producer_error)},
      {:producer_exit, &finish_and_drain(&1, :producer_exit)},
      {:cancel, &finish_and_drain(&1, :cancelled)},
      {:timeout, &finish_idle(&1, :timeout)}
    ]
  end

  defp invariants do
    [
      {"enumeration process owns cursor delivery", &enumerator_owns_delivery/1},
      {"chunks are delivered exactly once and in prefix order", &chunk_prefix/1},
      {"at most one queued chunk is in flight", &bounded_in_flight/1},
      {"terminal paths drain queued protocol messages", &terminal_drains_queue/1},
      {"all terminal paths retire the producer", &terminal_retires_producer/1},
      {"successful completion delivers every sent chunk", &complete_delivery/1},
      {"errors cancellation and timeout never become success", &terminal_outcome_preserved/1}
    ]
  end

  defp enumerate(%{phase: :created} = state) do
    {:next, %{state | phase: :streaming, owner: state.enumerator, producer_alive?: true}}
  end

  defp enumerate(_state), do: :disabled

  defp send_chunk(
         %{
           phase: :streaming,
           producer_alive?: true,
           in_flight?: false,
           queued_chunks: 0,
           chunks_sent: chunks
         } = state
       )
       when chunks < 2 do
    next = chunks + 1

    {:next, %{state | chunks_sent: next, queued_chunks: 1, in_flight?: true}, %{chunk: next}}
  end

  defp send_chunk(_state), do: :disabled

  defp deliver_chunk(%{phase: :streaming, in_flight?: true, queued_chunks: 1} = state) do
    {:next, %{state | chunks_delivered: state.chunks_delivered + 1, queued_chunks: 0},
     %{chunk: state.chunks_delivered + 1}}
  end

  defp deliver_chunk(_state), do: :disabled

  defp acknowledge_chunk(%{phase: :streaming, in_flight?: true, queued_chunks: 0} = state) do
    {:next, %{state | chunks_settled: state.chunks_sent, in_flight?: false}}
  end

  defp acknowledge_chunk(_state), do: :disabled

  defp finish_idle(
         %{
           phase: :streaming,
           producer_alive?: true,
           in_flight?: false,
           queued_chunks: 0
         } = state,
         outcome
       ) do
    {:next, terminate(state, outcome)}
  end

  defp finish_idle(_state, _outcome), do: :disabled

  defp finish_and_drain(%{phase: :streaming} = state, outcome) do
    {:next, terminate(state, outcome)}
  end

  defp finish_and_drain(_state, _outcome), do: :disabled

  defp terminate(state, outcome) do
    %{
      state
      | phase: :terminal,
        producer_alive?: false,
        chunks_settled: state.chunks_sent,
        chunks_drained: state.chunks_drained + state.queued_chunks,
        queued_chunks: 0,
        in_flight?: false,
        outcome: outcome
    }
  end

  defp enumerator_owns_delivery(%{phase: phase} = state) when phase in [:streaming, :terminal] do
    if state.owner == state.enumerator,
      do: :ok,
      else: {:error, {:wrong_stream_owner, state.owner, state.enumerator}}
  end

  defp enumerator_owns_delivery(_state), do: :ok

  defp chunk_prefix(state) do
    accounted_chunks = state.chunks_delivered + state.chunks_drained + state.queued_chunks

    if state.chunks_sent in 0..2 and state.chunks_delivered in 0..2 and
         state.chunks_settled in 0..2 and state.chunks_drained in 0..1 and
         accounted_chunks == state.chunks_sent,
       do: :ok,
       else: {:error, {:invalid_chunk_prefix, state}}
  end

  defp bounded_in_flight(state) do
    unsettled_chunks = state.chunks_sent - state.chunks_settled

    valid? =
      state.queued_chunks in 0..1 and
        case state.phase do
          :created ->
            state.chunks_sent == 0 and state.chunks_settled == 0 and not state.in_flight?

          :streaming ->
            state.chunks_settled <= state.chunks_delivered and
              state.chunks_delivered <= state.chunks_sent and unsettled_chunks in 0..1 and
              state.in_flight? == (unsettled_chunks == 1) and
              state.queued_chunks == state.chunks_sent - state.chunks_delivered

          :terminal ->
            state.chunks_settled == state.chunks_sent and state.queued_chunks == 0 and
              not state.in_flight?
        end

    if valid?, do: :ok, else: {:error, {:unbounded_stream_queue, state}}
  end

  defp terminal_drains_queue(%{
         phase: :terminal,
         queued_chunks: 0,
         in_flight?: false,
         chunks_sent: settled,
         chunks_settled: settled
       }),
       do: :ok

  defp terminal_drains_queue(%{phase: :terminal} = state),
    do: {:error, {:terminal_stream_protocol_not_drained, state}}

  defp terminal_drains_queue(_state), do: :ok

  defp terminal_retires_producer(%{phase: :terminal, producer_alive?: false}), do: :ok
  defp terminal_retires_producer(%{phase: :terminal}), do: {:error, :producer_leaked}
  defp terminal_retires_producer(_state), do: :ok

  defp complete_delivery(%{
         phase: :terminal,
         outcome: :complete,
         chunks_sent: sent,
         chunks_delivered: sent,
         chunks_drained: 0
       }),
       do: :ok

  defp complete_delivery(%{phase: :terminal, outcome: :complete} = state),
    do: {:error, {:incomplete_successful_stream, state}}

  defp complete_delivery(_state), do: :ok

  defp terminal_outcome_preserved(%{phase: :terminal, outcome: outcome})
       when outcome in [
              :complete,
              :transaction_error,
              :producer_error,
              :producer_exit,
              :cancelled,
              :timeout
            ],
       do: :ok

  defp terminal_outcome_preserved(%{phase: :terminal} = state),
    do: {:error, {:invalid_terminal_outcome, state.outcome}}

  defp terminal_outcome_preserved(_state), do: :ok
end
