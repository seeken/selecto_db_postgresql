defmodule SelectoDBPostgreSQL.WireTrace do
  @moduledoc false
  # What a call sent: every `Postgrex.query/4` the calling process made, and
  # the payload of every socket send made meanwhile by any process (one send
  # is one round trip: Postgrex writes a whole message group at once).

  def calls(fun) do
    Code.ensure_loaded!(Postgrex)
    tracer = spawn_link(fn -> collect([]) end)
    mfa = {Postgrex, :query, 4}
    1 = :erlang.trace_pattern(mfa, true, [:global])
    1 = :erlang.trace(self(), true, [:call, {:tracer, tracer}])

    result =
      try do
        fun.()
      after
        :erlang.trace(self(), false, [:call])
        :erlang.trace_pattern(mfa, false, [:global])
      end

    await_delivery(self())
    {result, for({Postgrex, :query, [_conn, sql, _params, _opts]} <- drain(tracer), do: sql)}
  end

  def frames(fun) do
    tracer = spawn_link(fn -> collect([]) end)
    patterns = [{:gen_tcp, :send, 2}, {:ssl, :send, 2}]

    Enum.each(patterns, fn {module, _function, _arity} = mfa ->
      Code.ensure_loaded!(module)
      1 = :erlang.trace_pattern(mfa, true, [:global])
    end)

    :erlang.trace(:all, true, [:call, {:tracer, tracer}])

    result =
      try do
        fun.()
      after
        :erlang.trace(:all, false, [:call])
        await_delivery(:all)
        Enum.each(patterns, &:erlang.trace_pattern(&1, false, [:global]))
      end

    {result,
     for({_module, :send, [_socket, data]} <- drain(tracer), do: IO.iodata_to_binary(data))}
  end

  defp await_delivery(target) do
    ref = :erlang.trace_delivered(target)

    receive do
      {:trace_delivered, _target, ^ref} -> :ok
    after
      1_000 -> :ok
    end
  end

  defp drain(tracer) do
    send(tracer, {:collect, self()})

    receive do
      {:collected, calls} -> calls
    after
      1_000 -> []
    end
  end

  defp collect(calls) do
    receive do
      {:trace, _pid, :call, call} -> collect([call | calls])
      {:collect, caller} -> send(caller, {:collected, Enum.reverse(calls)})
    end
  end
end
