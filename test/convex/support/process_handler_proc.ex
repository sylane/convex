defmodule Convex.Test.ProcessHandlerProc do

  #===========================================================================
  # Includes
  #===========================================================================

  alias Convex.Context, as: Ctx
  alias Convex.Handler.Process, as: Handler


  #===========================================================================
  # API Function
  #===========================================================================

  def start(opts) do
    spawn_link(fn -> start_loop(opts) end)
  end


  def stop(pid) do
    Process.unlink(pid)
    ref = :erlang.make_ref()
    monref = Process.monitor(pid)
    send(pid, {__MODULE__, self(), ref, :stop})
    receive do
      {__MODULE__, ^ref, :stopped} ->
        Process.demonitor(monref, [:flush])
        :ok
      {:DOWN, ^monref, :process, ^pid, _reason} ->
        :ok
    end
  end


  def perform(pid, pipeline) do
    ref = :erlang.make_ref()
    monref = Process.monitor(pid)
    send(pid, {__MODULE__, self(), ref, :perform, pipeline})
    receive do
      {__MODULE__, ^ref, outcome, result} ->
        Process.demonitor(monref, [:flush])
        {outcome, result}
      {:DOWN, ^monref, :process, ^pid, reason} ->
        {:failed, reason}
    end
  end


  #===========================================================================
  # Internal Function
  #===========================================================================

  defp start_loop(opts), do: loop(Handler.new(opts))


  defp loop(handler) do
    try do
      receive do
        {__MODULE__, pid, ref, :stop} ->
          send(pid, {__MODULE__, ref, :stopped})
          :ok
        {__MODULE__, pid, ref, :perform, pipeline} ->
          {handler2, ctx} = Handler.prepare(handler, nil, {pid, ref})
          Ctx.perform(ctx, pipeline)
          loop(handler2)
        msg ->
          case Handler.handle_info(msg, handler) do
            {_, handler2} -> loop(handler2)
            {:done, {pid, ref}, result, handler2} ->
              send(pid, {__MODULE__, ref, :done, result})
              loop(handler2)
            {:failed, {pid, ref}, reason, handler2} ->
              send(pid, {__MODULE__, ref, :failed, reason})
              loop(handler2)
          end
      end
    rescue
      e ->
        stacktrace = System.stacktrace()
        IO.puts "ERROR: #{Exception.message(e)}\n#{Exception.format_stacktrace(stacktrace)}"
        reraise e, stacktrace
    end
  end

end
