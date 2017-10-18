defmodule Convex.Test.ServiceCommon do

  alias Convex.Context, as: Ctx
  alias Convex.Guards


  def start(name, perform_fun) do
    ref = make_ref()
    from = self()
    pid = spawn(fn -> init(from, ref, name, perform_fun) end)
    receive do
      {:started, ^ref} -> {:ok, pid}
    after
      1000 -> {:error, :timeout}
    end
  end


  def stop(name) do
    case Process.whereis(name) do
      nil -> {:error, :noproc}
      pid ->
        monref = Process.monitor(pid)
        send(pid, :stop)
        receive do
          {:DOWN, ^monref, :process, ^pid, _reason} ->
            :ok
        after
          1000 ->
            Process.demonitor(monref, [:flush])
            {:error, :timeout}
        end
    end
  end


  def perform(name, ctx, op, args) do
    case Process.whereis(name) do
      nil -> Ctx.failed(ctx, :noproc)
      pid ->
        {ctx, sub} = Ctx.delegate(ctx, pid)
        send(pid, {:perform, sub, op, args})
        ctx
    end
  end


  def safe_perform(name, ctx, op, args) do
    case Process.whereis(name) do
      nil -> Ctx.failed(ctx, :noproc)
      pid ->
        {ctx, sub} = Ctx.delegate(ctx, pid)
        send(pid, {:safe_perform, sub, op, args})
        ctx
    end
  end


  def perform_values(ctx, values) do
    case Enum.any?(values, &is_atom/1) do
      false -> Ctx.produce(ctx, values)
      true ->
        {ctx, forks} = Enum.reduce(values, {ctx, []},
          fn
            value, {ctx, acc} when is_atom(value) ->
              {ctx, frk} = Ctx.fork(ctx)
              {ctx, [Ctx.failed(frk, value) | acc]}
            value, {ctx, acc} ->
              {ctx, frk} = Ctx.fork(ctx)
              {ctx, [Ctx.done(frk, value) | acc]}
          end)
        Ctx.join(ctx, forks)
    end
  end


  defp init(from, ref, name, perform_fun) do
    Process.register(self(), name)
    send(from, {:started, ref})
    loop(perform_fun)
  end


  defp loop(perform_fun) do
    receive do
      {:perform, ctx, op, args} ->
        perform_fun.(ctx, op, args)
        loop(perform_fun)
      {:safe_perform, ctx, op, args} ->
        case Guards.protect(ctx, fn -> perform_fun.(ctx, op, args) end) do
          {:error, _} -> loop(perform_fun)
          {:ok, ctx2} ->
            Guards.ensure_discharged(ctx2)
            loop(perform_fun)
        end
      :stop ->
        :ok
    end
  end

end
