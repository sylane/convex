defmodule Convex.Test.ServiceD do

  alias Convex.Context, as: Ctx


  def perform(ctx, op, args) do
    ref = make_ref()
    from = self()
    {ctx, sub} = Ctx.delegate_prepare(ctx)
    pid = spawn(fn -> init(from, ref, sub, op, args) end)
    receive do
      {:started, ^ref} -> Ctx.delegate_done(ctx, pid)
      {:failed, ^ref, reason} -> Ctx.delegate_failed(ctx, reason)
    after
      1000 -> Ctx.delegate_failed(ctx, :timeout)
    end
  end


  defp init(from, ref, _ctx, [:d, _], %{delegate_fail: reason}) do
    send(from, {:failed, ref, reason})
  end

  defp init(from, ref, ctx, op, args) do
    send(from, {:started, ref})
    _perform(ctx, op, args)
  end


  defp _perform(ctx, op, %{delay: delay} = args) do
    :timer.sleep(delay)
    _perform(ctx, op, Map.delete(args, :delay))
  end

  defp _perform(ctx, [:d, _], %{acc: acc, done: val}) do
    Ctx.done(ctx, [{:d, val} | acc])
  end

  defp _perform(ctx, [:d, _], %{fail: reason}) do
    Ctx.failed(ctx, reason)
  end

  defp _perform(ctx, [:d, _], %{bug: :lost}), do: ctx

  defp _perform(_ctx, [:d, _], %{die: reason}) do
    exit(reason)
  end

  defp _perform(ctx, _op, _args) do
    Ctx.failed(ctx, :unknown_operation)
  end

end
