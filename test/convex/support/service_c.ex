defmodule Convex.Test.ServiceC do

  alias Convex.Context, as: Ctx
  alias Convex.Test.ServiceCommon

  @service_name :test_service_c


  def start(), do: ServiceCommon.start(@service_name, &_perform/3)


  def stop(), do: ServiceCommon.stop(@service_name)


  def perform(ctx, [_, _, :out | _] = op, args) do
    _perform(ctx, op, args)
  end

  def perform(ctx, [_, _, :in | _] = op, args) do
    ServiceCommon.perform(@service_name, ctx, op, args)
  end

  def perform(ctx, _op, _args) do
    Ctx.failed(ctx, :unknown_operation)
  end


  defp _perform(ctx, op, %{delay: delay} = args) do
    :timer.sleep(delay)
    _perform(ctx, op, Map.delete(args, :delay))
  end

  defp _perform(ctx, [:c, _, _], %{acc: acc, done: val}) do
    Ctx.done(ctx, [{:c, val} | acc])
  end

  defp _perform(ctx, [:c, _, _], %{fail: reason}) do
    Ctx.failed(ctx, reason)
  end

  defp _perform(ctx, [:c, _, _], %{bug: :lost}), do: ctx

  defp _perform(_ctx, [:c, _, _], %{die: reason}) do
    exit(reason)
  end

  defp _perform(ctx, _op, _args) do
    Ctx.failed(ctx, :unknown_operation)
  end

end
