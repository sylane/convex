defmodule Convex.Test.ServiceB do

  alias Convex.Context, as: Ctx
  alias Convex.Test.ServiceD
  alias Convex.Test.ServiceCommon

  @service_name :test_service_b


  def start(), do: ServiceCommon.start(@service_name, &_perform/3)


  def stop(), do: ServiceCommon.stop(@service_name)


  def perform(ctx, [_, :out | _] = op, args) do
    _perform(ctx, op, args)
  end

  def perform(ctx, [_, :in | _] = op, args) do
    ServiceCommon.perform(@service_name, ctx, op, args)
  end

  def perform(ctx, _op, _args) do
    Ctx.failed(ctx, :unknown_operation)
  end


  defp _perform(ctx, [:d | _] = op, args) do
    ServiceD.perform(ctx, op, args)
  end

  defp _perform(ctx, op, %{delay: delay} = args) do
    :timer.sleep(delay)
    _perform(ctx, op, Map.delete(args, :delay))
  end

  defp _perform(ctx, [:b, _], %{acc: acc, done: val}) do
    Ctx.done(ctx, [{:b, val} | acc])
  end

  defp _perform(ctx, [:b, _], %{fail: reason}) do
    Ctx.failed(ctx, reason)
  end

  defp _perform(ctx, [:b, _], %{bug: :lost}), do: ctx

  defp _perform(_ctx, [:b, _], %{die: reason}) do
    exit(reason)
  end

  defp _perform(ctx, _op, _args) do
    Ctx.failed(ctx, :unknown_operation)
  end

end
