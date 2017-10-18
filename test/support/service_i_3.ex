defmodule Convex.Test.ServiceI3 do

  alias Convex.Context, as: Ctx
  alias Convex.Test.ServiceCommon

  @service_name :test_service_i3
  @tag :i3
  @op [:index]


  def start(), do: ServiceCommon.start(@service_name, &_perform/3)


  def stop(), do: ServiceCommon.stop(@service_name)


  def perform(ctx, @op = op, args) do
    if @tag in Map.get(args, :inline, []) do
      _perform(ctx, op, args)
    else
      ServiceCommon.safe_perform(@service_name, ctx, op, args)
    end
  end

  def perform(ctx, _op, _args) do
    Ctx.failed(ctx, :unknown_operation)
  end


  defp _perform(ctx, op, %{delays: %{@tag => delay} = delays} = args) do
    :timer.sleep(delay)
    _perform(ctx, op, Map.put(args, :delays, Map.delete(delays, @tag)))
  end

  defp _perform(ctx, @op, %{@tag => {:fail, reason}}) do
    Ctx.failed(ctx, reason)
  end

  defp _perform(_ctx, @op, %{@tag => {:die, reason}}) do
    exit(reason)
  end

  defp _perform(ctx, @op, %{@tag => values}) when is_list(values) do
    ServiceCommon.perform_values(ctx, values)
  end

  defp _perform(ctx, _op, _args) do
    Ctx.failed(ctx, :unknown_operation)
  end

end
