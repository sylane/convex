defmodule Convex.Test.ServiceI4 do

  alias Convex.Context, as: Ctx
  alias Convex.Guards
  alias Convex.Test.ServiceCommon

  @tag :i4
  @op [:index]


  def perform(ctx, op, args) do
    if @tag in Map.get(args, :inline, []) do
      _perform(ctx, op, args)
    else
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
  end


  defp init(from, ref, _ctx, @op, %{@tag => {:bug, :delegate_failed}}) do
    send(from, {:failed, ref, :delegate_failed})
  end

  defp init(_from, _ref, _ctx, @op, %{@tag => {:bug, :delegate_timeout}}) do
    :timer.sleep(2000)
  end

  defp init(from, ref, ctx, op, args) do
    send(from, {:started, ref})
    case Guards.protect(ctx, fn -> _perform(ctx, op, args) end) do
      {:error, _} = error -> error
      {:ok, ctx2} ->
        Guards.ensure_discharged(ctx2)
    end
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
