defmodule Convex.GenServer do

  #===========================================================================
  # Includes
  #===========================================================================

  alias Convex.Context, as: Ctx
  alias Convex.Guards
  alias Convex.Errors


  #===========================================================================
  # API Functions
  #===========================================================================

  def perform(pid, ctx, op, args) when is_pid(pid) do
    {ctx1, ctx2} = Ctx.delegate(ctx, pid)
    Elixir.GenServer.cast(pid, {:perform, [ctx2, op, args]})
    ctx1
  end

  def perform(server, ctx, op, args) when is_atom(server) do
    case Process.whereis(server) do
      nil -> Errors.process_not_found!(ctx, server)
      pid ->
        {ctx1, ctx2} = Ctx.delegate(ctx, pid)
        Elixir.GenServer.cast(pid, {:perform, [ctx2, op, args]})
        ctx1
    end
  end


  def handle_perform([ctx, op, args], state) do
    case safe_perform(ctx, op, args, state) do
      {:ok, state2} -> {:noreply, state2}
      {:error, _, state2} -> {:noreply, state2}
    end
  end


  def handle_perform(op_data, state, timeout) when is_function(timeout, 1) do
    {:noreply, state2} = handle_perform(op_data, state)
    {:noreply, state2, timeout.(state2)}
  end

  def handle_perform(op_data, state, timeout) do
    {:noreply, state2} = handle_perform(op_data, state)
    {:noreply, state2, timeout}
  end


  def safe_perform(ctx, op, args, %{__struct__: mod} = state) do
    handle_fun = fn -> mod.handle_operation(ctx, op, args, state) end
    case Guards.protect ctx, handle_fun do
      {:ok, {state2, ctx2}} ->
        # Checks the context has been handled properly
        Guards.ensure_discharged!(ctx2)
        {:ok, state2}
      {:error, ctx2} ->
        # Checks the context has been handled properly
        ctx3 = Guards.ensure_discharged!(ctx2)
        # Server state is rolled back
        {:error, ctx3.result, state}
    end
  end


  @doc """
  Just a helper for GenServer pipeline return value, this is optional
  so it should not do anything special.
  """

  def pack(ctx, state), do: {state, ctx}

end
