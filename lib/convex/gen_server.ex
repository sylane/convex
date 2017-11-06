defmodule Convex.GenServer do

  @moduledoc """
  Helper module to handle operation in a `GenServer`.
  """

  #===========================================================================
  # Includes
  #===========================================================================

  alias Convex.Context, as: Ctx
  alias Convex.Guards
  alias Convex.Errors


  #===========================================================================
  # Types
  #===========================================================================

  @type timeout_arg :: :infinity | integer | ((state :: map) -> integer)
  @type server_spec :: pid | atom


  #===========================================================================
  # API Functions
  #===========================================================================

  @spec perform(server :: server_spec,
                context :: Ctx.t, op :: Ctx.op, args :: map)
    :: context :: Ctx.t
  @doc """
  Delegate an operation to a `GenServer` process.

  To handle it the process will need to catch the cast message
  `{:perfrom, data}` and call `handle_perform/2` with the data and its state.
  """

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


  @spec handle_perform(data :: term, state :: map) :: {:noreply, map}
  @doc """
  Handles a delegated operation.

  The state **MUST** be a struct defined in in the server module, because
  the stuct name will be used to know which module to call back to handle
  the operation.

  This function will call back the function `handle_operation/4` with spec:

    ```Elixir
    @spec handle_operation(Ctx.t, Ctx.op, args :: map, state :: map)
      :: {state :: map, Ctx.t}
    ```

  The callback is called from a guarded section (See `Convex.Guards`) so
  if any exception is raise the operation will fail.

  """

  def handle_perform([ctx, op, args], state) do
    case safe_perform(ctx, op, args, state) do
      {:ok, state2} -> {:noreply, state2}
      {:error, _, state2} -> {:noreply, state2}
    end
  end


  @spec handle_perform(data :: term, state :: map, timeout :: timeout_arg)
    :: {:noreply, map}
  @doc """
  Handles a delegated operation and setup a timeout.

  Same as `handle_perform/2` but return the given `GenServer` timeout
  (or the result of the timeout function).
  """

  def handle_perform(op_data, state, timeout) when is_function(timeout, 1) do
    {:noreply, state2} = handle_perform(op_data, state)
    {:noreply, state2, timeout.(state2)}
  end

  def handle_perform(op_data, state, timeout) do
    {:noreply, state2} = handle_perform(op_data, state)
    {:noreply, state2, timeout}
  end


  @spec pack(context :: Ctx.t, state :: any) :: {Ctx.t, any}
  @doc """
  Just a helper for GenServer operation handler return values.

  Used in pipeline to pack the server state with the context in the expected
  return format.
  """

  def pack(ctx, state), do: {state, ctx}


  #===========================================================================
  # Internal Functions
  #===========================================================================

  defp safe_perform(ctx, op, args, %{__struct__: mod} = state) do
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

end
