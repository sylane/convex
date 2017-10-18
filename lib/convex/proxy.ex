defmodule Convex.Proxy do

  #===========================================================================
  # Includes
  #===========================================================================

  alias __MODULE__, as: This
  alias Convex.Context, as: Ctx
  alias Convex.Types


  #===========================================================================
  # Types
  #===========================================================================

  @type options :: Keyword.t
  @type state :: term
  @type message :: term

  @type t :: %This{
    mod: module,
    sub: state
  }

  defstruct [
    :mod,
    :sub,
  ]


  #===========================================================================
  # Behaviour Definition
  #===========================================================================

  @callback init(options) :: state

  @callback pid(state) :: pid

  @callback unbind(state) :: :ok

  @callback post(message, state) :: :ok

  @callback post(Ctx.t, message, state) :: :ok

  @callback close(state) :: :ok

  @callback policy_changed(state, Types.policy) :: :ok


  #===========================================================================
  # API Functions
  #===========================================================================

  @spec new(module, options) :: This.t

  def new(mod, opts \\ []) do
    %This{mod: mod, sub: mod.init(opts)}
  end


  @spec pid(This.t) :: pid

  def pid(%This{mod: mod, sub: sub}) do
    mod.pid(sub)
  end


  @spec unbind(This.t) :: :ok

  def unbind(%This{mod: mod, sub: sub}) do
    mod.unbind(sub)
  end


  @spec post(message, This.t) :: :ok

  def post(msg, %This{mod: mod, sub: sub}) do
    mod.post(msg, sub)
  end


  @spec post(Ctx.t, message, This.t) :: Ctx.t

  def post(ctx, msg, %This{mod: mod, sub: sub}) do
    mod.post(ctx, msg, sub)
    ctx
  end


  @spec close(This.t) :: :ok

  def close(%This{mod: mod, sub: sub}) do
    mod.close(sub)
  end


  @spec policy_changed(This.t, Policies.t) :: :ok

  def policy_changed(%This{mod: mod, sub: sub}, policy) do
    mod.policy_changed(sub, policy)
  end

end
