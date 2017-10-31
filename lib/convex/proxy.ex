defmodule Convex.Proxy do

  @moduledoc """
  Defines and encapsulate the proxy behaviour,

  A proxy is what is given by `Convex.Context` when binding it, and is used
  to open a comunication channel with the initiator service of the current\
  operation pipeline.

  This is used to allow an application to keep a reference to something it can
  use to post messages to some peer service later on.
  It can be unbound to terminate the communication channel with the peer
  service, it can be used to request the peer service to close, or used
  to update the peer service policy.

  When posting a message, the caller can specify the current context to
  possibly allows the callback module to associate the posted message with
  an operation pipeline. For example an operation could take a long time
  to be performed and during this time multiple messages could be sent
  to the peer and it could be able to know these are related to a pending
  operation pipeline.
  """

  #===========================================================================
  # Includes
  #===========================================================================

  alias __MODULE__, as: This
  alias Convex.Context, as: Ctx
  alias Convex.Policy


  #===========================================================================
  # Types
  #===========================================================================

  @type t :: %This{
    mod: module,
    sub: any,
  }

  defstruct [
    :mod,
    :sub,
  ]


  #===========================================================================
  # Behaviour Definition
  #===========================================================================

  @doc """
  Called to initialize the callback module.

  It should return the state for the callback module.
  """
  @callback init(options :: Keyword.t) :: state :: any

  @doc """
  Called to get a pid the caller can use to monitor the peer service is alive.
  """
  @callback pid(state :: any) :: pid


  @doc """
  Called to remove the communication channel with the peer service.
  """
  @callback unbind(state :: any) :: :ok

  @doc """
  Called to send a message to the peer service, without any context.
  """
  @callback post(message :: term, state :: any) :: :ok

  @doc """
  Called to send a message to the peer service with the given context.
  """
  @callback post(context :: Ctx.t, message :: term, state :: any) :: :ok

  @doc """
  Called to close the peer service.
  """
  @callback close(state :: any) :: :ok


  @doc """
  Called to notify the peer service its policy changed.
  """
  @callback policy_changed(state :: any, Policy.t) :: :ok


  #===========================================================================
  # API Functions
  #===========================================================================

  @spec new(callback_module :: module, options :: Keyword.t) :: proxy :: This.t
  @doc """
  Create a new proxy with given callback module and options.

  The options will be given as-is to the callback module.
  """

  def new(mod, opts \\ []) do
    %This{mod: mod, sub: mod.init(opts)}
  end


  @spec pid(proxy :: This.t) :: pid
  @doc """
  Returns a pid the caller can use to monitor the peer service is alive.
  """

  def pid(%This{mod: mod, sub: sub}) do
    mod.pid(sub)
  end


  @spec unbind(proxy :: This.t) :: :ok
  @doc """
  Disbands the communication channel with the peer service.

  After calling this function the proxy should not be used anymore.
  """

  def unbind(%This{mod: mod, sub: sub}) do
    mod.unbind(sub)
  end


  @spec post(message :: term, proxy :: This.t) :: :ok
  @doc """
  Posts a message to bound peer service without any context.
  """

  def post(msg, %This{mod: mod, sub: sub}) do
    mod.post(msg, sub)
  end


  @spec post(context :: Ctx.t, message :: term, proxy :: This.t) :: Ctx.t
  @doc """
  Posts a message to bound peer service with given context.

  When posting a message in an operation handler where a context is available,
  this function should be used. This allow the proxy backend to include
  tracking information so the message can be associated to the operation
  pipeline. See `Convex.Context.Process` and `Convex.Proxy.Process` for an
  example.
  """

  def post(ctx, msg, %This{mod: mod, sub: sub}) do
    mod.post(ctx, msg, sub)
    ctx
  end


  @spec close(proxy :: This.t) :: :ok
  @doc """
  Request the peer service to close.

  The only acceptable call after calling this function is `unbind/1`,
  even though the communication channel could already be terminated.
  """

  def close(%This{mod: mod, sub: sub}) do
    mod.close(sub)
  end


  @spec policy_changed(proxy :: This.t, policy :: Policies.t) :: :ok
  @doc """
  Notifies the peer service its policy changed.

  This is needed so any further operation pipeline the peer service
  would perform would have an updated policy (for access control).
  """

  def policy_changed(%This{mod: mod, sub: sub}, policy) do
    mod.policy_changed(sub, policy)
  end

end
