defmodule Convex.Proxy.Process do

  @moduledoc """
  Callback module for `Convex.Proxy` used when binding a context.

  It is used by `Convex.Context.Process`.


  It will send all posted message to the monitoring process possibly
  alongside tracking information if available.

  In addition it will notify the monitoring process about policy changes
  and request for closing the peer service.

  See `Convex.Protocol` for more information about the message protocol
  between the process context and proxy and the monitoring process.

  """

  @behaviour Convex.Proxy


  #===========================================================================
  # Includes
  #===========================================================================

  alias __MODULE__, as: This
  alias Convex.Context, as: Ctx
  alias Convex.Proxy


  #===========================================================================
  # Types
  #===========================================================================

  @type options :: Keyword.t

  @type t :: %This{
    ref: reference,
    pid: pid,
  }

  defstruct [
    ref: nil,
    pid: nil
  ]


  #===========================================================================
  # API Functions
  #===========================================================================

  @spec new(pid :: pid, pipeline_ref :: any) :: proxy :: Proxy.t
  @spec new(pid :: pid, pipeline_ref :: any, options :: Keyword.t)
    :: proxy :: Proxy.t
  @doc """
  Create a new proxy to the give n process with given pipeline reference.

  Used by `Convex.Context.Process` when bound.
  """

  def new(pid, ref, opts \\ []) do
    Proxy.new(This, [{:ctx_ref, ref}, {:ctx_pid, pid} | opts])
  end


  #===========================================================================
  # Behaviour Convex.Proxy Callback Functions
  #===========================================================================

  @doc false
  def init(opts) do
    pid = Keyword.fetch!(opts, :ctx_pid)
    ctx_ref = Keyword.fetch!(opts, :ctx_ref)
    proxy_ref = :erlang.make_ref()
    send(pid, {This, proxy_ref, :bind, ctx_ref, self()})
    %This{ref: proxy_ref, pid: pid}
  end


  @doc false
  def pid(%This{pid: pid}), do: pid


  @doc false
  def unbind(%This{ref: ref, pid: pid}) do
    send(pid, {This, ref, :unbound})
    :ok
  end


  @doc false
  def post(msg, %This{ref: proxy_ref, pid: pid}) do
    send(pid, {This, proxy_ref, nil, :post, msg})
    :ok
  end


  @doc false
  def post(ctx, msg, %This{ref: proxy_ref, pid: pid}) do
    reqref = Ctx.get_assigned(ctx, :reqref)
    send(pid, {This, proxy_ref, reqref, :post, msg})
    :ok
  end


  @doc false
  def close(%This{ref: proxy_ref, pid: pid}) do
    send(pid, {This, proxy_ref, :close})
    :ok
  end


  @doc false
  def policy_changed(%This{ref: proxy_ref, pid: pid}, policy) do
    send(pid, {This, proxy_ref, :policy_changed, policy})
    :ok
  end

end
