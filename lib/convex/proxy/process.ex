defmodule Convex.Proxy.Process do

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

  @spec new(pid, reference, options) :: Proxy.t

  def new(pid, ref, opts \\ []) do
    Proxy.new(This, [{:ctx_ref, ref}, {:ctx_pid, pid} | opts])
  end


  #===========================================================================
  # Behaviour Convex.Proxy Callback Functions
  #===========================================================================

  def init(opts) do
    pid = Keyword.fetch!(opts, :ctx_pid)
    ctx_ref = Keyword.fetch!(opts, :ctx_ref)
    proxy_ref = :erlang.make_ref()
    send(pid, {This, proxy_ref, :bind, ctx_ref, self()})
    %This{ref: proxy_ref, pid: pid}
  end


  def pid(%This{pid: pid}), do: pid


  def unbind(%This{ref: ref, pid: pid}) do
    send(pid, {This, ref, :unbound})
    :ok
  end


  def post(msg, %This{ref: proxy_ref, pid: pid}) do
    send(pid, {This, proxy_ref, nil, :post, msg})
    :ok
  end


  def post(ctx, msg, %This{ref: proxy_ref, pid: pid}) do
    reqref = Ctx.get_assigned(ctx, :reqref)
    send(pid, {This, proxy_ref, reqref, :post, msg})
    :ok
  end


  def close(%This{ref: proxy_ref, pid: pid}) do
    send(pid, {This, proxy_ref, :close})
    :ok
  end


  def policy_changed(%This{ref: proxy_ref, pid: pid}, policy) do
    send(pid, {This, proxy_ref, :policy_changed, policy})
    :ok
  end

end
