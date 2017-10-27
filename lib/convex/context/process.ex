defmodule Convex.Context.Process do


# If the caller wants to keep any request traking information the backend
#   may be keeping, it MUST specify the option `track: true`, otherwise the
#   backend MAY strip it.
#   """

  @behaviour Convex.Context


  #===========================================================================
  # Includes
  #===========================================================================

  use Convex.Tracer

  alias __MODULE__, as: This
  alias Convex.Proxy.Process, as: Proxy
  alias Convex.Context, as: Ctx


  #===========================================================================
  # Types
  #===========================================================================

  @type t :: %This{
    pid: pid,
    ref: reference
  }

  defstruct [
    pid: nil,
    ref: nil
  ]


  #===========================================================================
  # API Functions
  #===========================================================================

  def new(pid, reqref, piperef, opts \\ []) do
    Ctx.new(This, [{:pid, pid}, {:reqref, reqref}, {:ref, piperef} | opts])
  end


  def clone(ctx, pid, reqref, piperef, opts \\ []) do
    Ctx.clone(ctx, [{:pid, pid}, {:reqref, reqref}, {:ref, piperef} | opts])
  end


  #===========================================================================
  # Behaviour Convex.Context Callback Functions
  #===========================================================================

  def init(opts) do
    this = %This{pid: Keyword.fetch!(opts, :pid),
                 ref: Keyword.fetch!(opts, :ref)}
    assigns = %{reqref: Keyword.fetch!(opts, :reqref)}
    {:ok, this, assigns}
  end


  def prepare_recast(assigns, opts) do
    case Keyword.get(opts, :track, false) do
      false -> Map.delete(assigns, :reqref)
      true -> assigns
    end
  end


  def format_ident(ident, _this), do: ident


  def bind(_ctx, %This{} = this) do
    {:ok, this, Proxy.new(this.pid, this.ref)}
  end


  def operation_done(_op, result, _ctx, this), do: {this, result}


  def operation_failed(_op, reason, nil, _ctx, this), do: {this, reason}

  def operation_failed(_op, reason, debug, _ctx, this) do
    {this, {reason, debug}}
  end


  def pipeline_fork(_ctx, this), do: {this, this}


  def pipeline_join(_forked_subs, _ctx, this), do: this


  def pipeline_delegate(_ctx, this), do: {this, this}


  def pipeline_failed(reason, ctx, %This{pid: recip, ref: ref} = this) do
    msg = {This, ref, self(), ctx.depth, :failed, reason}
    send(recip, msg)
    this
  end


  def pipeline_forked(results, delegated, ctx, this) do
    %This{pid: recip, ref: ref} = this
    msg = {This, ref, self(), ctx.depth, :forked, {results, delegated}}
    send(recip, msg)
    this
  end


  def pipeline_done(result, ctx, %This{pid: recip, ref: ref} = this) do
    msg = {This, ref, self(), ctx.depth, :done, result}
    send(recip, msg)
    this
  end


  def pipeline_delegated(pid, ctx, %This{pid: recip, ref: ref} = this) do
    msg = {This, ref, self(), ctx.depth, :delegated, pid}
    send(recip, msg)
    this
  end


  def context_changed(ctx, %This{pid: recip, ref: ref} = this) do
    msg = {This, ref, self(), ctx.depth, :context_changed, Ctx.compact(ctx)}
    send(recip, msg)
    this
  end


  def policy_changed(ctx, %This{pid: recip, ref: ref} = this) do
    msg = {This, ref, self(), ctx.depth, :policy_changed, ctx.policy}
    send(recip, msg)
    this
  end


  def pipeline_performed(_opts, %Ctx{state: :done} = ctx, _this) do
    cvx_trace(">>>>>>>>>>", :finished, [0, 0, 0], [:process, :done], [result: ctx.result])
    ctx
  end

  def pipeline_performed(_opts, %Ctx{state: :failed} = ctx, _this) do
    cvx_trace(">>>>>>>>>>", :finished, [0, 0, 0], [:process, :failed], [reason: ctx.result])
    ctx
  end

  def pipeline_performed(_opts, %Ctx{state: :forked, delegated: []} = ctx, _this) do
    cvx_trace(">>>>>>>>>>", :finished, [0, 0, 0], [:process, :forked], [results: ctx.result, delegated: []])
    ctx
  end

  def pipeline_performed(_opts, ctx, _this), do: ctx


  def pipeline_performed!(opts, ctx, this) do
    pipeline_performed(opts, ctx, this)
  end

end
