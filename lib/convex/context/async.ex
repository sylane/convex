defmodule Convex.Context.Async do

  @behaviour Convex.Context

  #===========================================================================
  # Includes
  #===========================================================================

  use Convex.Tracer

  alias __MODULE__, as: This
  alias Convex.Context, as: Ctx


  #===========================================================================
  # Types
  #===========================================================================

  defstruct [
    binder: nil
  ]


  #===========================================================================
  # API Functions
  #===========================================================================

  def new(opts \\ []) do
    Ctx.new(This, opts)
  end


  def recast(ctx, opts \\ [])

  def recast(nil, opts) do
    Ctx.new(This, opts)
  end

  def recast(ctx, opts) do
    Ctx.recast(ctx, This, opts)
  end


  #===========================================================================
  # Behaviour Convex.Context Callback Functions
  #===========================================================================

  def init(opts) do
    binder = Keyword.get(opts, :binder)
    {:ok, %This{binder: binder}}
  end


  def prepare_recast(assigns, _opts), do: assigns


  def format_ident(ident, _this), do: ident


  def bind(_ctx, %This{binder: nil} = this), do: {:error, this, :not_implemented}

  def bind(ctx, %This{binder: binder} = this), do: {:ok, this, binder.(ctx)}


  def operation_done(_op, result, _ctx, this), do: {this, result}


  def operation_failed(_op, reason, nil, _ctx, this), do: {this, reason}

  def operation_failed(_op, reason, debug, _ctx, this) do
    {this, {reason, debug}}
  end


  def pipeline_fork(_ctx, this), do: {this, this}


  def pipeline_join(_forked_subs, _ctx, this), do: this


  def pipeline_delegate(_ctx, this), do: {this, this}


  def pipeline_failed(_reason, _ctx, this), do: this


  def pipeline_forked(_results, _delegated, _ctx, this), do: this


  def pipeline_done(_result, _ctx, this), do: this


  def pipeline_delegated(_pid, _ctx, this), do: this


  def context_changed(_ctx, this), do: this


  def policy_changed(_ctx, this), do: this


  def pipeline_performed(_opts, %Ctx{state: :done} = _ctx, _this) do
    # This will generate a warning when enabling tracing
    cvx_trace(">>>>>>>>>>", :finished, [0, 0, 0], [:async, :done], [result: _ctx.result])
    :ok
  end

  def pipeline_performed(_opts, %Ctx{state: :failed} = _ctx, _this) do
    # This will generate a warning when enabling tracing
    cvx_trace(">>>>>>>>>>", :finished, [0, 0, 0], [:async, :failed], [reason: _ctx.result])
    :ok
  end

  def pipeline_performed(_opts, %Ctx{state: :forked, delegated: []} = _ctx, _this) do
    # This will generate a warning when enabling tracing
    cvx_trace(">>>>>>>>>>", :finished, [0, 0, 0], [:async, :forked], [results: _ctx.result, delegated: []])
    :ok
  end

  def pipeline_performed(_opts, _ctx, _this), do: :ok


  def pipeline_performed!(opts, ctx, this) do
    pipeline_performed(opts, ctx, this)
  end

end
