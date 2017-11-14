defmodule Convex.Context.Async do

  @moduledoc """
  Callback module for `Convex.Context` providing asynchronous behaviour.

  When used with an operation pipeline, performing the pipeline will always
  return `:ok` and the pipeline result will be ignored.

  **IMPORTANT**
  Keep in mind that any operations done "in-proc" (not delegated to another
  process) will be executed in the caller process and block even though the
  result will ultimately be dropped.

  #### Usage

  ```Elixir
  :ok = perform Convex.Context.Async.new() do
    some.operation ^args
  end
  ```

  If you need to do something asynchronously from inside an operation handler
  and don't want to lose the authentication/session/policy/assigned values and
  tags you can recast the current context to clone this data:

  ```Elixir
  perform Convex.Context.Async.recast(current_context) do
    some.operation ^args
  end
  ```

  If you want to enable the ability to bind the context in the pipeline
  operation handlers, you can specify the `binder` option. It is a function
  taking the context as an argument and should return a `Convex.Proxy`.

  """

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

  @type t :: %This{
    binder: ((Ctx.t) -> term),
  }

  defstruct [
    binder: nil
  ]


  #===========================================================================
  # API Functions
  #===========================================================================

  @spec new() :: Ctx.t
  @spec new(options :: Keyword.t) :: context :: Ctx.t
  @doc """
  Creates a new asynchronous context.

  In addition to `Convex.Context.new/2` options, the following options
  are supported:

    - `binder`: a function taking a context and returning a `Convex.Proxy`
        enabling the ability to bind the context in the operation handlers.
  """

  def new(opts \\ []) do
    Ctx.new(This, opts)
  end


  @spec recast(base_context :: Ctx.t) :: context :: Ctx.t
  @spec recast(base_context :: Ctx.t, options :: Keyword.t) :: context :: Ctx.t
  @doc """
  Creates an asynchronous context from an existing context.

  Keeps the authentication, session, policy, assigned values and tags from
  the given context.

  In addition to `Convex.Context.recast/2` options, the following options
  are supported:

    - `binder`: a function taking a context and returning a `Convex.Proxy`
        enabling the ability to bind the context in the operation handlers.
  """

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

  @doc false
  def init(opts) do
    binder = Keyword.get(opts, :binder)
    {:ok, %This{binder: binder}}
  end


  @doc false
  def prepare_recast(assigns, _opts), do: assigns


  @doc false
  def format_ident(ident, _this), do: ident


  @doc false
  def bind(_ctx, %This{binder: nil} = this), do: {:error, this, :not_implemented}

  def bind(ctx, %This{binder: binder} = this), do: {:ok, this, binder.(ctx)}


  @doc false
  def operation_done(_op, result, _ctx, this), do: {this, result}


  @doc false
  def operation_failed(_op, reason, nil, _ctx, this), do: {this, reason}

  def operation_failed(_op, reason, debug, _ctx, this) do
    {this, {reason, debug}}
  end


  @doc false
  def pipeline_fork(_ctx, this), do: {this, this}


  @doc false
  def pipeline_join(_forked_subs, _ctx, this), do: this


  @doc false
  def pipeline_delegate(_ctx, this), do: {this, this}


  @doc false
  def pipeline_failed(_reason, _ctx, this), do: this


  @doc false
  def pipeline_forked(_results, _delegated, _ctx, this), do: this


  @doc false
  def pipeline_done(_result, _ctx, this), do: this


  @doc false
  def pipeline_delegated(_pid, _ctx, this), do: this


  @doc false
  def context_changed(_ctx, this), do: this


  @doc false
  def policy_changed(_ctx, this), do: this


  @doc false
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


  @doc false
  def pipeline_performed!(opts, ctx, this) do
    pipeline_performed(opts, ctx, this)
  end

end
