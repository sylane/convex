defmodule Convex.Context.Process do

  @moduledoc """
  Callback module for `Convex.Context` providing notifications of the
  execution state to a given process.

  Used to create contexts that will not block when performing an operation
  pipeline and notify a process through messages about the execution state.
  To handle these messages, `Convex.Handler.Process` can be used.

  See `Convex.Protocol` for more information about the messages sent to the
  handler and how the results are gathered and the process monitored.

  When used with an operation pipeline, performing the pipeline will always
  return `:ok`.

  **IMPORTANT**
  Keep in mind that all the operations done "in-proc" (not delegated to another
  process) will be executed in the caller process and block it until done or
  delegated..

  **e.g.**

    ```Elixir
    track_ref = make_ref()
    pipe_ref = make_ref()
    :ok = perform Convex.Context.Process.new(self(), track_ref, pipe_ref) do
      some.operation ^args
    end
    ```

  When binding a context with this callback module, a `Convex.Proxy` with
  `Convex.Proxy.Process` callback module will be returned.

  For the process receiving the status of the execution to handle
  multiple pipelines at the same time, the context must be given a unique
  pipeline reference that will be passed with all the messages sent to the
  monitoring process.

  In addition, an extra opaque tracking reference can be given that will be
  available through the `reqref` assigned value of the context. If the context
  is passed to the `Convex.Proxy.post/3` function, the tracking reference will
  be taken from it and it will be sent alongside the message. This allow
  the initiator of a pipeline to know a received message has been posted from
  one of its pipeline operations.

  When recasting the context to any other callback module, the default behavior
  is to remove this opaque tracking reference so sub-pipelines posted messages
  are not associated with the root pipeline. If the handler wants the new
  context to keep it, it can pass the option `track` with value `true`:

    ```Elixir
    perform Convex.Context.Sync.recast(ctx, track: true) do
      some.tracked.operation ^args
    end
    ```
  """

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

  @spec new(pid :: pid, track_ref :: any, pipeline_ref :: any)
    :: context :: Ctx.t
  @spec new(pid :: pid, track_ref :: any, pipeline_ref :: any,
            operations :: Keyword.t)
    :: context :: Ctx.t
  @doc """
  Creates a context that will send the pipeline execution status to a given
  process.

  The given tracking reference `reqref` is an opaque value that will be
  added to context's assigned value as `reqref`. It will be associated
  to all bound proxies and send alongside all the messages sent through
  the proxy. It can be used ot associate any proxy messages to the event
  that triggered the pipeline execution.

  The give pipeline reference `piperef` is a unique oapque value that will
  be in all the message sent to the given process ID. It can be used to
  handle multiple pipeline execution at the same time.

  Options are the same as for `Convex.Context.new/2`.
  """

  def new(pid, reqref, piperef, opts \\ []) do
    Ctx.new(This, [{:pid, pid}, {:reqref, reqref}, {:ref, piperef} | opts])
  end


  @spec clone(base_context :: Ctx.t, pid :: pid, track_ref :: any,
              pipeline_ref :: any)
    :: cloned_context :: Ctx.t
  @spec clone(base_context :: Ctx.t, pid :: pid, track_ref :: any,
              pipeline_ref :: any, options :: Keyword.t)
    :: cloned_context :: Ctx.t
  @doc """
  Clones a context with this backend passing a new pipeline reference,
  a new tracking refercence and new monitoring process pid.

  Options are the same as for `Convex.Context.clone/2`.
  """

  def clone(ctx, pid, reqref, piperef, opts \\ []) do
    Ctx.clone(ctx, [{:pid, pid}, {:reqref, reqref}, {:ref, piperef} | opts])
  end


  #===========================================================================
  # Behaviour Convex.Context Callback Functions
  #===========================================================================

  @doc false
  def init(opts) do
    this = %This{pid: Keyword.fetch!(opts, :pid),
                 ref: Keyword.fetch!(opts, :ref)}
    assigns = %{reqref: Keyword.fetch!(opts, :reqref)}
    {:ok, this, assigns}
  end


  @doc false
  def prepare_recast(assigns, opts) do
    case Keyword.get(opts, :track, false) do
      false -> Map.delete(assigns, :reqref)
      true -> assigns
    end
  end


  @doc false
  def format_ident(ident, _this), do: ident


  @doc false
  def bind(_ctx, %This{} = this) do
    {:ok, this, Proxy.new(this.pid, this.ref)}
  end


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
  def pipeline_failed(reason, ctx, %This{pid: recip, ref: ref} = this) do
    msg = {This, ref, self(), ctx.depth, :failed, reason}
    send(recip, msg)
    this
  end


  @doc false
  def pipeline_forked(results, delegated, ctx, this) do
    %This{pid: recip, ref: ref} = this
    msg = {This, ref, self(), ctx.depth, :forked, {results, delegated}}
    send(recip, msg)
    this
  end


  @doc false
  def pipeline_done(result, ctx, %This{pid: recip, ref: ref} = this) do
    msg = {This, ref, self(), ctx.depth, :done, result}
    send(recip, msg)
    this
  end


  @doc false
  def pipeline_delegated(pid, ctx, %This{pid: recip, ref: ref} = this) do
    msg = {This, ref, self(), ctx.depth, :delegated, pid}
    send(recip, msg)
    this
  end


  @doc false
  def context_changed(ctx, %This{pid: recip, ref: ref} = this) do
    msg = {This, ref, self(), ctx.depth, :context_changed, Ctx.compact(ctx)}
    send(recip, msg)
    this
  end


  @doc false
  def policy_changed(ctx, %This{pid: recip, ref: ref} = this) do
    msg = {This, ref, self(), ctx.depth, :policy_changed, ctx.policy}
    send(recip, msg)
    this
  end


  @doc false
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


  @doc false
  def pipeline_performed!(opts, ctx, this) do
    pipeline_performed(opts, ctx, this)
  end

end
