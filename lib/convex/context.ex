defmodule Convex.Context do

  @behaviour Access

  #===========================================================================
  # Includes
  #===========================================================================

  use Convex.Tracer

  require Logger

  alias Convex.Config
  alias Convex.Context, as: Ctx
  alias Convex.Guards
  alias Convex.Proxy
  alias Convex.Pipeline
  alias Convex.Auth
  alias Convex.Sess
  alias Convex.Policy
  alias Convex.Proxy
  alias Convex.Format
  alias Convex.Errors


  #===========================================================================
  # Types
  #===========================================================================

  @type op :: [atom]
  @type options :: Keyword.t
  @type state :: term
  @type assigns :: map
  @type result :: term
  @type reason :: term
  @type debug :: term

  @type t :: %Ctx{
    # Publicly accessible fields
    auth: nil | Auth.t,
    sess: nil | Sess.t,
    policy: nil | Policy.t,

    # Internal fields
    state: :standby | :reset | :running | :forking | :delegating | :failed | :done,
    forked: integer,
    depth: integer,
    dir: module,
    mod: module,
    sub: any,
    ident: nil | String.t,
    result: any,
    delegated: pid,
    store: map,
    pipe: nil | Pipeline.t,
    assigns: map,
    tags: MapSet.t,
  }

  defstruct [
    :dir,
    state: nil,
    forked: 0,
    depth: 0,
    mod: nil,
    sub: nil,
    ident: nil,
    auth: nil,
    sess: nil,
    policy: nil,
    result: nil,
    delegated: nil,
    store: %{},
    pipe: nil,
    assigns: %{},
    tags: MapSet.new()
  ]


  #===========================================================================
  # Behaviour Definition
  #===========================================================================

  @callback init(options) :: state | {state, assigns}

  @callback prepare_recast(assigns, options) :: assigns

  @callback format_ident(binary, state) :: binary

  @callback bind(This.t, state) :: Proxy.t

  @callback operation_done(op, result, This.t, state) :: {state, term}

  @callback operation_failed(op, reason, debug, This.t, state) :: {state, reason}

  @callback pipeline_fork(This.t, state) :: {state, state}

  @callback pipeline_join([state], This.t, state) :: state

  @callback pipeline_delegate(This.t, state) :: {state, state}

  @callback pipeline_failed(reason, This.t, state) :: state

  @callback pipeline_forked([result], [pid], This.t, state) :: state

  @callback pipeline_done(result, This.t, state) :: state

  @callback pipeline_delegated(pid, This.t, state) :: state

  @callback context_changed(This.t, state) :: state

  @callback policy_changed(This.t, state) :: state

  @callback pipeline_performed(options, This.t, state) :: any


  #===========================================================================
  # API Functions
  #===========================================================================

  def new(mod, opts \\ []) when is_atom(mod) do
    {dir, opts} = extract_opt(opts, :director, &Config.director/0)
    {tags, opts} = extract_opt(opts, :tags, [])
    {sub, assigns} = _mod_init(mod, %{}, opts)
    %Ctx{state: :standby, dir: dir, mod: mod, sub: sub,
         assigns: assigns, tags: MapSet.new(tags)}
      |> _update_ident()
  end


  def authenticated?(%Ctx{} = ctx), do: ctx.auth != nil


  def attached?(%Ctx{} = ctx), do: ctx.sess != nil


  @doc """
  Create a new context with the specified backend but keeping some state
  like the authentication information, the attached session, the tags
  and the assigned values.
  Some of the assigned values MAY be changed by the current backend
  before the switch to the new backend.
  If the caller wants to keep any request traking information the backend
  may be keeping, it MUST specify the option `track: true`, otherwise the
  backend MAY strip it.
  """

  def recast(%Ctx{} = ctx, mod, opts \\ []) when is_atom(mod) do
    {dir, opts} = extract_opt(opts, :director, &Config.director/0)
    new_assigns = _mod_prepare_recast(ctx.mod, ctx.assigns, opts)
    _duplicate_context(ctx, dir, mod, new_assigns, opts)
  end


  def clone(%Ctx{} = ctx, opts \\ []) do
    _duplicate_context(ctx, ctx.dir, ctx.mod, ctx.assigns, opts)
  end


  def compact(%Ctx{} = ctx) do
    %Ctx{dir: dir, mod: mod, auth: auth, sess: sess, policy: policy} = ctx
    %Ctx{state: :standby, dir: dir, mod: mod, sub: nil,
         auth: auth, sess: sess, policy: policy}
  end


  def reset(%Ctx{} = ctx) do
    %Ctx{dir: dir, mod: mod} = ctx
    %Ctx{state: :reset, dir: dir, mod: mod}
  end


  @doc """
  Merges authentication, session, policy, NEW assignment and tags
  from a given context into another context. Contexts can be of different type
  (Different callback module).
  """

  def merge(%Ctx{} = base, %Ctx{} = from) do
    %Ctx{base |
      auth: from.auth,
      sess: from.sess,
      policy: from.policy,
    }
      |> assign_new(from.assigns)
      |> add_tags(from.tags)
  end


  def bind(%Ctx{mod: mod, sub: sub} = ctx) do
    case mod.bind(ctx, sub) do
      {:ok, new_sub, proxy} -> {:ok, %Ctx{ctx | sub: new_sub}, proxy}
      {:error, new_sub, reason} -> {:error, %Ctx{ctx | sub: new_sub}, reason}
    end
  end


  def perform(ctx, pipeline, opts \\ [])

  def perform(%Ctx{state: :standby} = ctx, [{_, _, _, _} | _] = pipe, opts) do
    _start_performing(ctx, pipe, opts)
  end

  def perform(%Ctx{state: :standby}, _pipe, _opts) do
    raise ArgumentError, message: "Invalid pipeline"
  end

  def perform(%Ctx{}, _pipe, _opts) do
    raise ArgumentError, message: "Context pipeline's is already running"
  end


  def perform!(ctx, pipeline, opts \\ [])

  def perform!(%Ctx{state: :standby} = ctx, [{_, _, _, _} | _] = pipe, opts) do
    _start_performing!(ctx, pipe, opts)
  end

  def perform!(%Ctx{state: :standby}, _pipe, _opts) do
    raise ArgumentError, message: "Invalid pipeline"
  end

  def perform!(%Ctx{}, _pipe, _opts) do
    raise ArgumentError, message: "Context pipeline's is already running"
  end


  @doc """
  Resumes the execution of an operation. Useful if the operation mas moved
  around to another node and should be resumed there.
  """

  def resume(%Ctx{dir: dir} = ctx, op, args), do: dir.perform(ctx, op, args)


  def done(ctx, result \\ nil)

  def done(%Ctx{state: :running} = ctx, result) do
    _operation_done(ctx, result)
  end

  def done(%Ctx{}, _result) do
    raise ArgumentError, message: "Context pipeline's is not running"
  end


  def failed(ctx, reason, debug \\ nil)

  def failed(%Ctx{state: :running} = ctx, reason, debug) do
    _operation_failed(ctx, reason, debug)
  end

  def failed(%Ctx{}, _reason, _debug) do
    raise ArgumentError, message: "Context pipeline's is not running"
  end


  def produce(%Ctx{state: :running, forked: 0} = ctx, results) do
    # Fork the context for all values
    {ctx2, forks} = Enum.reduce results, {ctx, []}, fn result, {ctx, acc} ->
      {new_ctx, forked_ctx} = _fork(ctx)
      # Mark the operation as done
      new_forked_ctx = _operation_done(forked_ctx, result)
      {new_ctx, [new_forked_ctx | acc]}
    end
    # Join all forked contexts right away
    _straight_join(ctx2, forks)
  end

  def produce(%Ctx{state: :running} = ctx, results) do
    # Fork the context for all values
    {ctx2, forks} = Enum.reduce results, {ctx, []}, fn result, {ctx, acc} ->
      {new_ctx, forked_ctx} = _fork(ctx)
      # Mark the operation as done
      new_forked_ctx = _operation_done(forked_ctx, result)
      {new_ctx, [new_forked_ctx | acc]}
    end
    # Join all forked contexts right away
    _forked_join(ctx2, forks)
  end

  def produce(%Ctx{}, _results) do
    raise ArgumentError, message: "Context pipeline's is not running"
  end


  def map(ctx, [], _fun), do: done(ctx, [])

  def map(ctx, values, fun), do: map(ctx, [], values, fun)


  def fork(%Ctx{state: state} = ctx) when state in [:running, :forking] do
    _fork(ctx)
  end

  def fork(%Ctx{}) do
    raise ArgumentError, message: "Context pipeline's is not running or forking"
  end


  def join(%Ctx{state: :forking, forked: 0} = ctx, forked_contexts) do
    _straight_join(ctx, forked_contexts)
  end

  def join(%Ctx{state: :forking} = ctx, forked_contexts) do
    _forked_join(ctx, forked_contexts)
  end

  def join(%Ctx{}, _branching_contexts) do
    raise ArgumentError, message: "Context pipeline's has not been forked"
  end


  def delegate(%Ctx{state: :running, forked: 0} = ctx, pid)
   when is_pid(pid) do
    {new_ctx, delegated_ctx} = _delegate_prepare(ctx)
    {_straight_delegate_done(new_ctx, pid), delegated_ctx}
  end

  def delegate(%Ctx{state: :running} = ctx, pid)
   when is_pid(pid) do
    {new_ctx, delegated_ctx} = _delegate_prepare(ctx)
    {_forked_delegate_done(new_ctx, pid), delegated_ctx}
  end

  def delegate(%Ctx{state: :running}, _other) do
    raise ArgumentError, message: "Context can only be delegated to a pid"
  end

  def delegate(%Ctx{}, _pid) do
    raise ArgumentError, message: "Context pipeline's is not running"
  end


  def delegate_prepare(%Ctx{state: :running} = ctx) do
    _delegate_prepare(ctx)
  end

  def delegate_prepare(%Ctx{}) do
    raise ArgumentError, message: "Context pipeline's is not running"
  end


  def delegate_done(%Ctx{state: :delegating, forked: 0} = ctx, pid)
   when is_pid(pid) do
    _straight_delegate_done(ctx, pid)
  end

  def delegate_done(%Ctx{state: :delegating} = ctx, pid)
   when is_pid(pid) do
    _forked_delegate_done(ctx, pid)
  end

  def delegate_done(%Ctx{state: :delegating}, _other) do
    raise ArgumentError, message: "Context can only be delegated to a pid"
  end

  def delegate_done(%Ctx{}) do
    raise ArgumentError, message: "Context is not delegating"
  end


  def delegate_failed(ctx, reason, debug \\ nil)

  def delegate_failed(%Ctx{state: :delegating} = ctx, reason, debug) do
    _delegate_failed(ctx, reason, debug)
  end

  def delegate_failed(%Ctx{}, _reason, _debug) do
    raise ArgumentError, message: "Context is not delegating"
  end


  def operation(ctx), do: _curr_op_name(ctx)


  def arguments(ctx), do: _curr_op_args(ctx)


  def value(ctx), do: ctx.result


  def authenticate(%Ctx{state: state, auth: nil, forked: 0} = ctx, auth, policy)
   when state in [:standby, :running] and auth != nil do
    %Ctx{ctx | auth: auth, policy: policy}
      |> _update_ident()
      |> _context_changed()
  end

  def authenticate(%Ctx{state: state, forked: 0} = ctx, auth, _policy)
   when state in [:standby, :running] and auth != nil do
    Errors.already_authenticated!(ctx, auth)
  end

  def authenticate(%Ctx{}, _auth, _policy) do
    raise ArgumentError, message: "Can only authenticate standby or running contexts"
  end


  def attach(%Ctx{state: state, sess: nil, forked: 0} = ctx, sess)
   when state in [:standby, :running] and sess != nil do
    %Ctx{ctx | sess: sess}
      |> _update_ident()
      |> _context_changed()
  end

  def attach(%Ctx{state: state, forked: 0} = ctx, sess)
   when state in [:standby, :running] and sess != nil do
    Errors.already_attached!(ctx, sess)
  end

  def attach(%Ctx{}, _sess) do
    raise ArgumentError, message: "Can only attach standby or running contexts"
  end


  def restore(%Ctx{state: state, auth: nil, sess: nil, forked: 0} = ctx, auth, sess, policy)
   when state in [:standby, :running] and auth != nil and sess != nil do
    %Ctx{ctx | auth: auth, sess: sess, policy: policy}
      |> _update_ident()
      |> _context_changed()
  end

  def restore(%Ctx{state: state, auth: nil, forked: 0} = ctx, _auth, sess, _policy)
   when state in [:standby, :running] do
    Errors.already_attached!(ctx, sess)
  end

  def restore(%Ctx{state: state, forked: 0} = ctx, auth, _sess, _policy)
   when state in [:standby, :running] do
    Errors.already_authenticated!(ctx, auth)
  end

  def restore(%Ctx{}, _auth, _sess, _policy) do
    raise ArgumentError, message: "Can only restore standby or running contexts"
  end


  def update_policy(%Ctx{auth: nil} = ctx, _policy) do
    Errors.not_authenticated!(ctx)
  end

  def update_policy(%Ctx{state: state, forked: 0} = ctx, policy)
   when state in [:standby, :running] do
    %Ctx{ctx | policy: policy}
      |> _policy_changed()
  end

  def update_policy(%Ctx{}, _policy) do
    raise ArgumentError, message: "Can only update policy for standby or running contexts"
  end


  #---------------------------------------------------------------------------
  # Assigns and Tags Handling Functions
  #---------------------------------------------------------------------------

  def fetch_assigned(%Ctx{assigns: assigns}, key) do
    Map.fetch(assigns, key)
  end


  def get_assigned(%Ctx{assigns: assigns}, key, default \\ nil) do
    Map.get(assigns, key, default)
  end


  def assign(ctx, kw \\ []) do
    assigns = Enum.reduce kw, ctx.assigns, fn {k, v}, map ->
      Map.put(map, k, v)
    end
    %Ctx{ctx | assigns: assigns}
  end


  def assign(ctx, key, value) do
    %Ctx{ctx | assigns: Map.put(ctx.assigns, key, value)}
  end


  def assign_new(ctx, map) do
    assigns = Enum.reduce map, ctx.assigns, fn {k, v}, acc ->
      Map.put_new(acc, k, v)
    end
    %Ctx{ctx | assigns: assigns}
  end


  def add_tags(ctx, tags) do
    %Ctx{ctx | tags: MapSet.union(ctx.tags, MapSet.new(tags))}
  end


  def del_tags(ctx, tags) do
    %Ctx{ctx | tags: MapSet.difference(ctx.tags, MapSet.new(tags))}
  end


  #---------------------------------------------------------------------------
  # Protected Functions
  #---------------------------------------------------------------------------

  @doc """
  Only used by Convex.Context.Guards in case an invalid context goes
  out of the protected block  in order to try informing the recipient
  of the system internal error.
  DO NOT USE IF YOU DON'T KNOW WHAT YOU ARE DOING.
  """
  def _internal_failure(%Ctx{mod: mod, sub: sub} = ctx, reason, debug \\ nil) do
    opname = _curr_op_name(ctx)
    {sub2, reason2} = mod.operation_failed(opname, reason, debug, ctx, sub)
    sub3 = mod.pipeline_failed(reason2, ctx, sub2)
    op_trace("ERROR", ctx, [], reason2)
    # We don't touch much the context in case it helps for debugging
    %Ctx{ctx | state: :failed, result: reason2, sub: sub3}
  end


  #---------------------------------------------------------------------------
  # Logging Functions
  #---------------------------------------------------------------------------

  def error(ctx, message) do
    Logger.error("#{_log_prefix(ctx)}#{message}")
    ctx
  end


  def warn(ctx, message) do
    Logger.warn("#{_log_prefix(ctx)}#{message}")
    ctx
  end


  def info(ctx, message) do
    Logger.info("#{_log_prefix(ctx)}#{message}")
    ctx
  end


  def debug(ctx, message) do
    Logger.debug("#{_log_prefix(ctx)}#{message}")
    ctx
  end


  def trace(ctx, msg, error, trace, extra \\ []) do
    Errors.log_exception(msg, error, trace, [
      {:context, ctx.ident},
      {:operation, operation(ctx)},
      {:arguments, arguments(ctx)}
      | extra])
    ctx
  end


  #===========================================================================
  # Access Protocol Callbacks
  #===========================================================================

  def fetch(%Ctx{auth: auth}, :auth), do: {:ok, auth}

  def fetch(%Ctx{sess: sess}, :sess), do: {:ok, sess}

  def fetch(%Ctx{policy: policy}, :policy), do: {:ok, policy}

  def fetch(%Ctx{}, _key), do: :error


  def get(%Ctx{auth: auth}, :auth, _default), do: auth

  def get(%Ctx{sess: sess}, :sess, _default), do: sess

  def get(%Ctx{policy: policy}, :policy, _default), do: policy

  def get(%Ctx{}, _key, default), do: default


  def get_and_update(%Ctx{auth: auth} = ctx, :auth, fun) do
    case fun.(auth) do
      {get_value, new_value} -> {get_value, %Ctx{ctx | auth: new_value}}
      :pop -> {auth, %Ctx{ctx | auth: nil}}
    end
  end

  def get_and_update(%Ctx{sess: sess} = ctx, :sess, fun) do
    case fun.(sess) do
      {get_value, new_value} -> {get_value, %Ctx{ctx | sess: new_value}}
      :pop -> {sess, %Ctx{ctx | sess: nil}}
    end
  end

  def get_and_update(%Ctx{policy: policy} = ctx, :policy, fun) do
    case fun.(policy) do
      {get_value, new_value} -> {get_value, %Ctx{ctx | policy: new_value}}
      :pop -> {policy, %Ctx{ctx | policy: nil}}
    end
  end

  def get_and_update(%Ctx{} = ctx, _key, fun) do
    case fun.(nil) do
      {get_value, _new_value} -> {get_value, ctx}
      :pop -> {nil, ctx}
    end
  end


  def pop(%Ctx{auth: auth} = ctx, :auth), do: {auth, %Ctx{ctx | auth: nil}}

  def pop(%Ctx{sess: sess} = ctx, :sess), do: {sess, %Ctx{ctx | sess: nil}}

  def pop(%Ctx{policy: policy} = ctx, :policy), do: {policy, %Ctx{ctx | policy: nil}}

  def pop(%Ctx{} = ctx, _key), do: {nil, ctx}


  #===========================================================================
  # Internal Functions
  #===========================================================================

  defp extract_opt(opts, key, default_fun) when is_function(default_fun) do
    case Keyword.split(opts, [key]) do
      {[], opts} -> {default_fun.(), opts}
      {[{^key, val}], opts} -> {val, opts}
    end
  end

  defp extract_opt(opts, key, default) do
    case Keyword.split(opts, [key]) do
      {[], opts} -> {default, opts}
      {[{^key, val}], opts} -> {val, opts}
    end
  end


  defp map(ctx, forked, [], _fun), do: Ctx.join(ctx, forked)

  defp map(ctx, forked, [value | values], fun) do
    {ctx2, frk} = Ctx.fork(ctx)
    frk2 = fun.(frk, value)
    map(ctx2, [frk2 | forked], values, fun)
  end


  defp _duplicate_context(ctx, dir, mod, assigns, opts) do
    {new_tags, opts} = extract_opt(opts, :tags, [])
    %Ctx{auth: auth, sess: sess, policy: policy, tags: old_tags} = ctx
    tags = MapSet.union(old_tags, MapSet.new(new_tags))
    {sub, new_assigns} = _mod_init(mod, assigns, opts)
    %Ctx{state: :standby, dir: dir, mod: mod, sub: sub,
         auth: auth, sess: sess, policy: policy,
         assigns: new_assigns, tags: tags}
      |> _update_ident()
  end


  defp _mod_init(mod, old_assigns, opts) do
    case mod.init(opts) do
      {:ok, sub} -> {sub, old_assigns}
      {:ok, sub, %{} = sub_assigns} ->
        {sub, Map.merge(old_assigns, sub_assigns)}
    end
  end


  defp _mod_prepare_recast(mod, assigns, opts) do
    mod.prepare_recast(assigns, opts)
  end


  defp _start_performing(ctx, pipeline, opts) do
    ctx = %Ctx{ctx | state: :running, pipe: pipeline}
    case Guards.protect(ctx, fn -> _perform(ctx) end) do
      {:error, %Ctx{mod: mod, sub: sub} = ctx2} ->
        mod.pipeline_performed(opts, ctx2, sub)
      {:ok, ctx2} ->
        case Guards.ensure_discharged(ctx2) do
          {:ok, %Ctx{mod: mod, sub: sub} = ctx3} ->
            mod.pipeline_performed(opts, ctx3, sub)
          {:error, _reason} = error -> error
        end
    end
  end


  defp _start_performing!(ctx, pipeline, opts) do
    ctx = %Ctx{ctx | state: :running, pipe: pipeline}
    case Guards.protect(ctx, fn -> _perform(ctx) end) do
      {:error, %Ctx{mod: mod, sub: sub} = ctx2} ->
        mod.pipeline_performed!(opts, ctx2, sub)
      {:ok, ctx2} ->
        ctx3 = Guards.ensure_discharged!(ctx2)
        %Ctx{mod: mod, sub: sub} = ctx3
        mod.pipeline_performed!(opts, ctx3, sub)
    end
  end


  defp _fork(%Ctx{mod: mod, sub: sub} = ctx) do
    op_trace("FORK", ctx, [])
    {new_sub, forked_sub} = mod.pipeline_fork(ctx, sub)
    new_ctx = %Ctx{ctx | state: :forking, sub: new_sub}
    forked_ctx = %Ctx{ctx | state: :running, forked: ctx.forked + 1, sub: forked_sub}
    {new_ctx, forked_ctx}
  end


  defp _operation_done(%Ctx{mod: mod, sub: sub} = ctx, result) do
    opname = _curr_op_name(ctx)
    case _finalize_operation(ctx, result) do
      {:failed, ctx2, reason, debug} ->
        _operation_failed(ctx2, reason, debug)
      {:ok, ctx2, result2} ->
        {sub2, result3} = mod.operation_done(opname, result2, ctx2, sub)
        op_trace("DONE", ctx2, [], result3)
        %Ctx{ctx2 | result: result3, sub: sub2}
          |> _perform_next()
    end
  end


  defp _operation_failed(%Ctx{mod: mod, sub: sub} = ctx, reason, debug) do
    # Handle the error regardless of being forked or not
    opname = _curr_op_name(ctx)
    {sub2, reason2} = mod.operation_failed(opname, reason, debug, ctx, sub)
    sub3 = mod.pipeline_failed(reason2, ctx, sub2)
    op_trace("FAILED", ctx, [], reason2)
    %Ctx{ctx | state: :failed, pipe: nil, store: nil, result: reason2, sub: sub3}
  end


  defp _delegate_failed(%Ctx{mod: mod, sub: sub} = ctx, reason, debug) do
    opname = _curr_op_name(ctx)
    {sub2, reason2} = mod.operation_failed(opname, reason, debug, ctx, sub)
    sub3 = mod.pipeline_failed(reason2, ctx, sub2)
    op_trace("DELEGATE.FAILED", ctx, [], reason2)
    %Ctx{ctx | state: :failed, pipe: nil, store: nil,
               result: reason2, delegated: nil, sub: sub3}
  end


  defp _delegate_prepare(%Ctx{depth: depth, mod: mod, sub: sub} = ctx) do
    {sub2, delegated_sub} = mod.pipeline_delegate(ctx, sub)
    new_ctx = %Ctx{ctx | state: :delegating, sub: sub2}
    delegated_ctx = %Ctx{ctx | depth: depth + 1, forked: 0, sub: delegated_sub}
    {new_ctx, delegated_ctx}
  end


  defp _straight_delegate_done(%Ctx{mod: mod, sub: sub} = ctx, pid) do
    sub2 = mod.pipeline_delegated(pid, ctx, sub)
    op_trace("DELEGATE.DONE", ctx, [], pid)
    %Ctx{ctx | state: :delegated, pipe: nil, store: nil,
               result: nil, delegated: pid, sub: sub2}
  end


  defp _forked_delegate_done(ctx, pid) do
    # The results will be handled by the parent context's join call
    op_trace("DELEGATE.DONE", ctx, [], pid)
    %Ctx{ctx | state: :delegated, pipe: nil, store: nil,
               result: nil, delegated: pid}
  end


  defp _straight_join(%Ctx{mod: mod, sub: sub} = ctx, forked_contexts) do
    case _reduce_forked(:forked, [], [], [], forked_contexts) do
      {:failed, reason, _, subs} ->
        new_sub = mod.pipeline_join(subs, ctx, sub)
        # Failure already handled by the forked context
        op_trace("JOIN", ctx, [], length(forked_contexts))
        %Ctx{ctx | state: :failed, result: reason, delegated: nil, sub: new_sub}
      {state, results, delegated, subs} ->
        sub2 = mod.pipeline_join(subs, ctx, sub)
        sub3 = mod.pipeline_forked(results, delegated, ctx, sub2)
        op_trace("JOIN", ctx, [], length(forked_contexts))
        op_trace("FORKED", ctx, [], {results, delegated})
        %Ctx{ctx | state: state, result: results, delegated: delegated, sub: sub3}
    end
  end


  defp _forked_join(%Ctx{mod: mod, sub: sub} = ctx, forked_contexts) do
    # The results will be handled by the parent context's join call
    {state, result, delegated, subs} =
      _reduce_forked(:forked, [], [], [], forked_contexts)
    new_sub = mod.pipeline_join(subs, ctx, sub)
    op_trace("JOIN", ctx, [], length(forked_contexts))
    %Ctx{ctx | state: state, result: result, delegated: delegated, sub: new_sub}
  end


  defp _reduce_forked(:forked, results, delegated, subs, []) do
    {:forked, :lists.append(results), :lists.append(delegated), subs}
  end

  defp _reduce_forked(:failed, reason, delegated, subs, []) do
    {:failed, reason, delegated, subs}
  end

  defp _reduce_forked(_state, _racc, _dacc, _sacc,
      [%Ctx{forked: false} | _rem]) do
    raise Convex.Error, reason: :internal_error,
      message: "tried to join a context that is not the result of a fork"
  end

  defp _reduce_forked(_state, _racc, _dacc, _sacc,
      [%Ctx{state: :running} | _rem]) do
    raise Convex.Error, reason: :internal_error,
      message: "forked context not properly discharged; one of the functions 'done', 'failed' or 'delegate' must be called"
  end

  defp _reduce_forked(_state, _racc, _dacc, _sacc,
      [%Ctx{state: :delegating} | _rem]) do
    raise Convex.Error, reason: :internal_error,
      message: "forked context not properly delegated; one of the functions 'delegate_done' or 'delegated_failed' must be called"
  end


  defp _reduce_forked(:failed, reason, delegated, sacc,
      [%Ctx{state: _state, sub: s} | rem]) do
    _reduce_forked(:failed, reason, delegated, [s | sacc], rem)
  end

  defp _reduce_forked(:forked, _racc, _dacc, sacc,
      [%Ctx{state: :failed, result: r, sub: s} | rem]) do
    _reduce_forked(:failed, r, nil, [s | sacc], rem)
  end

  defp _reduce_forked(:forked, racc, dacc, sacc,
      [%Ctx{state: :forked, result: r, delegated: d, sub: s} | rem]) do
    _reduce_forked(:forked, [r | racc], [d | dacc], [s | sacc], rem)
  end

  defp _reduce_forked(:forked, racc, dacc, sacc,
      [%Ctx{state: :done, result: r, sub: s} | rem]) do
    _reduce_forked(:forked, [[r] | racc], dacc, [s | sacc], rem)
  end

  defp _reduce_forked(:forked, racc, dacc, sacc,
      [%Ctx{state: :delegated, delegated: d, sub: s} | rem]) do
    _reduce_forked(:forked, racc, [[d] | dacc], [s | sacc], rem)
  end


  defp _perform_next(%Ctx{pipe: [_, next | rem]} = ctx) do
    # Perform next operation.
    %Ctx{ctx | pipe: [next | rem]}
      |> _perform()
  end

  defp _perform_next(%Ctx{forked: 0} = ctx) do
    # No more operation to perform and not in a fork.
    %Ctx{result: result, mod: mod, sub: sub} = ctx
    op_trace("FINISHED", ctx, [], result)
    sub2 = mod.pipeline_done(result, ctx, sub)
    %Ctx{ctx | state: :done, pipe: nil, store: nil, sub: sub2}
  end

  defp _perform_next(ctx) do
    # In a forked context, the result will be handled in parent context join
    %Ctx{ctx | state: :done, pipe: nil, store: nil}
  end


  defp _perform(%Ctx{} = ctx) do
    case _prepare_operation(ctx) do
      {:perform, ctx2, op, args} ->
        op_trace("PERFORM", ctx2, args)
        resume(ctx2, op, args)
      {:continue, ctx2} ->
        _perform_next(ctx2)
    end
  end


  defp _prepare_operation(%Ctx{pipe: [{:pack, pack, nil} | _]} = ctx) do
    {:continue, %Ctx{ctx | result: pack.(ctx)}}
  end

  defp _prepare_operation(%Ctx{pipe: [{:pack, pack, var} | _]} = ctx) do
    result = pack.(ctx)
    store = Map.put(ctx.store, var, result)
    {:continue, %Ctx{ctx | result: result, store: store}}
  end

  defp _prepare_operation(%Ctx{pipe: [{op, args, nil, _} | _]} = ctx) do
    {:perform, ctx, op, args}
  end

  defp _prepare_operation(%Ctx{pipe: [{op, args, mutations, _} | _]} = ctx) do
    {:perform, ctx, op, _mutate_args(ctx, args, mutations)}
  end


  defp _mutate_args(_ctx, args, []), do: args

  defp _mutate_args(ctx, args, [{:store, akey, skey, []} | rem]) do
    _mutate_args(ctx, Map.put(args, akey, Map.get(ctx.store, skey)), rem)
  end

  defp _mutate_args(ctx, args, [{:store, akey, skey, path} | rem]) do
    value = get_in(Map.get(ctx.store, skey), path)
    _mutate_args(ctx, Map.put(args, akey, value), rem)
  end

  defp _mutate_args(ctx, args, [{:ctx, akey, []} | rem]) do
    _mutate_args(ctx, Map.put(args, akey, compact(ctx)), rem)
  end

  defp _mutate_args(ctx, args, [{:ctx, akey, path} | rem]) do
    _mutate_args(ctx, Map.put(args, akey, get_in(ctx, path)), rem)
  end

  defp _mutate_args(ctx, args, [{:merge, map} | rem]) do
    _mutate_args(ctx, Map.merge(map, args), rem)
  end


  defp _finalize_operation(%Ctx{pipe: [{_, _, _, nil} | _]} = ctx, result) do
    {:ok, ctx, result}
  end

  defp _finalize_operation(%Ctx{pipe: [{_, _, _, key} | _]} = ctx, result)
   when is_atom(key) do
    {:ok, %Ctx{ctx | store: Map.put(ctx.store, key, result)}, result}
  end

  defp _finalize_operation(%Ctx{pipe: [{_, _, _, fun} | _]} = ctx, result)
   when is_function(fun) do
    try do
      {ctx, result} = fun.(ctx, result)
      {:ok, ctx, result}
    rescue
      e -> {:failed, ctx, :invalid_result, Exception.message(e)}
    end
  end


  defp _context_changed(%Ctx{mod: mod, sub: sub} = ctx) do
    %Ctx{ctx | sub: mod.context_changed(ctx, sub)}
  end


  defp _policy_changed(%Ctx{mod: mod, sub: sub} = ctx) do
    %Ctx{ctx | sub: mod.policy_changed(ctx, sub)}
  end


  defp _curr_op_name(%Ctx{pipe: [{op, _, _, _} | _]}), do: op

  defp _curr_op_name(_ctx), do: nil


  defp _curr_op_args(%Ctx{pipe: [{_, args, _, _} | _]}), do: args

  defp _curr_op_args(_ctx), do: nil


  defp _update_ident(%Ctx{mod: mod, sub: sub} = ctx) do
    %Ctx{ctx | ident: "<#{mod.format_ident(_format_ident(ctx), sub)}>"}
  end


  defp _log_prefix(%Ctx{ident: ident, pipe: pipeline}) do
    "#{ident}#{_format_pipeline(pipeline)}; "
  end


  defp _format_ident(%Ctx{auth: auth}), do: Auth.describe(auth)


  defp _format_pipeline(nil), do: ""

  defp _format_pipeline([]), do: ""

  defp _format_pipeline([{op, _, _, _} | _]), do: " #{Format.opname(op)}"

end
