defmodule Convex.Context do

  @moduledoc """
  This defines the structure that is passed around to perform the operation
  pipeline, and the functions to interact with it.

  The context encapsulates the pipeline operations and the state of the execution.
  It uses a callback module to alter its behavior in function of the initiator
  requirments.

  The context structure implements the `Access` protocol for its *public*
  fields `auth`, `sess` and `policy`.
  These fields are opaque for `Convex`, they are defined by the application.
  The only requirment is for the application to ensure these values support
  the `Convex.Auth`, `Convex.Sess` and `Convex.Policy` protocols respectively.


  ## Concepts

  ### Callback Modules

  The effects of performing operations is driven by the context callback module
  it has been created with. The effects could be:

    - Blocking:

      When performing an operation pipeline, the call blocks and the final
      result is returned (See `Convex.Context.Sync`).

    - Asynchronous:

      When performing an operation pipeline, the call returns right away and
      the final result is lost (See `Convex.Context.Async`).

    - Monitored:

      When performing an operation pipeline, the call return right away
      and the context sends messages to a given process so execution can
      be monitored and the results or failure can be gathered
      (See `Convex.Context.Process` and `Convex.Handler.Process`).

  Sometime you want to perform a new pipeline from inside an operation handler.
  For example a service could be handling an operation that requires performing
  a different operation synchronously to be able to return a result.
  The service could create a new `Convex.Context.Sync` context but it
  would lose all the current context meta-data like authentication, session,
  policy, assigned values and tags. To keep this information the `recast/2`
  or `recast/3` function can be used to create a new context with a different
  callback module but keeping all the meta-data. Even simpler would be to use
  the callback module helper functions like `Convex.Context.Sync.recast/1`
  or `Convex.Context.Sync.recast/2`:

    ```Elixir
    result = perform! with: Convex.Context.Sync.recast(ctx) do
      some.opeation ^args
    end
    Convex.Context.done(ctx, result)

    ```

  ### Delegating

  When a service wants an operation to be performed by a different process
  it needs to delegate to it. The delegation ensure the operation execution
  is properly monitored and allow the pipeline initiator to be notified if
  any involved process dies unexpectedly.

  More detailed information on how to delegate in the later section
  [*Delegated Execution*](#module-delegated-execution).


  ### Forking (Producing)

  An operation can fork the context to generate multiple results.

  This can be done by either forking the context explicitly, by using the
  `Convex.Pipeline.fork/2` macro or simply by producing multiple results
  calling `Convex.Context.produce/2` or `Convex.Context.map/3`.

  When an operation is forked, the following operations in the pipelines are
  performed **for each** of the produced results.

  e.g.

  Given the pipeline (See `Convex.Pipeline`):

    ```Elixir
    perform ctx do
      id = find.something tag: "foo"
      value = produce.values id: id
      process.value value: value
    end
    ```

  With the operations being:

    - `find.something`: an operation returning a single identifier.
    - `produce.values`: an operation producing (forking) multiple values
      for a given identifier.
    - `number.duplicate`: an operation processing a **single** value
      and returning a **single** result.

  Performing the pipeline will perform the following operations:
    - `find.something` with arguments `%{tag: "foo"}` -> succeed with `42`
    - `produce.values` with arguments `%{id: 42}` -> **produce** [1, 2, 3]
    - `process.value` with arguments `%{value: 1}` -> succeed with `2`
    - `process.value` with arguments `%{value: 2}` -> succeed with `4`
    - `process.value` with arguments `%{value: 3}` -> succeed with `6`

  The `process.value` operations may be performed in parallel,
  and the pipeline final result order is **NOT GUARANTEED** .
  For example in the described use case the pipeline result could be:

    `[4, 2, 6]`, `[6, 4, 2]` or `[2, 4, 6]`

  Any number of operations in a pipeline can fork the context resulting
  in the context propagation looking like a tree with all the results
  being sent back to the initiator directly.

  More detailed information on how to fork in the later section
  [*Forked Execution*](#module-forked-execution).


  ## Context Meta-Data

  A context contains data that follow it arround to the operation handlers.
  It contains:
    - authentication data (`auth` field).
    - session data (`sess` field).
    - access policy (`policy` field).
    - tags (`tags` field).
    - assigned values.


  ### Authentication and Session  Data

  The *public* field `auth` of a context contains the application-defined
  authentication information. The only requirment for the data type is that
  it supports `Convex.Auth` protocol.

  Another *public* field related to authentication is `policy`. The policy is
  some application-defined data supporting `Convex.Policy` protocol that could
  describe any access policy for the authenticated entity.
  The main difference between `auth` and `policy` is that policy could change
  while `auth` is not supposed to.

  One of the differences between this data and the tags and assigned values
  is that when updating it the context is considered to
  have changed in a way the initiator of the pipeline might be interested in.
  This means that all the functions modifying this data will trigger the
  callback module callback function `c:context_changed/2` to be called.
  The callback module is then responsible to send these changes to the
  initiator if this is the desired behavior (See `Convex.Context.Process`).

  For the same reasons, if the policy changes in the context, the callbak
  module function `c:policy_changed/2` will be called.


  #### Pre-Authenticate

  To authenticate a context and define its policy, the functions
  `authenticate/3` could be called before performing a pipeline:

    ```Elixir
    ctx = Convex.Context.authenticate(ctx, auth_data, policy_data)
    perform ctx do
      do.something ^args
    end
    ```

  This is equivalent to:

    ```Elixir
    perform with: ctx, as: auth_data, policy: policy_data do
      do.something ^args
    end
    ```


  #### Authenticated by an Operation

  Another possibility is for an operation to authenticate the context.
  If an operation does:

    ```Elixir
    ctx
    |> Convex.Context.authenticate(auth_data, policy_data)
    |> Convex.Context.done(result)
    ```

  any further operation handler in the pipeline will have access to the
  `auth` and `policy` data and be considered authenticate:

    ```Elixir
    perform ctx do
      user_id = users.authenticate username: ^username, password: ^password
      some.authenticated.operation %{^args | user_id: user_id}
    end
    ```


  #### Attaching to Session

  In the same way the context contains authentication data, it can contain
  session data. Like with authentication data, it can be set by calling
  `attach/2` either before performing a pipeline or from inside an operation
  handler:

  Example of pipeline handling authentication and sessions:

    ```Elixir
    perform ctx do
      users.authenticate username: ^username, password: ^password
      sessions.attach session_id: ^session_id
      do.something ^args
    end
    ```

  In this example the operation `users.authenticate` must call
  `authenticate/3` and the operation `sessions.attach` must call `attach/2`.


  #### Restoring Authentication and Session

  if both authentication data and session data can be set at the same time
  it is called restoration and can be done using function `restore/4`:

    ```Elixir
    Convex.Context.restore(ctx, auth_data, session_data, policy_data)
    ```

  As for `authenticate/3` and `attach/2`, `restore/4` can be called in an
  operation handler.


  ### Assigned Values

  A context can be assigned key-value pairs that will move arround to the
  services operation handlers. It can be used to store metadata like user agent,
  client IP, debuging information...

  Assigning a value is done using the functions `assign/2` or `assign/3`:

    ```Elixir
    ctx
    |> Convex.Context.assign(:foo, "something")
    |> Convex.Context.assign([bar: 1, spam: true])
    ```

  If a service wants to assign some values only if they are not already defined
  it can use function `assign_new/2`. Only new keys will be added the the
  context meta-data, already existing keys will stay untouched:

    ```Elixir
    Convex.Context.assign_new([bar: 1, spam: true])
    ```

  These values can be accessed by any operation handler using
  `get_assigned/2`, `get_assigned/3` or `fetch_assigned/2` functions:

    ```Elixir
    foo = Convex.Context.get_assign(ctx, :foo, "default")
    bar = Convex.Context.get_assign(ctx, :bar)
    {:ok, spam} = Convex.Context.fetch_assign(ctx, :spam)
    ```


  ### Context Tags

  Another meta-data the context contains is a set of untyped tags.

  These tags can be used for metrics, debugging...

  At any time tags can be added or removed and all further operation handler
  in the pipeline can retrieve them using the context `tags` field.

  Adding tags with the function `add_tags/2`:

    ```Elixir
    ctx = Convex.Context.add_tags(ctx, ["foo", :bar, 123])
    ctx.tags == MapSet.new(["foo", :bar, 123])

    ```

  Deleting tags with the function `del_tags/2`:

    ```Elixir
    ctx = Convex.Context.del_tags(ctx, ["foo", "spam"])
    ctx.tags == MapSet.new([:bar, 123])
    ```


  ## Operation Handling

  An operation pipeline is a list of operations, each with their arguments and
  a recipe how the result should be stored.

  For each operations, the context uses the director passed as option or the
  globally configured one (See `new/2` and `Convex.Config`) to route itself
  to the service providing the operation. The service can make the pipeline
  progress in one of the following ways:


  ### Successful Operation

  If the operation execution succeed, the service need to call the function
  `done/1` or `done/2` with the result:

    ```Elixir
    Convex.Context.done(ctx, result)
    ```

  When an operation is done, the context store the result in the context
  possibly unpacking it into multiple stored values and continue to the next
  operation in the pipeline. For that it pops the next operation from the
  pipeline and calls the director to route the next operation.

  If the operation was the last one in the pipeline, the pipeline is terminated.


  ### Failed Operation

  If the operation failed, the service needs to call the function `failed/2`
  or `failed/3` with the reason for the failure:

    ```Elixir
    Convex.Context.failed(ctx, :some_error)
    ```

  As soon as one of the operations fails, the pipeline is terminated.


  ### Delegated Operation

  It the service wants to delegate the operation to another process, there
  is two possible way to do so.

  If the process the operation is to be delegated to is already running,
  the service needs to call the function `delegate/2`. It will return a new
  context to be passed to the process the operation is delegated to so it can
  continue the operation processing. It is the responsibility of the service
  to send the new context to the process.

    ```Elixir
    {ctx, delegated_ctx} = Convex.Context.delegate(ctx, pid)
    send pid, {:do_something, delegated_ctx}
    ctx

    ```

  If a new process needs to be spawned, and the new context must be passed
  before even knowing the pid, the delegation **MUST** be done in two
  steps:

  ```Elixir
  {ctx, delegated_ctx} = Convex.Context.delegate_prepare(ctx)
  pid = spawn(fn -> do_something(delegated_ctx) end)
  Convex.Context.delegate_done(ctx, pid)

  ```

  If after calling `delegate_prepare/1` an error happend while spawning
  a process, the function `delegate_failed/2` or `delegate_failed/3` **MUST**
  be called:

  ```Elixir
  {ctx, delegated_ctx} = Convex.Context.delegate_prepare(ctx)
  try do
    spawn_process(delegated_ctx)
  rescue
    _ -> Convex.Context.delegate_failed(ctx, :some_error)
  else
    pid -> Convex.Context.delegate_done(ctx, pid)
  end

  ```

  After the operation got delegated, the service shouldn't do anything more
  with the context beside returning it.


  ### Forked Execution

  An operation can be forked to return multiple results that will be processed
  independently by the further operations in the pipeline.

  This can be done in different ways:

  #### Explicit Forking

  Every explicit fork must **ALWAYS** be joined back:

    ```Elixir
      {ctx, forked_ctx1} = Convex.Context.fork(ctx)
      forked_ctx1 = do_something(forked_ctx1, :foo)
      {ctx, forked_ctx2} = Convex.Context.fork(ctx)
      forked_ctx2 = do_something_else(forked_ctx2, :bar)
      Convex.Context.join(ctx, [forked_ctx1, forked_ctx2])
    ```

  This will make the operation generate two results.
  The forked contexts can be used to continue the processing of the operation
  the exact same way it would have been done with the source context,
  they can succeed or fail, they can be delegated or even forked again.


  #### Forking Using Helper Macro

  The same things described before can be done in a simpler way using
  the macro `Convex.Pipeline.fork/2`:

    ```Elixir
      fork ctx do
        do_something(:foo)
        do_something_else(:bar)
      end
    ```

  See `Convex.Pipeline` documentation for more information.


  #### Producing Multiple Results

  If the service already have the results it wants to fork the operation with,
  it can simply produce them using `Convex.Context.produce/2` function:

    ```Elixir
    Convex.Context.produce(ctx, [1, 2, 3])
    ```

  or `Convex.Context.map/3`:

    ```Elixir
    Convex.Context.map(ctx, [1, 2, 3], &(&1 * 2))
    ```

  With these calls, the rest of the pipleline operations will be performes
  for each values.
  """

  @behaviour Access


  #===========================================================================
  # Includes
  #===========================================================================

  use Convex.Tracer

  require Logger

  alias __MODULE__, as: This

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

  @type t :: %Ctx{
    # "public" fields available through Access protocol
    auth: nil | Auth.t,
    sess: nil | Sess.t,
    policy: nil | Policy.t,

    # "public" fields
    tags: MapSet.t,
    depth: integer,

    # Internal fields
    state: :standby | :reset | :running | :forking | :delegating | :failed | :done,
    forked: integer,
    dir: module,
    mod: module,
    sub: any,
    ident: nil | String.t,
    result: any,
    delegated: pid,
    store: map,
    pipe: nil | Pipeline.t,
    assigns: map,
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

  @doc """
  Called by the context when created to initialize the callback module.

  Should returns the callback module state that will be passed to every calls.

  In addition the callback module can return a map of key-values that will be
  merged in the context assigned values. This is usefull if the associated
  `Convex.Proxy` callback module needs some extra data.

  Called by `new/1` and `new/2`.

  """
  @callback init(options :: Keyword.t)
    :: state :: any
     | {state :: any, assigns :: map}

  @doc """
  Called to update context's assigned values when recasting to a different
  callback module.

  It receives the extra options passed to `recast/3` and return the updated
  assigned values that will be used with the new recasted context.

  Called by `recast/2` and `recast/3`.
  """
  @callback prepare_recast(assigns :: map, options :: Keyword.t)
    :: assigns :: map

  @doc """
  Called to format the description string of the context.

  Allow the callback module to customize the way the context will look
  when using the logging functions.

  Called by `new/1`, `new/2`, `recast/2`, `recast/3`, `clone/1`, `clone/2`,
  `authenticate/3`, `attach/2` and `restore/4`.

  """
  @callback format_ident(binary, state :: any)
    :: description :: String.t

  @doc """
  Called to create a binding proxy.

  The proxy will be a communication channel between the calling process
  and the initiator of the pipeline.

  Called by `bind/1`.
  """
  @callback bind(context :: This.t, state :: any)
    :: Proxy.t


  @doc """
  Called to when an operation succeeded.

  Called by `done/1`, `done/2` and `produce/2`.
  """
  @callback operation_done(operation :: op, result :: any,
                           context :: This.t, state :: any)
    :: {state :: any, term}

  @doc """
  Called when an operation failed.

  Called by `failed/2` (and by `done/1` and `done/2` if some error is raised
  while *unpacking* the operation result).
  """
  @callback operation_failed(operation :: op, reason :: any, debug :: any,
                             context :: This.t, state :: any)
    :: {state :: any, reason :: any}

  @doc """
  Called when forking a context to get the forked state.

  The returned forked state will be used in the forked context.

  Called by `fork/1` and `produce/2`.
  """
  @callback pipeline_fork(context :: This.t, state :: any)
    :: {base_state :: any, forked_state :: any}

  @doc """
  Called when joining multiple forked callback module states.

  Called by `join/2` and `produce/2`.
  """
  @callback pipeline_join(forked_states :: [any],
                          context :: This.t, state :: any)
    :: state :: any

  @doc """
  Called when delegating an operation to create the callback module state
  of the delegated context.

  Called by `delegate/2` and `delegate_prepare/1`.
  """
  @callback pipeline_delegate(context :: This.t, state :: any)
    :: {base_state :: any, delegated_state :: any}

  @doc """
  Called when the context has been delegated successfully.

  Called by `delegate/2` and `delegate_done/2`.
  """
  @callback pipeline_delegated(delegated_pid :: pid, context :: This.t,
                               state :: any)
    :: state :: any

  @doc """
  Called when the context has been joined successfully after forks.

  Called by `produce/2` and `join/2`.
  """
  @callback pipeline_forked(results :: [any], delegated :: [pid],
                            context :: This.t, state :: any)
    :: state :: any

  @doc """
  Called when any pipeline operation failed.

  No more callbacks will be called after this.
  """
  @callback pipeline_failed(reason :: any, context :: This.t, state :: any)
    :: state :: any

  @doc """
  Called when all pipeline operations suceeded.

  No more callbacks will be called after this.
  """
  @callback pipeline_done(result :: any, context :: This.t, state :: any)
    :: state :: any

  @doc """
  Called when the context authentication or session information changed.
  """
  @callback context_changed(context :: This.t, state :: any)
    :: state :: any

  @doc """
  Called when the context policy changed.
  """
  @callback policy_changed(context :: This.t, state :: any)
    :: state :: any

  @doc """
  Called in the initiator process when the pipeline has been performed.

  This does **NOT** mean all the operations have been executed, they may
  have neen delegated to other processes.

  This is where the callback module can deside to block and wait for
  all the operations to be executed (See `Convex.Context.Sync`) or
  just return right away (See `Convex.Context.Async`).

  This function should not raise any exception, all the errors must
  be returned as a result.
  """
  @callback pipeline_performed(options :: Keyword.t, context :: This.t,
                               state :: any)
    :: {:ok, any} | {:error, any}

  @doc """
  Same as callback `c:pipeline_performed/3` but errors will be raised
  as exceptions and result will be returned directly.
  """
  @callback pipeline_performed!(options :: Keyword.t, context :: This.t,
                                state :: any)
    :: any | no_return()


  #===========================================================================
  # API Functions
  #===========================================================================

  @spec new(callback_module :: module) :: context :: This.t
  @spec new(callback_module :: module, options :: Keyword.t) :: context :: This.t
  @doc """
  Creates a new context using the given callback module.

  Callback module could be one of `Convex.Context.Async`, `Convex.Context.Sync`,
  `Convex.Context.Process` or any custom module implementing `Convex.Context`
  behaviour.

  Options `director` and `tags` will be handled by the context, any other
  options will be passed to the callback module `c:init/1` function.

    - `director`:

      The director module to use to route operations (See `Convex.Director`).
      If not specified the director will be retrieved from conviguration
      (See `Convex.Config`).

    - `tags`: `MapSet` or `list` of tags associated with the created context.

  """

  def new(mod, opts \\ []) when is_atom(mod) do
    {dir, opts} = extract_opt(opts, :director, &Config.director/0)
    {tags, opts} = extract_opt(opts, :tags, [])
    {sub, assigns} = _mod_init(mod, %{}, opts)
    %Ctx{state: :standby, dir: dir, mod: mod, sub: sub,
         assigns: assigns, tags: MapSet.new(tags)}
      |> _update_ident()
  end


  @spec authenticated?(context :: This.t) :: boolean
  @doc """
  Tells if the contrext is authenticated.
  """

  def authenticated?(%Ctx{} = ctx), do: ctx.auth != nil


  @spec attached?(context :: This.t) :: boolean
  @doc """
  Tells if the contrext is attached to a session.
  """

  def attached?(%Ctx{} = ctx), do: ctx.sess != nil


  @spec operation(context :: This.t) :: operation :: This.op
  @doc """
  Returns the context current operation name.
  """

  def operation(ctx), do: _curr_op_name(ctx)


  @spec arguments(context :: This.t) :: args :: map
  @doc """
  Returns the context current operation arguments.
  """

  def arguments(ctx), do: _curr_op_args(ctx)


  @spec value(context :: This.t) :: any
  @doc """
  Returns the result of the last context operation.
  """

  def value(ctx), do: ctx.result


  @spec recast(context :: This.t, callback_module :: module) :: context :: This.t
  @spec recast(context :: This.t, callback_module :: module, Keyword.t)
    :: context :: This.t
  @doc """
  Create a new context with the specified callback module but keeping most
  of the current meta-data.

  Some of the assigned values MAY be changed by the current backend
  before the switch to the new backend (through `c:prepare_recast/2` callback).

  This should be mostly used from the callback module's `recast` helper
  functions like `Convex.Context.Sync.recast/1`, `Convex.Context.Sync.recast/2`,
  `Convex.Context.Async.recast/1` or `Convex.Context.Async.recast/2`
  """

  def recast(%Ctx{} = ctx, mod, opts \\ []) when is_atom(mod) do
    {dir, opts} = extract_opt(opts, :director, &Config.director/0)
    new_assigns = _mod_prepare_recast(ctx.mod, ctx.assigns, opts)
    _duplicate_context(ctx, dir, mod, new_assigns, opts)
  end


  @spec clone(context :: This.t) :: context :: This.t
  @spec clone(context :: This.t, options :: Keyword.t) :: context :: This.t
  @doc """
  Clones given context.

  Options:

    - `tags`: tags to be merged into the given context tags.

  The callback module's `c:init/1` function will be called again with the extra
  options.
  """

  def clone(%Ctx{} = ctx, opts \\ []) do
    _duplicate_context(ctx, ctx.dir, ctx.mod, ctx.assigns, opts)
  end


  @spec compact(context :: This.t) :: context :: This.t
  @doc """
  Converts the given context to its smallest form still usable to
  recast for starting a new operation pipeline.

  The only data keept is:
    - The callback module (but not its state).
    - The director.
    - The authentication data.
    - The session data.
    - The access policy.

  After calling this function the context cannot be used to continue
  the processing of an operation pipeline, but it can be used to start a new one.

  This is called when returning a context as a pipeline result.

  A compacted context cannot be used to perform a pipeline without recasting,
  but it can be merged into another context to extract autoentication, session
  and policy data.
  """

  def compact(%Ctx{} = ctx) do
    %Ctx{dir: dir, mod: mod, auth: auth, sess: sess, policy: policy} = ctx
    %Ctx{state: :standby, dir: dir, mod: mod, sub: nil,
         auth: auth, sess: sess, policy: policy}
  end


  @spec reset(context :: This.t) :: context :: This.t
  @doc """
  Resets a context keeping **ONLY** the callback module and director.

  The returned context can only be used after recasting.
  """

  def reset(%Ctx{} = ctx) do
    %Ctx{dir: dir, mod: mod} = ctx
    %Ctx{state: :reset, dir: dir, mod: mod}
  end


  @spec merge(base_context :: This.t, context :: This.t) :: context :: This.t
  @doc """
  Merges authentication, session, policy, NEW assignment and tags
  from a given context into base context. Contexts can be of different type
  (different callback module).
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


  @spec perform(context :: This.t, pipeline :: Pipeline.t, options :: Keyword.t)
    :: {:ok, result :: any()} | {:error, reason :: any()}
  @doc """
  Performs a pipeline without exception.

  The pipeline could be created using `Convex.Pipeline.prepare/1`.

  The options are passed to the callback module function `c:pipeline_performed/3`.
  """

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


  @spec perform!(context :: This.t, pipeline :: Pipeline.t, options :: Keyword.t)
    :: (result :: any()) | no_return()
  @doc """
  Performs a pipeline raising an exception in case of failure and returning
  any possible result directly.

  The pipeline could be created using `Convex.Pipeline.prepare/1`.

  The options are passed to the callback module function `c:pipeline_performed!/3`.
  """

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


  @spec resume(context :: This.t, operation :: This.op, args :: map)
    :: context :: This.t
  @doc """
  Resumes the execution of an operation. Useful if the operation was moved
  around to another node and should be resumed there.
  """

  def resume(%Ctx{dir: dir} = ctx, op, args), do: dir.perform(ctx, op, args)


  @spec bind(context :: This.t)
    :: {:ok, context :: This.t, proxy :: Proxy.t}
     | {:error, context :: This.t, reason :: any}
  @doc """
  Creates a proxy to open a channel with the initiator of a pipeline.

  This allow the holder of the proxy the ability to send messages directly
  to the initiator, update initiator policy and request the initiator
  to close. See `Convex.Proxy` for more information.

  The proxy behavior will depend on the context callback module.
  """

  def bind(%Ctx{mod: mod, sub: sub} = ctx) do
    case mod.bind(ctx, sub) do
      {:ok, new_sub, proxy} -> {:ok, %Ctx{ctx | sub: new_sub}, proxy}
      {:error, new_sub, reason} -> {:error, %Ctx{ctx | sub: new_sub}, reason}
    end
  end


  @spec done(context :: This.t) :: context :: This.t
  @spec done(context :: This.t, result :: any) :: context :: This.t
  @doc """
  Marks the current operation as done.

  If no result is specified for the operation it will be `nil`.

  When called, the next operation in the pipeline will be performed.

  After calling this function the context should be returned and no more
  functions must be called.
  """

  def done(ctx, result \\ nil)

  def done(%Ctx{state: :running} = ctx, result) do
    _operation_done(ctx, result)
  end

  def done(%Ctx{}, _result) do
    raise ArgumentError, message: "Context pipeline's is not running"
  end


  @spec failed(context :: This.t, reason :: any) :: context :: This.t
  @spec failed(context :: This.t, reason :: any, debug :: any) :: context :: This.t
  @doc """
  Marks the current operation as failed.

  No more operation in the pipeline will be performed.

  After calling this function the context should be returned and no more
  functions must be called.
  """

  def failed(ctx, reason, debug \\ nil)

  def failed(%Ctx{state: :running} = ctx, reason, debug) do
    _operation_failed(ctx, reason, debug)
  end

  def failed(%Ctx{}, _reason, _debug) do
    raise ArgumentError, message: "Context pipeline's is not running"
  end


  @spec produce(context :: This.t, results :: [any]) :: context :: This.t
  @doc """
  Forks the pipeline multiple times so it processes all the given results.

  The following execution **MAY** be paralellized if any handler delegate
  to other processes.

  **IMPORTANT**: The final result order is **NOT** guaranteed.
  """

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


  @spec map(context :: This.t, values :: [any], (any -> any))
    :: context :: This.t
  @doc """
  Forks the pipeline multiple times so it processes all the results of the
  mapping function for each given values.

  The following execution **MAY** be paralellized if any handler delegate
  to other processes.

  **IMPORTANT**: The final result order is **NOT** guaranteed.
  """

  def map(ctx, [], _fun), do: done(ctx, [])

  def map(ctx, values, fun), do: map(ctx, [], values, fun)


  @spec fork(context :: This.t)
    :: {base_context :: This.t, forked_context :: This.t}
  @doc """
  Forks the context so the operation can generate two results.

  The base context can be forked multiple times.

  The forked context can be used the same way the base context could have
  been used, it can make the operation as done or failed, it can delegate
  it to another process or even fork it again.

  After calling this function, the corresponding `Convex.Context.join/2`
  **MUST** be called with all the forked contexts.
  """

  def fork(%Ctx{state: state} = ctx) when state in [:running, :forking] do
    _fork(ctx)
  end

  def fork(%Ctx{}) do
    raise ArgumentError, message: "Context pipeline's is not running or forking"
  end


  @spec join(base_context :: This.t, forked_contexts :: [This.t])
    :: context :: This.t
  @doc """
  Join all forked contexts out of a base context.

  This is used to gather results and monitor any delegated contexts.

  This function **MUST** be called every time `Convex.Context.fork/1`
  as been called at least once with the base context.
  If the same context is forked multiple times, this function only need
  to be called once with all the forked contexts.
  """

  def join(%Ctx{state: :forking, forked: 0} = ctx, forked_contexts) do
    _straight_join(ctx, forked_contexts)
  end

  def join(%Ctx{state: :forking} = ctx, forked_contexts) do
    _forked_join(ctx, forked_contexts)
  end

  def join(%Ctx{}, _branching_contexts) do
    raise ArgumentError, message: "Context pipeline's has not been forked"
  end


  @spec delegate(context :: This.t, pid :: pid)
    :: {base_context :: This.t, delegated_context :: This.t}
  @doc """
  Delegates the execution of the current operation to another process.

  After calling this function, the delegated context must be passed to
  the process alongside any information it would requires to perform it,
  and no further functions must be called on the base context.
  """

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


  @spec delegate_prepare(context :: This.t)
    :: {base_context :: This.t, delegated_context :: This.t}
  @doc """
  Prepares the context to be delegated to another process.

  If the delegated context is required before we know the pid of the process
  it will be delegated to, for example when spawning a new process, this
  function is used to create prepare the delegation.

  After calling this function, either `Convex.Context.delegate_done/2`,
  `Convex.Context.delegate_failed/2` or `Convex.Context.delegate_failed/3`
  **MUST** be called.
  """

  def delegate_prepare(%Ctx{state: :running} = ctx) do
    _delegate_prepare(ctx)
  end

  def delegate_prepare(%Ctx{}) do
    raise ArgumentError, message: "Context pipeline's is not running"
  end


  @spec delegate_done(base_context :: This.t, pid :: pid) :: context :: This.t
  @doc """
  Completes the delegation of the current operation to another process.

  After calling `Convex.Context.delegate_prepare/1` and the pid of the process
  the operation has been delegated to is known this function **MUST** be called.

  After this call no further context functions must be called.
  """

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


  @spec delegate_failed(base_context :: This.t, reason :: any)
    :: context :: This.t
  @spec delegate_failed(base_context :: This.t, reason :: any, debug :: any)
    :: context :: This.t
  @doc """
  Cancel a prepared delegation.

  After calling `Convex.Context.delegate_prepare/1`, if anything fail
  this function **MUST** be called.

  After this call no further context functions must be called.
  """

  def delegate_failed(ctx, reason, debug \\ nil)

  def delegate_failed(%Ctx{state: :delegating} = ctx, reason, debug) do
    _delegate_failed(ctx, reason, debug)
  end

  def delegate_failed(%Ctx{}, _reason, _debug) do
    raise ArgumentError, message: "Context is not delegating"
  end


  @spec authenticate(context :: This.t, auth :: Auth.t, policy :: Policy.t)
    :: context :: This.t
  @doc """
  Sets the context's authentication data and access policy.

  The given data must implements the protocols `Convex.Auth` and `Convex.Policy`
  respictively.

  If the authentication data was already set this function will raise an exception.

  The callback module's `c:context_changed/2` will be called allowing it to forward
  any changes to interest party.
  """

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


  @spec attach(context :: This.t, sess :: Sess.t) :: context :: This.t
  @doc """
  Sets the context's attached session.

  The given data must implements the protocol `Convex.Sess`.

  If the context is already attached to a session this function will raise
  an exception.

  The callback module's `c:context_changed/2` will be called allowing it to forward
  any changes to interest party.
  """

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



  @spec restore(context :: This.t, auth :: Auth.t,
                sess :: Sess.t, policy :: Policy.t)
    :: context :: This.t
  @doc """
  Sets the context's authentication. attached session and policy.

  Same effect as calling both `authenticate/3` and `attach/2`.
  """

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


  @spec update_policy(context :: This.t, policy :: Policy.t) :: context :: This.t
  @doc """
  Updates the context policy.

  The policy must implements protocol `Convex.Policy`.

  If the context is not authenticated, this call as no effect.

  The policy can be called multiple times and every times the callback module
  function `c:policy_changed/2` will be called allowing it to forward
  any changes to interest party.
  """

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

  @spec fetch_assigned(context :: This.t, key :: any)
    :: :error | {:ok, value :: any}
  @doc """
  Fetches an assigned value.
  """

  def fetch_assigned(%Ctx{assigns: assigns}, key) do
    Map.fetch(assigns, key)
  end


  @spec get_assigned(context :: This.t, key :: any) :: any
  @spec get_assigned(context :: This.t, key :: any, default :: any) :: any
  @doc """
  Gets an assigned value. If the key has not be assigned it returns `nil`
  or the given default value.
  """

  def get_assigned(%Ctx{assigns: assigns}, key, default \\ nil) do
    Map.get(assigns, key, default)
  end


  @spec assign(context :: This.t, Keyword.t) :: context :: This.t
  @doc """
  Assigns multiple key-value pairs to the context.
  """

  def assign(ctx, kw \\ []) do
    assigns = Enum.reduce kw, ctx.assigns, fn {k, v}, map ->
      Map.put(map, k, v)
    end
    %Ctx{ctx | assigns: assigns}
  end


  @spec assign(context :: This.t, key :: any, value :: any) :: context :: This.t
  @doc """
  Assigns a key-value pair to the context.
  """

  def assign(ctx, key, value) do
    %Ctx{ctx | assigns: Map.put(ctx.assigns, key, value)}
  end


  @spec assign_new(context :: This.t, Keyword.t) :: context :: This.t
  @doc """
  Assigns multiple key-value pairs to the context if the key is not yet assigned.
  """

  def assign_new(ctx, map) do
    assigns = Enum.reduce map, ctx.assigns, fn {k, v}, acc ->
      Map.put_new(acc, k, v)
    end
    %Ctx{ctx | assigns: assigns}
  end


  @spec add_tags(context :: This.t, tags :: MapSet.t | list) :: context :: This.t
  @doc """
  Adds some tags to the context.
  """

  def add_tags(ctx, tags) do
    %Ctx{ctx | tags: MapSet.union(ctx.tags, MapSet.new(tags))}
  end


  @spec del_tags(context :: This.t, tags :: MapSet.t | list) :: context :: This.t
  @doc """
  Removes some tags from the context.
  """

  def del_tags(ctx, tags) do
    %Ctx{ctx | tags: MapSet.difference(ctx.tags, MapSet.new(tags))}
  end


  #---------------------------------------------------------------------------
  # Logging Functions
  #---------------------------------------------------------------------------

  @spec error(context :: This.t, message :: String.t) :: context :: This.t
  @doc """
  Logs an error message with extra context information.
  """

  def error(ctx, message) do
    Logger.error("#{_log_prefix(ctx)}#{message}")
    ctx
  end


  @spec warn(context :: This.t, message :: String.t) :: context :: This.t
  @doc """
  Logs a warning message with extra context information.
  """

  def warn(ctx, message) do
    Logger.warn("#{_log_prefix(ctx)}#{message}")
    ctx
  end


  @spec info(context :: This.t, message :: String.t) :: context :: This.t
  @doc """
  Logs an information message with extra context information.
  """

  def info(ctx, message) do
    Logger.info("#{_log_prefix(ctx)}#{message}")
    ctx
  end


  @spec debug(context :: This.t, message :: String.t) :: context :: This.t
  @doc """
  Logs a debug message with extra context information.
  """

  def debug(ctx, message) do
    Logger.debug("#{_log_prefix(ctx)}#{message}")
    ctx
  end


  @spec trace(context :: This.t, message :: String.t, error :: any, trace :: any)
    :: context :: This.t
  @spec trace(context :: This.t, message :: String.t, error :: any,
              trace :: any, extra :: Keyword.t)
    :: context :: This.t
  @doc """
  Logs a a error message with the given message and information extracted
  from the given data.

  It will try to extract a stack trace from the error or the given stack.
  Extra key-value pairs will be logged too.
  """

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

  @spec fetch(Ctx.t, atom) :: :error | {:ok, any}
  @doc false

  def fetch(%Ctx{auth: auth}, :auth), do: {:ok, auth}

  def fetch(%Ctx{sess: sess}, :sess), do: {:ok, sess}

  def fetch(%Ctx{policy: policy}, :policy), do: {:ok, policy}

  def fetch(%Ctx{}, _key), do: :error


  @spec get(Ctx.t, atom, any) :: any
  @doc false

  def get(%Ctx{auth: auth}, :auth, _default), do: auth

  def get(%Ctx{sess: sess}, :sess, _default), do: sess

  def get(%Ctx{policy: policy}, :policy, _default), do: policy

  def get(%Ctx{}, _key, default), do: default


  @spec get_and_update(Ctx.t, atom, ((any) -> {any, any} | :pop)) :: {any, Ctx.t}
  @doc false

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


  @spec pop(Ctx.t, any) :: {any, Ctx.t}
  @doc false

  def pop(%Ctx{auth: auth} = ctx, :auth), do: {auth, %Ctx{ctx | auth: nil}}

  def pop(%Ctx{sess: sess} = ctx, :sess), do: {sess, %Ctx{ctx | sess: nil}}

  def pop(%Ctx{policy: policy} = ctx, :policy), do: {policy, %Ctx{ctx | policy: nil}}

  def pop(%Ctx{} = ctx, _key), do: {nil, ctx}


  #---------------------------------------------------------------------------
  # Protected Functions
  #---------------------------------------------------------------------------

  @spec _internal_failure(Ctx.t, term, nil | term) :: Ctx.t
  @doc false

  # Only used by Convex.Context.Guards in case an invalid context goes
  # out of the protected block  in order to try informing the recipient
  # of the system internal error.
  # DO NOT USE IF YOU DON'T KNOW WHAT YOU ARE DOING.

  def _internal_failure(%Ctx{mod: mod, sub: sub} = ctx, reason, debug \\ nil) do
    opname = _curr_op_name(ctx)
    {sub2, reason2} = mod.operation_failed(opname, reason, debug, ctx, sub)
    sub3 = mod.pipeline_failed(reason2, ctx, sub2)
    cvx_trace("ERROR", ctx, [], reason2)
    # We don't touch much the context in case it helps for debugging
    %Ctx{ctx | state: :failed, result: reason2, sub: sub3}
  end


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
    cvx_trace("FORK", ctx, [])
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
        cvx_trace("DONE", ctx2, [], result3)
        %Ctx{ctx2 | result: result3, sub: sub2}
          |> _perform_next()
    end
  end


  defp _operation_failed(%Ctx{mod: mod, sub: sub} = ctx, reason, debug) do
    # Handle the error regardless of being forked or not
    opname = _curr_op_name(ctx)
    {sub2, reason2} = mod.operation_failed(opname, reason, debug, ctx, sub)
    sub3 = mod.pipeline_failed(reason2, ctx, sub2)
    cvx_trace("FAILED", ctx, [], reason2)
    %Ctx{ctx | state: :failed, pipe: nil, store: nil, result: reason2, sub: sub3}
  end


  defp _delegate_failed(%Ctx{mod: mod, sub: sub} = ctx, reason, debug) do
    opname = _curr_op_name(ctx)
    {sub2, reason2} = mod.operation_failed(opname, reason, debug, ctx, sub)
    sub3 = mod.pipeline_failed(reason2, ctx, sub2)
    cvx_trace("DELEGATE.FAILED", ctx, [], reason2)
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
    cvx_trace("DELEGATE.DONE", ctx, [], pid)
    %Ctx{ctx | state: :delegated, pipe: nil, store: nil,
               result: nil, delegated: pid, sub: sub2}
  end


  defp _forked_delegate_done(ctx, pid) do
    # The results will be handled by the parent context's join call
    cvx_trace("DELEGATE.DONE", ctx, [], pid)
    %Ctx{ctx | state: :delegated, pipe: nil, store: nil,
               result: nil, delegated: pid}
  end


  defp _straight_join(%Ctx{mod: mod, sub: sub} = ctx, forked_contexts) do
    case _reduce_forked(:forked, [], [], [], forked_contexts) do
      {:failed, reason, _, subs} ->
        new_sub = mod.pipeline_join(subs, ctx, sub)
        # Failure already handled by the forked context
        cvx_trace("JOIN", ctx, [], length(forked_contexts))
        %Ctx{ctx | state: :failed, result: reason, delegated: nil, sub: new_sub}
      {state, results, delegated, subs} ->
        sub2 = mod.pipeline_join(subs, ctx, sub)
        sub3 = mod.pipeline_forked(results, delegated, ctx, sub2)
        cvx_trace("JOIN", ctx, [], length(forked_contexts))
        cvx_trace("FORKED", ctx, [], {results, delegated})
        %Ctx{ctx | state: state, result: results, delegated: delegated, sub: sub3}
    end
  end


  defp _forked_join(%Ctx{mod: mod, sub: sub} = ctx, forked_contexts) do
    # The results will be handled by the parent context's join call
    {state, result, delegated, subs} =
      _reduce_forked(:forked, [], [], [], forked_contexts)
    new_sub = mod.pipeline_join(subs, ctx, sub)
    cvx_trace("JOIN", ctx, [], length(forked_contexts))
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
    cvx_trace("FINISHED", ctx, [], result)
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
        cvx_trace("PERFORM", ctx2, args)
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
