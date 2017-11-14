defmodule Convex.Handler.Process do

  @moduledoc """
  Helper module to handle the messages sent by `Convex.Context.Process` and
  `Convex.Proxy.Process`.

  This is used to monitor the state of multiple pipelines without blocking.

  #### Basic usage

  ```Elixir
  handler = Convex.Handler.Process.new()
  # Synchronous pipeline
  ctx = Convex.Handler.Process.sync(handler)
  result = perform with: ctx do
    do.something.synchronous ^args
  end
  # Asynchronous pipeline
  ctx = Convex.Handler.Process.async(handler)
  perform with: ctx do
    do.something.asynchronous ^args
  end
  # Monitored pipeline
  track_ref = make_ref()
  {handler, ctx} = Convex.Handler.Process.prepare(handler, track_refm, [])
  perform with: ctx do
    do.something.monitored ^args
  end
  ```

  Then to handle the monitored pipelines all received messages must be forwarded
  to the handler:

  ```Elixir
  def handle_info(msg, %This{handler: handler} = this) do
    case Convex.Handler.Process.handle_info(msg, handler) do
      {:ignored, handler} ->
        # Not a message for the handler, do whatever you want with it
        {:noreply, %This{this | handler: handler}}
      {:ok, handler} ->
        # Handled but no result yet
        {:noreply, %This{this | handler: handler}}
      {:send, traking_ref, messages, handler} ->
        # Message sent through a proxy, with the associated tracking ref if available
        {:noreply, %This{this | handler: handler}}
      {:shutdown, reason, handler} ->
        # A service requested the peer service to be closed
        {:shutdown, reason, %This{this | handler: handler}}
      {:done, params, result, handler} ->
        # A pipeline finished successfully
        {:noreply, %This{this | handler: handler}}
      {:failed, params, reason, handler} ->
        # A pipeline failed
        {:noreply, %This{this | handler: handler}}
    end
  end
  ```

  It provides functions to create contexts (either synchronous, asynchronous or
  monitored) that can be used to perform the operation pipeline. Then the handler
  process must pass any received messages through the `handle_info/2` function
  to handle any pipeline status updates, or if a proxy was used to post a
  message.

  This module handles operations authenticating, attaching a session or
  updating the policy. It does this by having the context or proxy send a
  message updating the base (reference) context so the next one given by
  `sync/1`, `sync/2`, `async/1`, `async/2`, `prepare/3` or `prepare/4`
  will contain the updated authentication, session and policy data.

  To simulate a message being sent by a proxy, so it can be handled
  via the `{:send, _, _, _}` return value of `handle_info/2`,
  it is possible to create a posting function by calling `make_post_fun/1`.
  The returned function can then be called to post messages that will
  trigger `handle_info/2` in the same way:

    ```Elixir
      f = Convex.Handler.Process.make_post_fun(handler)
      f.({:my_message, any_data})
    ```
  """

  #===========================================================================
  # Includes
  #===========================================================================

  alias __MODULE__, as: This
  alias Convex.Context.Process, as: Context
  alias Convex.Context.Sync
  alias Convex.Context.Async
  alias Convex.Proxy.Process, as: Proxy
  alias Convex.Context, as: Ctx
  alias Convex.Protocol


  #===========================================================================
  # Attributes
  #===========================================================================

  @default_timeout 5000
  @default_monitoring_delay 150
  @static_ref :static_context_ref


  #===========================================================================
  # Types
  #===========================================================================

  @type t :: %This{
    pid: pid,
    opts: Keyword.t,
    ctx: Ctx.t,
    delay: integer,
    timeout: integer,
    states: %{reference => {reference, reference, term, Protocol.t}},
    proxies: %{reference => term}
  }

  defstruct [
    pid: nil,
    opts: nil,
    ctx: nil,
    delay: nil,
    timeout: nil,
    states: %{},
    proxies: %{}
  ]


  #===========================================================================
  # API Functions
  #===========================================================================

  @spec new() :: handler :: This.t
  @spec new(options :: Keyword.t) :: handler :: This.t
  @doc """
  Creates a handler for tracking pipelines using contexts with
  `Convex.Context.Process` callback modules.

  Options:
    - `pid`: The pid on behalf the handler is monitoring, by default
        it is the calling process PID.
    - `assigns`: The assigned values to be merged in the contexts provided by
      `sync/1`, `sync/2`, `async/1`, `async/2`, `prepare/3` and `prepare/4`.
    - `tags`: The tags to be merged in the contexts provided by
      `sync/1`, `sync/2`, `async/1`, `async/2`, `prepare/3` and `prepare/4`.
    - `timeout`: The time in milliseconds after which a pipeline performed
      with a context returned by `prepare/3` or `prepare/4` will timeout.
      The default timeout is `5000` milliseconds.
    - `monitoring_delay`: The time after which the delegated process will
      start bing monitored wen using the contexts returned by
      `prepare/3` or `prepare/4`. Default value: `150` milliseconds.
  """

  def new(opts \\ []) do
    mgropts_keys = [:pid, :assigns, :tags, :timeout, :monitoring_delay]
    {mgropts, ctxopts} = Keyword.split(opts, mgropts_keys)
    pid = Keyword.get(mgropts, :pid, self())
    timeout = Keyword.get(mgropts, :timeout, @default_timeout)
    delay = Keyword.get(mgropts, :monitoring_delay, @default_monitoring_delay)
    assigns = Keyword.get(mgropts, :assigns, []) |> Enum.into(%{})
    tags = Keyword.get(mgropts, :tags, [])
    ctx = Context.new(pid, nil, nil, ctxopts)
      |> Ctx.compact()
      |> Ctx.add_tags(tags)
      |> Ctx.assign(assigns)
    %This{pid: pid, opts: ctxopts, ctx: ctx, delay: delay, timeout: timeout}
  end


  @spec context(handler :: This.t) :: context :: Ctx.t
  @doc """
  Returns the reference context.

  To perform a pipeline, the context returned from
  `sync/1`, `sync/2`, `async/1`, `async/2`, `prepare/3` and `prepare/4`
  should be used.
  """

  def context(%This{ctx: ctx}), do: ctx


  @spec authenticated?(handler :: This.t) :: boolean
  @doc """
  Tells if the current context is authenticated and attached.
  """

  def authenticated?(this) do
    Ctx.authenticated?(this.ctx) and Ctx.attached?(this.ctx)
  end


  @spec is_linked?(pid :: pid, handler :: This.t) :: boolean
  @doc """
  Tells if the given process is bound (If it holds a proxy).
  """

  def is_linked?(pid, this) do
    pid in Map.values(this.proxies)
  end


  @spec pid(handler :: This.t) :: pid
  @doc """
  Returns the pid the handler is monitoring for.
  It is usually `self()`.
  """

  def pid(this), do: this.pid


  @spec async(handler :: This.t) :: context :: Ctx.t
  @spec async(handler :: This.t, options :: Keyword.t) :: context :: Ctx.t
  @doc """
  Returns an asynchronous context with support for binding.
  See `Convex.Context.Async.recast/2` for options documentation.
  """

  def async(%This{pid: pid} = this, opts \\ []) do
    binder_fun = fn _ctx -> Proxy.new(pid, @static_ref) end
    Async.recast(this.ctx, Keyword.merge(opts, binder: binder_fun))
  end


  @spec sync(handler :: This.t) :: context :: Ctx.t
  @spec sync(handler :: This.t, options :: Keyword.t) :: context :: Ctx.t
  @doc """
  Returns a synchronous context with support for binding.
  See `Convex.Context.Sync.recast/2` for options documentation.
  """

  def sync(%This{pid: pid} = this, opts \\ []) do
    binder_fun = fn _ctx -> Proxy.new(pid, @static_ref) end
    Sync.recast(this.ctx, Keyword.merge(opts, binder: binder_fun))
  end


  @spec prepare(handler :: This.t, reqref :: any, params :: any)
    :: {handler :: This.t, context :: Ctx.t}
  @spec prepare(handler :: This.t, reqref :: any, params :: any, options :: Keyword.t)
    :: {handler :: This.t, context :: Ctx.t}
  @doc """
  Prepares a context for monitoring.

  The returned context can be used to perform a non-blocking operation pipeline
  and monitor its execution by forwarding all the received message through
  function `handle_info/2`.

  The `reqref` parameter is used to track the message sent by proxies.
  If an operation handler use a proxy to post a message passing the context
  as parameter, `handle_info/2` result will contain the `reqref`.

  The specified parameters is an opaque data that will be returned by
  `handle_info/2` when the pipeline is done or fail.

  **NOTE** that the timeout starts from the call to this function, not from
  the time the pipeline is performing, so the context must be used right away,
  not stored for later use.

  For options documentation, see `Convex.Context.new/2`.

  Extra options:

    - `timeout`: overrides the handler timeout only for the returned context.
  """

  def prepare(%This{} = this, reqref, params, reqopts \\ []) do
    %This{pid: pid, opts: opts, ctx: ctx, states: states, timeout: timeout} = this
    ref = :erlang.make_ref()
    tref = schedule_timeout(nil, pid, ref, Keyword.get(reqopts, :timeout, timeout))
    data = {tref, nil, params, Protocol.new()}
    this = %This{this | states: Map.put(states, ref, data)}
    {this, Context.clone(ctx, pid, reqref, ref, opts)}
  end


  @spec update(context :: Ctx.t, handler :: This.t) :: handler :: This.t
  @doc """
  Updates the base context.

  The given context will be merged into the base context,
  see `Convex.Context.merge/2` for more information.
  """

  def update(ctx, this), do: update_context(ctx, this)


  @spec reset(handler :: This.t) :: handler :: This.t
  @doc """
  Resets the reference context, removing authentication and session information.
  """

  def reset(%This{ctx: ctx} = this), do: %This{this | ctx: Ctx.reset(ctx)}


  @spec handle_info(message :: any, handler :: This.t)
    :: {:ok, handler :: This.t}
     | {:ignored, handler :: This.t}
     | {:done, params :: any, result :: any, handler :: This.t}
     | {:failed, params :: any, reason :: any, handler :: This.t}
     | {:send, reqref :: any, msg :: any, handler :: This.t}
     | {:shutdown, reason :: any, handler :: This.t}
  @doc """
  Handles messages sent by `Convex.Context.Process` and `Convex.Proxy.Process`.

  Used to monitor messages sent by proxies and operation pipelines performed
  using the contexts returned by `prepare/3` and `prepare/4`.

  Possible return values :

    - `{:ok, new_handler}`:

      The message was for the handler but there is no pipeline result or failure
      yet available.

    - `{:ignored, new_handler}`:

      The message is not for the handler, the caller is free to do whatever
      it wants with it.

    - `{:done, params, result, new_handler}`:

      A pipeline finished successfully with the returned `result`.
      The returned `params` is the exact same given to `prepare/3`
      or `prepare/4` as parameter.

    - `{:failed, params, reason, new_handler}`:

      A pipeline failed with returned `reason`.
      The returned `params` is the exact same given to `prepare/3`
      or `prepare/4` as parameter.

    - `{:send, reqref, message, new_handler}`:

      A proxy (or a function returned by `make_post_fun/1`) posted a message.
      If a context was given to `Convex.Proxy.post/3` and it contained a
      tracking reference it will be there as the `reqref` value, otherwise
      `reqref` will be `nil`.

    - `{:shutdown, reason, new_handler}`:

      A service requested a shutdown through its proxy.
      The caller is free to shutdown or not.
  """

  def handle_info({Context, ref, from, depth, :failed, reason}, this) do
    case Map.fetch(this.states, ref) do
      :error -> {:ok, this}
      {:ok, {_tref, _dref, _params, proto} = data} ->
        res = Protocol.handle_failed(from, depth, reason, proto)
        handle_proto_response(res, ref, data, this)
    end
  end

  def handle_info({Context, ref, from, depth, :forked, {results, delegated}}, this) do
    case Map.fetch(this.states, ref) do
      :error -> {:ok, this}
      {:ok, {_tref, _dref, _params, proto} = data} ->
        res = Protocol.handle_forked(from, depth, results, delegated, proto)
        handle_proto_response(res, ref, data, this)
    end
  end

  def handle_info({Context, ref, from, depth, :done, result}, this) do
    case Map.fetch(this.states, ref) do
      :error -> {:ok, this}
      {:ok, {_tref, _dref, _params, proto} = data} ->
        res = Protocol.handle_done(from, depth, result, proto)
        handle_proto_response(res, ref, data, this)
    end
  end

  def handle_info({Context, ref, from, depth, :delegated, pid}, this) do
    case Map.fetch(this.states, ref) do
      :error -> {:ok, this}
      {:ok, {_tref, _dref, _params, proto} = data} ->
        res = Protocol.handle_delegated(from, depth, pid, proto)
        handle_proto_response(res, ref, data, this)
    end
  end

  def handle_info({Context, ref, _from, _depth, :context_changed, ctx}, this) do
    case Map.fetch(this.states, ref) do
      :error -> {:ok, this}
      {:ok, _data} -> {:ok, update_context(ctx, this)}
    end
  end

  def handle_info({Context, ref, _from, _depth, :policy_changed, policy}, this) do
    case Map.fetch(this.states, ref) do
      :error -> {:ok, this}
      {:ok, _data} -> {:ok, update_policy(policy, this)}
    end
  end

  def handle_info({This, :post, msg}, this) do
    {:send, nil, msg, this}
  end

  def handle_info({Proxy, proxy_ref, reqref, :post, msg}, this) do
    case Map.fetch(this.proxies, proxy_ref) do
      :error -> {:ok, this}
      {:ok, _data} ->
        {:send, reqref, msg, this}
    end
  end

  def handle_info({Proxy, ref, :bind, @static_ref, pid}, this) do
    {:ok, %This{this | proxies: Map.put(this.proxies, ref, pid)}}
  end

  def handle_info({Proxy, ref, :bind, ctx_ref, pid}, this) do
    case Map.fetch(this.states, ctx_ref) do
      :error -> {:ok, this}
      {:ok, _data} ->
        {:ok, %This{this | proxies: Map.put(this.proxies, ref, pid)}}
    end
  end

  def handle_info({Proxy, ref, :unbound}, this) do
    case Map.fetch(this.proxies, ref) do
      :error -> {:ok, this}
      {:ok, _data} ->
        {:ok, %This{this | proxies: Map.delete(this.proxies, ref)}}
    end
  end

  def handle_info({Proxy, ref, :close}, this) do
    case Map.fetch(this.proxies, ref) do
      :error -> {:ok, this}
      {:ok, _data} ->
        {:shutdown, :closed, %This{this | proxies: Map.delete(this.proxies, ref)}}
    end
  end

  def handle_info({Proxy, ref, :policy_changed, policy}, this) do
    case Map.fetch(this.proxies, ref) do
      :error -> {:ok, this}
      {:ok, _data} -> {:ok, update_policy(policy, this)}
    end
  end

  def handle_info({:DOWN, mon_ref, :process, from, reason}, this) do
    delegate_down(from, mon_ref, reason, Map.to_list(this.states), this)
  end

  def handle_info({This, ref, :monitor}, %This{states: states} = this) do
    case Map.fetch(states, ref) do
      :error -> {:ok, this}
      {:ok, {tref, _dref, params, proto}} ->
        proto2 = Protocol.start_monitoring(proto)
        data = {tref, nil, params, proto2}
        {:ok, %This{this | states: Map.put(states, ref, data)}}
    end
  end

  def handle_info({This, ref, :timeout}, %This{states: states} = this) do
    case Map.fetch(states, ref) do
      :error -> {:ok, this}
      {:ok, {_tref, dref, params, _state}} ->
        cancel_timer(dref)
        this2 = %This{this | states: Map.delete(states, ref)}
        {:failed, params, :timeout, this2}
    end
  end

  def handle_info(_msg, this) do
    {:ignored, this}
  end


  @spec make_post_fun(handler :: This.t) :: ((message :: any) -> :ok)
  @doc """
  Returns a function that can be used to send a message as a proxy would do.

  After calling the returned function, the process will receive a message
  that when given to `handle_info/2` will return
  `{:send, nil, message, handler}`.
  """

  def make_post_fun(this) do
    myself = this.pid
    fn msg -> send(myself, {This, :post, {:event, msg}}) end
  end


  #===========================================================================
  # Internal Functions
  #===========================================================================

  defp update_context(%Ctx{} = ctx, this) do
    %This{this | ctx: Ctx.merge(this.ctx, ctx)}
  end


  defp update_policy(new_policy, this) do
    update_context(%Ctx{this.ctx | policy: new_policy}, this)
  end


  defp delegate_down(_from, _mon_ref, _reason, [], this) do
    {:ignored, this}
  end

  defp delegate_down(from, mon_ref, reason, [{ref, data} | rem], this) do
    {tref, dref, params, proto} = data
    case Protocol.handle_down(from, mon_ref, reason, proto) do
      {:ignored, proto2} ->
        data2 = {tref, dref, params, proto2}
        this2 = %This{this | states: Map.put(this.states, ref, data2)}
        delegate_down(from, mon_ref, reason, rem, this2)
      response -> handle_proto_response(response, ref, data, this)
    end
  end

  defp handle_proto_response(response, ref, {tref, dref, params, _}, this) do
    %This{pid: pid, delay: delay, states: states} = this
    case response do
      {:continue, proto} ->
        dref2 = schedule_monitoring(dref, pid, ref, delay)
        data = {tref, dref2, params, proto}
        {:ok, %This{this | states: Map.put(states, ref, data)}}
      {:done, result} ->
        cancel_timer(tref)
        cancel_timer(dref)
        {:done, params, result, %This{this | states: Map.delete(states, ref)}}
      {:failed, reason} ->
        cancel_timer(tref)
        cancel_timer(dref)
        {:failed, params, reason, %This{this | states: Map.delete(states, ref)}}
    end
  end


  defp cancel_timer(nil), do: :ok

  defp cancel_timer(ref), do: :erlang.cancel_timer(ref)


  defp update_timer(nil, pid, msg, delay) do
    Process.send_after(pid, msg, delay)
  end

  defp update_timer(ref, pid, msg, delay) do
    :erlang.cancel_timer(ref)
    Process.send_after(pid, msg, delay)
  end


  defp schedule_monitoring(dref, pid, ref, delay) do
    update_timer(dref, pid, {This, ref, :monitor}, delay)
  end


  defp schedule_timeout(tref, pid, ref, delay) do
    update_timer(tref, pid, {This, ref, :timeout}, delay)
  end

end
