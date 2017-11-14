defmodule Convex.Context.Sync do

  @moduledoc """
  Callback module for `Convex.Context` providing synchronous execution
  behaviour.

  When used with an operation pipeline, performing the pipeline will block
  until all operations are done or any of them fail.

  When using `Convex.Pipeline.perform` it will return a tuple `{:ok, result}`
  or `{:error, reason}`, and when using `Convex.Pipeline.perform!` it will
  return the result directly or raise an error.

  #### Usage

  ```Elixir
  {:ok, result} = perform Convex.Context.Sync.new() do
    some.operation ^args
  end

  result = perform! Convex.Context.Sync.new() do
    some.operation ^args
  end
  ```

  If you need to do something synchronously from inside an operation handler
  and don't want to lose the authentication/session/policy/assigned values and
  tags you can recast the current context:

  ```Elixir
  perform Convex.Context.Sync.recast(current_context) do
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
  # Attributes
  #===========================================================================

  @default_timeout 5000
  @default_monitoring_delay 150
  @opt_keys [:timeout, :monitoring_delay]


  #===========================================================================
  # Types
  #===========================================================================

  @type t :: %This{
    pid:    pid,
    ref:    reference,
    binder: ((Ctx.t) -> term),
    opts:   Keyword.t,
  }

  defstruct [
    :pid,
    :ref,
    :binder,
    :opts,
  ]


  #===========================================================================
  # API Functions
  #===========================================================================

  @spec new() :: context :: Ctx.t
  @spec new(options :: Keyword.t) :: context :: Ctx.t
  @doc """
  Creates a new synchronous context.

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
  Creates an synchronous context out of another context.

  Keeps the authentication, session, policy, assigned values and tags form
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
    ctx_opts = Keyword.take(opts, @opt_keys)
    {:ok, %This{binder: binder, opts: ctx_opts}}
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
  def pipeline_fork(%Ctx{depth: 0}, %This{pid: nil} = this) do
    # The pipeline is forked for the first time.
    # Because multiple forks could get delegate we need to prepare right away
    # to be sure all of them use the same reference.
    this2 = prepare(this)
    {this2, this2}
  end

  def pipeline_fork(_ctx, this), do: {this, this}


  @doc false
  def pipeline_join(_forked_subs, _ctx, this), do: this


  @doc false
  def pipeline_delegate(%Ctx{depth: 0}, %This{pid: nil} = this) do
    # The context is being delegated to another process for the first time.
    this2 = prepare(this)
    {this2, this2}
  end

  def pipeline_delegate(_ctx, this), do: {this, this}


  @doc false
  def pipeline_failed(_reason, %Ctx{depth: 0}, this), do: this

  def pipeline_failed(reason, ctx, %This{pid: recip, ref: ref} = this) do
    msg = {This, ref, self(), ctx.depth, :failed, reason}
    send(recip, msg)
    this
  end


  @doc false
  def pipeline_forked(_results, _delegated, %Ctx{depth: 0}, this), do: this

  def pipeline_forked(results, delegated, ctx, this) do
    # The pipelines generated some results and delegated to some processes.
    %This{pid: recip, ref: ref} = this
    msg = {This, ref, self(), ctx.depth, :forked, {results, delegated}}
    send(recip, msg)
    this
  end


  @doc false
  def pipeline_done(_result, %Ctx{depth: 0}, this), do: this

  def pipeline_done(result, ctx, %This{pid: recip, ref: ref} = this) do
    msg = {This, ref, self(), ctx.depth, :done, result}
    send(recip, msg)
    this
  end


  @doc false
  def pipeline_delegated(_pid, %Ctx{depth: 0}, this), do: this

  def pipeline_delegated(pid, ctx, %This{pid: recip, ref: ref} = this) do
    msg = {This, ref, self(), ctx.depth, :delegated, pid}
    send(recip, msg)
    this
  end


  @doc false
  def context_changed(_ctx, this), do: this


  @doc false
  def policy_changed(_ctx, this), do: this


  @doc false
  def pipeline_performed(_opts, %Ctx{state: :done, result: result}, _this) do
    cvx_trace(">>>>>>>>>>", :finished, [0, 0, 0], [:sync, :done], [result: result])
    {:ok, result}
  end

  def pipeline_performed(_opts, %Ctx{state: :failed, result: reason}, _this) do
    cvx_trace(">>>>>>>>>>", :finished, [0, 0, 0], [:sync, :failed], [reason: reason])
    {:error, reason}
  end

  def pipeline_performed(_opts, %Ctx{state: :forked, delegated: []} = ctx, _this) do
    cvx_trace(">>>>>>>>>>", :finished, [0, 0, 0], [:sync, :forked], [results: ctx.result, delegated: []])
    {:ok, ctx.result}
  end

  def pipeline_performed(opts, %Ctx{state: :delegated} = ctx, this) do
    %This{pid: recip, ref: ref} = this
    assert_self(recip)
    wait(ref, nil, [ctx.delegated], Keyword.merge(this.opts, opts))
  end

  def pipeline_performed(opts, %Ctx{state: :forked} = ctx, this) do
    %This{pid: recip, ref: ref} = this
    %Ctx{result: results, delegated: pids} = ctx
    assert_self(recip)
    wait(ref, [results], pids, Keyword.merge(this.opts, opts))
  end


  @doc false
  def pipeline_performed!(opts, ctx, this) do
    case pipeline_performed(opts, ctx, this) do
      {:ok, result} -> result
      {:error, reason} ->
        #FIXME: add more info about the failed operation.
        raise Convex.Error, reason: reason,
          message: "Error performing pipeline: #{inspect reason}"
    end
  end


  #===========================================================================
  # Internal Functions
  #===========================================================================

  defp prepare(this) do
    %This{this | pid: self(), ref: :erlang.make_ref()}
  end


  defp assert_self(pid) do
    if pid != self() do
      raise Convex.Error, reason: :internal_error,
        message: "Waiting for synchronous context from process #{inspect self()} when the result will be sent to process #{inspect pid}"
    end
  end


  defp wait(ref, results, delegated, opts) do
    timeout = Keyword.get(opts, :timeout, @default_timeout)
    mdelay = Keyword.get(opts, :monitoring_delay, @default_monitoring_delay)
    # Schedule timeout timer.
    timeout_msg = {This, ref, nil, nil, :timeout, nil}
    timeout_ref = Process.send_after(self(), timeout_msg, timeout)
    # Wait for the result.
    cmsg = length(delegated)
    tmsg = cmsg
    lvl = 1
    cpids = pending_new(delegated)
    monitored = monitored_new()
    cvx_trace(">>>>>>>>>>", :waiting, [lvl, cmsg, cmsg], [:sync, :start], [results: results, delegated: delegated])
    {result, monitored, requeue} =
      wait(ref, lvl, cmsg, tmsg, cpids, [], mdelay, results, monitored, [])
    # Cancel timeout timer
    :erlang.cancel_timer(timeout_ref)
    # Demonitor all monitored processes.
    demonitor(monitored)
    # Requeue monitoring messages not mean for us.
    Enum.map(requeue, &send(self(), &1))
    result
  end


  # Parameters:
  #   - `ref`: The pipeline reference used to identify the messages.
  #   - `lvl`: The current level in the tree of processes.
  #   - `cmsg`: The number of messages we are waiting for at current level;
  #     when it reaches `0` we advance to the next level.
  #   - `tmsg`: The number of messages we are waiting for globaly;
  #     when it reaches `0` we received all the response.
  #   - `cpids`: The reference-counted pids of the processes we are waiting
  #     a response from for the current level; if no message is received after
  #     a delay we will start monitoring them.
  #   - `npids`: The pid of the processes we will be waiting a response from
  #     during the next level; it will become `cpids` when advancing level.
  #   - `mdelay`: The delay after which we will start monitoring current level
  #     processes.
  #   - `acc`: The list of all result lists when streaming, `nil` otherwise.
  #   - `monitored`: The reference counted monitored processes.
  #   - `requeued`: The Monitoring messages received that weren't for us.
  defp wait(ref, lvl, cmsg, tmsg, cpids, npids, mdelay, acc, monitored, requeued) do
    receive do
      {This, ^ref, from, ^lvl, :done, result} ->
        {cpids, monitored} = demonitor(cpids, monitored, from)
        case handle_response(lvl, cmsg, tmsg, cpids, npids, []) do
          :done ->
            cvx_trace(">>>>>>>>>>", :finished, [lvl, 0, 0], [:sync, :done], [result: result])
            {{:ok, return_result(acc, result)}, monitored, requeued}
          {:wait, lvl, cmsg, tmsg, cpids, npids} ->
            cvx_trace(">>>>>>>>>>", :waiting, [lvl, cmsg, tmsg], [:sync, :done], [result: result])
            wait(ref, lvl, cmsg, tmsg, cpids, npids, mdelay,
                 append_result(acc, result), monitored, requeued)
        end

      {This, ^ref, _from, ^lvl, :failed, reason} ->
        cvx_trace(">>>>>>>>>>", :finished, [lvl, cmsg, tmsg], [:sync, :failed], [reason: reason])
        # Error during pipeline processing.
        {{:error, reason}, monitored, requeued}

      {This, ^ref, _, _, :timeout, _} ->
        cvx_trace(">>>>>>>>>>", :finished, [lvl, cmsg, tmsg], [:sync, :timeout], [], :timeout)
        # Timeout while processing the pipeline.
        {{:error, :timeout}, monitored, requeued}

      {This, ^ref, from, ^lvl, :delegated, pid} ->
        {cpids, monitored} = demonitor(cpids, monitored, from)
        case handle_response(lvl, cmsg, tmsg, cpids, npids, [pid]) do
          :done ->
            cvx_trace(">>>>>>>>>>", :finished, [lvl, 0, 0], [:sync, :delegated], [pid: pid])
            {{:ok, return_result(acc)}, monitored, requeued}
          {:wait, lvl, cmsg, tmsg, cpids, npids} ->
            cvx_trace(">>>>>>>>>>", :waiting, [lvl, cmsg, tmsg], [:sync, :delegated], [pid: pid])
            wait(ref, lvl, cmsg, tmsg, cpids, npids, mdelay,
                 acc, monitored, requeued)
        end

      {This, ^ref, from, ^lvl, :forked, {results, delegated}} ->
        {cpids, monitored} = demonitor(cpids, monitored, from)
        case handle_response(lvl, cmsg, tmsg, cpids, npids, delegated) do
          :done ->
            cvx_trace(">>>>>>>>>>", :finished, [lvl, 0, 0], [:sync, :forked], [result: results, delegated: delegated])
            {{:ok, return_results(acc, results)}, monitored, requeued}
          {:wait, lvl, cmsg, tmsg, cpids, npids} ->
            cvx_trace(">>>>>>>>>>", :waiting, [lvl, cmsg, tmsg], [:sync, :forked], [result: results, delegated: delegated])
            wait(ref, lvl, cmsg, tmsg, cpids, npids, mdelay,
                 append_results(acc, results), monitored, requeued)
        end

      {This, other_ref, _, _, _, _} when other_ref != ref ->
        # Purges old message from the inbox and continue.
        wait(ref, lvl, cmsg, tmsg, cpids, npids, mdelay,
             acc, monitored, requeued)

      {:DOWN, mon_ref, :process, pid, _reason} = msg ->
        case monitored_pop_monref(monitored, pid, mon_ref) do
          {:ok, monitored, _} ->
            cvx_trace(">>>>>>>>>>", :finished, [lvl, cmsg, tmsg], [:sync, :down], [pid: pid], :noproc)
            # One of our monitored process just died.
            {{:error, :noproc}, monitored, requeued}
          :error ->
            # Not for us, keep it for sending back later,
            # we don't want the calling process to miss a monitoring message.
            wait(ref, lvl, cmsg, tmsg, cpids, npids, mdelay,
                 acc, monitored, [msg | requeued])
        end

    after
      mdelay ->
        cvx_trace("MONITORING", :waiting, [lvl, cmsg, tmsg], [:sync, :monitor], [pids: cpids])
        # No messages received, start monitoring current level pids.
        {cpids, monitored} = monitor(cpids, monitored)
        wait(ref, lvl, cmsg, tmsg, cpids, npids, mdelay,
             acc, monitored, requeued)
    end
  end


  defp handle_response(lvl, cmsg, tmsg, cpids, npids, delegated) do
    # Updates the pending messages counter for current level
    cmsg = cmsg - 1
    # Updates the total pending messages counter
    tmsg = tmsg - 1 + length(delegated)
    # Updates the next level process pid list
    npids = delegated ++ npids
    # handle the level progression
    handle_response(lvl, cmsg, tmsg, cpids, npids)
  end


  defp handle_response(_lvl, 0, 0, _cpids, _npids) do
    # Received the last response
    :done
  end

  defp handle_response(lvl, 0, tmsg, _cpids, npids) do
    # Received the last response for current level
    {:wait, lvl + 1, tmsg, tmsg, pending_new(npids), []}
  end

  defp handle_response(lvl, cmsg, tmsg, cpids, npids) do
    # Received one more response for current level
    {:wait, lvl, cmsg, tmsg, cpids, npids}
  end


  defp return_result(nil), do: nil

  defp return_result(acc), do: :lists.append(acc)


  defp append_result(nil, result), do: [[result]]

  defp append_result(acc, result), do: [[result] | acc]


  defp return_result(nil, result), do: result

  defp return_result(acc, result), do: :lists.append([[result] | acc])


  defp append_results(nil, results), do: [results]

  defp append_results(acc, results), do: [results | acc]


  defp return_results(nil, results), do: results

  defp return_results(acc, results), do: :lists.append([results | acc])


  defp monitor(pending, monitored) do
    {pending_new(), monitored_merge(monitored, pending)}
  end


  defp demonitor(pending, monitored, pid) do
    case monitored_del(monitored, pid) do
      {true, monitored} -> {pending, monitored}
      {false, monitored} ->
        {_, pending} = pending_del(pending, pid)
        {pending, monitored}
    end
  end


  defp demonitor(monitored) do
    monitored_reset(monitored)
  end


  defp pending_new(pids \\ []), do: pending_add(%{}, pids)


  defp pending_add(map, []), do: map

  defp pending_add(map, [pid | pids]) do
    map = case Map.fetch(map, pid) do
      :error -> Map.put(map, pid, 1)
      {:ok, countref} -> Map.put(map, pid, countref + 1)
    end
    pending_add(map, pids)
  end


  defp pending_del(map, pid) do
    case Map.fetch(map, pid) do
      :error -> {false, map}
      {:ok, 1} -> {true, Map.delete(map, pid)}
      {:ok, countref} -> {true, Map.put(map, pid, countref - 1)}
    end
  end


  defp monitored_new(pids \\ []), do: monitored_add(%{}, pids)


  defp monitored_add(map, []), do: map

  defp monitored_add(map, [pid | pids]) do
    map = case Map.fetch(map, pid) do
      :error ->
        Map.put(map, pid, {1, Process.monitor(pid)})
      {:ok, {countref, monref}} ->
        Map.put(map, pid, {countref + 1, monref})
    end
    monitored_add(map, pids)
  end


  defp monitored_merge(map, pending) do
    monitored_merge_items(map, Map.to_list(pending))
  end


  defp monitored_merge_items(map, []), do: map

  defp monitored_merge_items(map, [{pid, countref} | items]) do
    map = case Map.fetch(map, pid) do
      :error ->
        Map.put(map, pid, {countref, Process.monitor(pid)})
      {:ok, {countref2, monref}} ->
        Map.put(map, pid, {countref2 + countref, monref})
    end
    monitored_merge_items(map, items)
  end


  defp monitored_del(map, pid) do
    case Map.fetch(map, pid) do
      :error -> {false, map}
      {:ok, {1, monref}} ->
        Process.demonitor(monref, [:flush])
        {true, Map.delete(map, pid)}
      {:ok, {countref, monref}} ->
        {true, Map.put(map, pid, {countref - 1, monref})}
    end
  end


  defp monitored_pop_monref(map, pid, monref) do
    case Map.fetch(map, pid) do
      {:ok, {countref, ^monref}} -> {:ok, map, countref}
      _ -> :error
    end
  end


  defp monitored_reset(map) do
    for {_pid, {_, monref}} <- map do
      Process.demonitor(monref, [:flush])
    end
    monitored_new()
  end

end
