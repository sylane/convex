defmodule Convex.Handler.Process do

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


  def context(%This{ctx: ctx}), do: ctx


  def authenticated?(this) do
    Ctx.authenticated?(this.ctx) and Ctx.attached?(this.ctx)
  end


  def async(%This{pid: pid} = this, opts \\ []) do
    binder_fun = fn _ctx -> Proxy.new(pid, @static_ref) end
    Async.recast(this.ctx, Keyword.merge(opts, binder: binder_fun))
  end


  def sync(%This{pid: pid} = this, opts \\ []) do
    binder_fun = fn _ctx -> Proxy.new(pid, @static_ref) end
    Sync.recast(this.ctx, Keyword.merge(opts, binder: binder_fun))
  end


  def prepare(%This{} = this, reqref, params, reqopts \\ []) do
    %This{pid: pid, opts: opts, ctx: ctx, states: states, timeout: timeout} = this
    ref = :erlang.make_ref()
    tref = schedule_timeout(nil, pid, ref, Keyword.get(reqopts, :timeout, timeout))
    data = {tref, nil, params, Protocol.new()}
    this = %This{this | states: Map.put(states, ref, data)}
    {this, Context.clone(ctx, pid, reqref, ref, opts)}
  end


  def is_linked?(pid, this) do
    pid in Map.values(this.proxies)
  end


  def update(ctx, this), do: update_context(ctx, this)


  def reset(%This{ctx: ctx} = this), do: %This{this | ctx: Ctx.reset(ctx)}


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


  def make_post_fun(this) do
    myself = this.pid
    fn msg -> send(myself, {This, :post, {:event, msg}}) end
  end


  def pid(this), do: this.pid


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
