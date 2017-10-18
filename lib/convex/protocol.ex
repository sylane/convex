defmodule Convex.Protocol do

  #===========================================================================
  # Includes
  #===========================================================================

  alias __MODULE__, as: This


  #===========================================================================
  # Types
  #===========================================================================

  @type t :: %This{
    lvl: integer,
    state: {integer, integer, list},
    pend: map,
    acc: list,
    buffer: map,
    mon: map,
  }

  defstruct [
    lvl: nil,
    state: nil,
    acc: nil,
    buffer: %{},
    pend: nil,
    mon: nil,
  ]


  #===========================================================================
  # API Functions
  #===========================================================================

  def new(opts \\ []) do
    lvl = Keyword.get(opts, :lvl, 0)
    pending_pids = Keyword.get(opts, :pending, [])
    pending_count = max(1, length(pending_pids))
    pending = pending_new(pending_pids)
    monitored = monitored_new()
    state = {pending_count, pending_count, []}
    %This{lvl: lvl, state: state, pend: pending, mon: monitored}
  end


  def handle_done(from, lvl, result, %This{lvl: lvl} = this) do
    handle_events([{:done, from, result}], this)
  end

  def handle_done(from, lvl, result, this) do
    {:continue, buffer(lvl, {:done, from, result}, this)}
  end


  def handle_failed(from, lvl, reason, %This{lvl: lvl} = this) do
    handle_events([{:failed, from, reason}], this)
  end

  def handle_failed(from, lvl, reason, this) do
    {:continue, buffer(lvl, {:failed, from, reason}, this)}
  end


  def handle_delegated(from, lvl, pid, %This{lvl: lvl} = this) do
    handle_events([{:delegated, from, pid}], this)
  end

  def handle_delegated(from, lvl, pid, this) do
    {:continue, buffer(lvl, {:delegated, from, pid}, this)}
  end


  def handle_forked(from, lvl, results, delegates, %This{lvl: lvl} = this) do
    handle_events([{:forked, from, results, delegates}], this)
  end

  def handle_forked(from, lvl, results, delegates, this) do
    {:continue, buffer(lvl, {:forked, from, results, delegates}, this)}
  end


  def handle_down(pid, mon_ref, _reason, %This{mon: mon} = this) do
    case monitored_pop_monref(mon, pid, mon_ref) do
      {:ok, mon, _} ->
        # One of our monitored process just died.
        demonitor(mon)
        {:failed, :noproc}
      _ -> {:ignored, this}
    end
  end


  @doc """
  Must be called after a caller-defined time without any new message.
  This is used to start monitoring processes and detect dead ones.
  """
  def start_monitoring(%This{pend: pend, mon: mon} = this) do
    {pend2, mon2} = monitor(pend, mon)
    %This{this | pend: pend2, mon: mon2}
  end


  #===========================================================================
  # Internal Functions
  #===========================================================================

  defp buffer(lvl, event, %This{buffer: buff} = this) do
    case Map.fetch(buff, lvl) do
      :error -> %This{this | buffer: Map.put(buff, lvl, [event])}
      {:ok, events} ->
        %This{this | buffer: Map.put(buff, lvl, [event | events])}
    end
  end


  defp bootstrap(%This{lvl: lvl, buffer: buffer} = this) do
    case Map.fetch(buffer, lvl) do
      :error -> {:continue, this}
      {:ok, []} -> {:continue, %This{this | buffer: Map.delete(buffer, lvl)}}
      {:ok, events} ->
        handle_events(events, %This{this | buffer: Map.delete(buffer, lvl)})
    end
  end


  defp handle_events([], this), do: {:continue, this}

  defp handle_events([{:done, from, result} | rem], this) do
    %This{lvl: lvl, state: state, acc: acc, pend: pend, mon: mon} = this
    {pend2, mon2} = demonitor(pend, mon, from)
    case update_protocol(lvl, pend2, [], state) do
      :done ->
        demonitor(mon2)
        {:done, return_result(acc, result)}
      {:continue, ^lvl, pend3, state2} ->
        acc2 = append_result(acc, result)
        this2 = %This{this | state: state2, acc: acc2, pend: pend3, mon: mon2}
        handle_events(rem, this2)
      {:continue, lvl2, pend3, state2} ->
        acc2 = append_result(acc, result)
        this2 = %This{this | lvl: lvl2, state: state2, pend: pend3, mon: mon2, acc: acc2}
        bootstrap(this2)
    end
  end

  defp handle_events([{:failed, _from, reason} | _rem], %This{mon: mon}) do
    demonitor(mon)
    {:failed, reason}
  end

  defp handle_events([{:delegated, from, pid} | rem], this) do
    %This{lvl: lvl, state: state, pend: pend, mon: mon} = this
    {pend2, mon2} = demonitor(pend, mon, from)
    case update_protocol(lvl, pend2, [pid], state) do
      {:continue, ^lvl, pend3, state2} ->
        this2 = %This{this | state: state2, pend: pend3, mon: mon2}
        handle_events(rem, this2)
      {:continue, lvl2, pend3, state2} ->
        this2 = %This{this | lvl: lvl2, state: state2, pend: pend3, mon: mon2}
        bootstrap(this2)
    end
  end

  defp handle_events([{:forked, from, results, delegates} | rem], this) do
    %This{lvl: lvl, state: state, acc: acc, pend: pend, mon: mon} = this
    {pend2, mon2} = demonitor(pend, mon, from)
    case update_protocol(lvl, pend2, delegates, state) do
      :done ->
        demonitor(mon2)
        {:done, return_results(acc, results)}
      {:continue, ^lvl, pend3, state2} ->
        acc2 = append_results(acc, results)
        this2 = %This{this | state: state2, pend: pend3, mon: mon2, acc: acc2}
        handle_events(rem, this2)
      {:continue, lvl2, pend3, state2} ->
        acc2 = append_results(acc, results)
        this2 = %This{this | lvl: lvl2, state: state2, pend: pend3, mon: mon2, acc: acc2}
        bootstrap(this2)
    end
  end


  defp update_protocol(lvl, pend, delegated, {cmsg, tmsg, next}) do
    # Updates the pending messages counter for current level
    cmsg = cmsg - 1
    # Updates the total pending messages counter
    tmsg = tmsg - 1 + length(delegated)
    # Updates the next level process pid list
    next = delegated ++ next
    # handle the level progression
    update_protocol(lvl, cmsg, tmsg, pend, next)
  end


  defp update_protocol(_lvl, 0, 0, _pend, _next) do
    # Received the last response
    :done
  end

  defp update_protocol(lvl, 0, tmsg, _pend, next) do
    # Received the last response for current level
    {:continue, lvl + 1, pending_new(next), {tmsg, tmsg, []}}
  end

  defp update_protocol(lvl, cmsg, tmsg, pend, next) do
    # Received one more response for current level
    {:continue, lvl, pend, {cmsg, tmsg, next}}
  end


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
