defmodule Convex.SyncForkedTest do

  #===========================================================================
  # Sub-Modules
  #===========================================================================

  defmodule TestDirector do
    alias Convex.Context, as: Ctx
    alias Convex.Test.ServiceI1
    alias Convex.Test.ServiceI5
    alias Convex.Test.ServiceView
    def perform(ctx, [:index] = op, args), do: ServiceI1.perform(ctx, op, args)
    def perform(ctx, [:mon_bug] = op, args), do: ServiceI5.perform(ctx, op, args)
    def perform(ctx, [:show] = op, args), do: ServiceView.perform(ctx, op, args)
    def perform(ctx, _op, _args), do: Ctx.failed(ctx, :unknown_operation)
  end


  #===========================================================================
  # Includes
  #===========================================================================

  use ExUnit.Case, async: false

  import Convex.Pipeline

  alias Convex.Context.Sync
  alias Convex.Test.ServiceI1
  alias Convex.Test.ServiceI2
  alias Convex.Test.ServiceI3
  alias Convex.Test.ServiceI5
  alias Convex.Test.ServiceView
  alias Convex.SyncForkedTest.TestDirector


  #===========================================================================
  # Attributes
  #===========================================================================

  @delay 60
  @mon_delay 30
  @moduletag :context_test


  #===========================================================================
  # Tests Setup
  #===========================================================================

  setup do
    ServiceI1.start()
    ServiceI2.start()
    ServiceI3.start()
    ServiceI5.start()
    ServiceView.start()
    on_exit fn ->
      ServiceI1.stop()
      ServiceI2.stop()
      ServiceI3.stop()
      ServiceI5.stop()
      ServiceView.stop()
    end
    {:ok, [ctx: Sync.new(director: TestDirector)]}
  end


  #===========================================================================
  # Test Cases
  #===========================================================================

  test "simple one operation pipeline", %{ctx: ctx} do
    for inline <- combinations([:i1, :i2, :i3, :i4]) do
      assert [] =
        single_success(ctx, inline: inline,
                       i1: [], i2: [], i3: [], i4: [])
      assert [1, 2] =
        single_success(ctx, inline: inline,
                       i1: [1, 2], i2: [], i3: [], i4: [])
      assert [3, 4] =
        single_success(ctx, inline: inline,
                       i1: [], i2: [3, 4], i3: [], i4: [])
      assert [5, 6] =
        single_success(ctx, inline: inline,
                       i1: [], i2: [], i3: [5, 6], i4: [])
      assert [7, 8] =
        single_success(ctx, inline: inline,
                       i1: [], i2: [], i3: [], i4: [7, 8])
      assert [1, 2, 3, 4] =
        single_success(ctx, inline: inline,
                       i1: [1], i2: [2], i3: [3], i4: [4])
      assert [1, 2, 3, 4, 5, 6, 7, 8] =
        single_success(ctx, inline: inline,
                       i1: [1, 2], i2: [3, 4], i3: [5, 6], i4: [7, 8])
      assert [1, 2, 3, 3, 4, 5, 5, 6, 7, 7, 8, 9] =
        single_success(ctx, inline: inline,
                       i1: [1, 2, 3], i2: [3, 4, 5], i3: [5, 6, 7], i4: [7, 8, 9])
    end
  end


  test "simple multi operation pipeline", %{ctx: ctx} do
    for inline <- combinations([:i1, :i2, :i3, :i4]) do
      assert [] =
        multi_success(ctx, :x, inline: inline,
                      i1: [], i2: [], i3: [], i4: [])
      assert [a: 1, a: 2] =
        multi_success(ctx, :a, inline: inline,
                      i1: [1, 2], i2: [], i3: [], i4: [])
      assert [b: 3, b: 4] =
        multi_success(ctx, :b, inline: inline,
                      i1: [], i2: [3, 4], i3: [], i4: [])
      assert [c: 5, c: 6] =
        multi_success(ctx, :c, inline: inline,
                      i1: [], i2: [], i3: [5, 6], i4: [])
      assert [d: 7, d: 8] =
        multi_success(ctx, :d, inline: inline,
                      i1: [], i2: [], i3: [], i4: [7, 8])
      assert [e: 1, e: 2, e: 3, e: 4] =
        multi_success(ctx, :e, inline: inline,
                      i1: [1], i2: [2], i3: [3], i4: [4])
      assert [f: 1, f: 2, f: 3, f: 4, f: 5, f: 6, f: 7, f: 8] =
        multi_success(ctx, :f, inline: inline,
                      i1: [1, 2], i2: [3, 4], i3: [5, 6], i4: [7, 8])
      assert [g: 1, g: 2, g: 3, g: 3, g: 4, g: 5, g: 5, g: 6, g: 7, g: 7, g: 8, g: 9] =
        multi_success(ctx, :g, inline: inline,
                      i1: [1, 2, 3], i2: [3, 4, 5], i3: [5, 6, 7], i4: [7, 8, 9])
    end
  end


  test "simple multi operation pipeline with delay", %{ctx: ctx} do
    all_delays = [i1: @delay, i2: @delay, i3: @delay, i4: @delay, view: @delay]
    for delays <- combinations(all_delays) do
      delays = Enum.into(delays, %{})
      for inline <- combinations([:i1, :i2, :i3, :i4]) do
        assert [g: 1, g: 2, g: 3, g: 4, g: 5, g: 6] =
          multi_success(ctx, :g, inline: inline, delays: delays,
                        i1: [1], i2: [], i3: [2, 3], i4: [4, 5, 6])
      end
    end
  end


  test "failed single operation pipeline", %{ctx: ctx} do
    for inline <- combinations([:i1, :i2, :i3, :i4]) do
      assert :foo = single_failure(ctx, inline: inline,
                      i1: {:fail, :foo}, i2: [3, 4], i3: [5, 6], i4: [7, 8])
      assert :bar = single_failure(ctx, inline: inline,
                      i1: [1, 2], i2: {:fail, :bar}, i3: [5, 6], i4: [7, 8])
      assert :buz = single_failure(ctx, inline: inline,
                      i1: [1, 2], i2: [3, 4], i3: {:fail, :buz}, i4: [7, 8])
      assert :biz = single_failure(ctx, inline: inline,
                      i1: [1, 2], i2: [3, 4], i3: [5, 6], i4: {:fail, :biz})
    end
  end


  test "failed multi operation pipeline", %{ctx: ctx} do
    for inline <- combinations([:i1, :i2, :i3, :i4]) do
      assert :foo = multi_failure(ctx, :a, inline: inline,
                      i1: {:fail, :foo}, i2: [3, 4], i3: [5, 6], i4: [7, 8])
      assert :bar = multi_failure(ctx, :b, inline: inline,
                      i1: [1, 2], i2: {:fail, :bar}, i3: [5, 6], i4: [7, 8])
      assert :buz = multi_failure(ctx, :c, inline: inline,
                      i1: [1, 2], i2: [3, 4], i3: {:fail, :buz}, i4: [7, 8])
      assert :biz = multi_failure(ctx, :d, inline: inline,
                      i1: [1, 2], i2: [3, 4], i3: [5, 6], i4: {:fail, :biz})
      assert :boz = multi_failure(ctx, {:fail, :boz}, inline: inline,
                      i1: [1, 2], i2: [3, 4], i3: [5, 6], i4: [7, 8])
    end
  end


  test "dead multi operation pipeline", %{ctx: ctx} do
    # Don't test with the entry point inlined or the test would die
    for inline <- combinations([:i2, :i3, :i4]) do
      assert :noproc = multi_failure(ctx, :a, inline: inline,
                        i1: {:die, :foo}, i2: [3, 4], i3: [5, 6], i4: [7, 8])
      reset_services()
      assert :noproc = multi_failure(ctx, :b, inline: inline,
                        i1: [1, 2], i2: {:die, :bar}, i3: [5, 6], i4: [7, 8])
      reset_services()
      assert :noproc = multi_failure(ctx, :c, inline: inline,
                        i1: [1, 2], i2: [3, 4], i3: {:die, :buz}, i4: [7, 8])
      reset_services()
      assert :noproc = multi_failure(ctx, :d, inline: inline,
                        i1: [1, 2], i2: [3, 4], i3: [5, 6], i4: {:die, :biz})
      reset_services()
      assert :noproc = multi_failure(ctx, {:die, :boz}, inline: inline,
                        i1: [1, 2], i2: [3, 4], i3: [5, 6], i4: [7, 8])
      reset_services()
    end
  end


  test "monitoring bug while forking", %{ctx: ctx} do
    result = perform with: ctx, monitoring_delay: @mon_delay do
      mon_bug i5: [1, 2], i3: [3, 4], i4: [5, 6], delays: %{i3: @delay}
      show view: :x
    end
    assert {:ok, values} = result
    assert [x: 1, x: 2, x: 3, x: 4, x: 5, x: 6] == sort(values)
  end


  #===========================================================================
  # Internal Functions
  #===========================================================================

  defp single_failure(ctx, args) do
    result = perform with: ctx, monitoring_delay: @mon_delay do
      index(^args)
    end
    assert {:error, reason} = result
    reason
  end


  defp multi_failure(ctx, view, args) do
    result = perform with: ctx, monitoring_delay: @mon_delay do
      index ^args
      show view: ^view
    end
    assert {:error, reason} = result
    reason
  end


  defp single_success(ctx, args) do
    result = perform with: ctx, monitoring_delay: @mon_delay do
      index ^args
    end
    assert {:ok, values} = result
    sort(values)
  end


  defp multi_success(ctx, view, args) do
    result = perform with: ctx, monitoring_delay: @mon_delay do
      index ^args
      show view: ^view
    end
    assert {:ok, values} = result
    sort(values)
  end


  defp reset_services() do
    ServiceI1.stop()
    ServiceI2.stop()
    ServiceI3.stop()
    ServiceI5.stop()
    ServiceView.stop()
    ServiceI1.start()
    ServiceI2.start()
    ServiceI3.start()
    ServiceI5.start()
    ServiceView.start()
  end


  defp sort([{_, _} | _] = kw) when is_list(kw) do
    Enum.sort(kw, fn {_, a}, {_, b} -> a < b end)
  end

  defp sort(list) when is_list(list), do:  Enum.sort(list)


  defp combinations([]), do: []

  defp combinations([v]), do: [[v]]

  defp combinations([v | rem]) do
    combs = combinations(rem)
    Enum.map(combs, &([v | &1])) ++ combs
  end

end
