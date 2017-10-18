defmodule Convex.SyncStraightTest do

  #===========================================================================
  # Sub-Modules
  #===========================================================================

  defmodule TestDirector do
    alias Convex.Context, as: Ctx
    alias Convex.Test.ServiceA
    alias Convex.Test.ServiceB
    def perform(ctx, [:a | _] = op, args), do: ServiceA.perform(ctx, op, args)
    def perform(ctx, [:b | _] = op, args), do: ServiceB.perform(ctx, op, args)
    def perform(ctx, [:c | _] = op, args), do: ServiceA.perform(ctx, op, args)
    def perform(ctx, [:d | _] = op, args), do: ServiceB.perform(ctx, op, args)
    def perform(ctx, _op, _args), do: Ctx.failed(ctx, :unknown_operation)
  end


  #===========================================================================
  # Includes
  #===========================================================================

  use ExUnit.Case, async: false

  import Convex.Pipeline

  alias Convex.Context.Sync
  alias Convex.Test.ServiceA
  alias Convex.Test.ServiceB
  alias Convex.Test.ServiceC
  alias Convex.SyncStraightTest.TestDirector


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
    ServiceA.start()
    ServiceB.start()
    ServiceC.start()
    on_exit fn ->
      ServiceA.stop()
      ServiceB.stop()
      ServiceC.stop()
    end
    {:ok, [ctx: Sync.new(director: TestDirector)]}
  end


  #===========================================================================
  # Test Cases
  #===========================================================================

  test "successful single operation pipelines", %{ctx: ctx} do
    assert [a: 1] = perform!(ctx, a.out(done: 1, acc: []))
    assert [a: 2] = perform!(ctx, a.in(done: 2, acc: []))
    assert [b: 3] = perform!(ctx, b.out(done: 3, acc: []))
    assert [b: 4] = perform!(ctx, b.in(done: 4, acc: []))
    assert [c: 5] = perform!(ctx, c.out.out(done: 5, acc: []))
    assert [c: 6] = perform!(ctx, c.out.in(done: 6, acc: []))
    assert [c: 7] = perform!(ctx, c.in.out(done: 7, acc: []))
    assert [c: 8] = perform!(ctx, c.in.in(done: 8, acc: []))
    assert [d: 9] = perform!(ctx, d.out(done: 9, acc: []))
    assert [d: 10] = perform!(ctx, d.in(done: 10, acc: []))
  end


  test "successful multi service A operation pipelines", %{ctx: ctx} do
    result = perform! with: ctx do
      acc1 = a.out done: 1, acc: []
      acc2 = a.out done: 2, acc: acc1
             a.out done: 3, acc: acc2
    end
    assert [a: 3, a: 2, a: 1] = result

    result = perform! with: ctx do
      acc1 = a.in  done: 1, acc: []
      acc2 = a.out done: 2, acc: acc1
             a.out done: 3, acc: acc2
    end
    assert [a: 3, a: 2, a: 1] = result

    result = perform! with: ctx do
      acc1 = a.out done: 1, acc: []
      acc2 = a.in  done: 2, acc: acc1
             a.out done: 3, acc: acc2
    end
    assert [a: 3, a: 2, a: 1] = result

    result = perform! with: ctx do
      acc1 = a.out done: 1, acc: []
      acc2 = a.out done: 2, acc: acc1
             a.in  done: 3, acc: acc2
    end
    assert [a: 3, a: 2, a: 1] = result

    result = perform! with: ctx do
      acc1 = a.in  done: 1, acc: []
      acc2 = a.in  done: 2, acc: acc1
             a.out done: 3, acc: acc2
    end
    assert [a: 3, a: 2, a: 1] = result

    result = perform! with: ctx do
      acc1 = a.in  done: 1, acc: []
      acc2 = a.out done: 2, acc: acc1
             a.in  done: 3, acc: acc2
    end
    assert [a: 3, a: 2, a: 1] = result

    result = perform! with: ctx do
      acc1 = a.out done: 1, acc: []
      acc2 = a.in  done: 2, acc: acc1
             a.in  done: 3, acc: acc2
    end
    assert [a: 3, a: 2, a: 1] = result
  end


  test "successful multi service C operation pipelines", %{ctx: ctx} do
    result = perform! with: ctx do
      acc1 = c.out.out done: 1, acc: []
      acc2 = c.out.out done: 2, acc: acc1
             c.out.out done: 3, acc: acc2
    end
    assert [c: 3, c: 2, c: 1] = result

    result = perform! with: ctx do
      acc1 = c.in.out  done: 1, acc: []
      acc2 = c.out.out done: 2, acc: acc1
             c.out.out done: 3, acc: acc2
    end
    assert [c: 3, c: 2, c: 1] = result

    result = perform! with: ctx do
      acc1 = c.out.out done: 1, acc: []
      acc2 = c.in.out  done: 2, acc: acc1
             c.out.out done: 3, acc: acc2
    end
    assert [c: 3, c: 2, c: 1] = result

    result = perform! with: ctx do
      acc1 = c.out.out done: 1, acc: []
      acc2 = c.out.out done: 2, acc: acc1
             c.in.out  done: 3, acc: acc2
    end
    assert [c: 3, c: 2, c: 1] = result

    result = perform! with: ctx do
      acc1 = c.in.out  done: 1, acc: []
      acc2 = c.in.out  done: 2, acc: acc1
             c.out.out done: 3, acc: acc2
    end
    assert [c: 3, c: 2, c: 1] = result

    result = perform! with: ctx do
      acc1 = c.in.out  done: 1, acc: []
      acc2 = c.out.out done: 2, acc: acc1
             c.in.out  done: 3, acc: acc2
    end
    assert [c: 3, c: 2, c: 1] = result

    result = perform! with: ctx do
      acc1 = c.out.out done: 1, acc: []
      acc2 = c.in.out  done: 2, acc: acc1
             c.in.out  done: 3, acc: acc2
    end
    assert [c: 3, c: 2, c: 1] = result

    result = perform! with: ctx do
      acc1 = c.out.in  done: 1, acc: []
      acc2 = c.out.in  done: 2, acc: acc1
             c.out.in  done: 3, acc: acc2
    end
    assert [c: 3, c: 2, c: 1] = result

    result = perform! with: ctx do
      acc1 = c.in.in   done: 1, acc: []
      acc2 = c.out.in  done: 2, acc: acc1
             c.out.in  done: 3, acc: acc2
    end
    assert [c: 3, c: 2, c: 1] = result

    result = perform! with: ctx do
      acc1 = c.out.in  done: 1, acc: []
      acc2 = c.in.in   done: 2, acc: acc1
             c.out.in  done: 3, acc: acc2
    end
    assert [c: 3, c: 2, c: 1] = result

    result = perform! with: ctx do
      acc1 = c.out.in  done: 1, acc: []
      acc2 = c.out.in  done: 2, acc: acc1
             c.in.in   done: 3, acc: acc2
    end
    assert [c: 3, c: 2, c: 1] = result

    result = perform! with: ctx do
      acc1 = c.in.in   done: 1, acc: []
      acc2 = c.in.in   done: 2, acc: acc1
             c.out.in  done: 3, acc: acc2
    end
    assert [c: 3, c: 2, c: 1] = result

    result = perform! with: ctx do
      acc1 = c.in.in   done: 1, acc: []
      acc2 = c.out.in  done: 2, acc: acc1
             c.in.in   done: 3, acc: acc2
    end
    assert [c: 3, c: 2, c: 1] = result

    result = perform! with: ctx do
      acc1 = c.out.in  done: 1, acc: []
      acc2 = c.in.in   done: 2, acc: acc1
             c.in.in   done: 3, acc: acc2
    end
    assert [c: 3, c: 2, c: 1] = result
  end


  test "successful multi service D operation pipelines", %{ctx: ctx} do
    result = perform! with: ctx do
      acc1 = d.out done: 1, acc: []
      acc2 = d.out done: 2, acc: acc1
             d.out done: 3, acc: acc2
    end
    assert [d: 3, d: 2, d: 1] = result

    result = perform! with: ctx do
      acc1 = d.in  done: 1, acc: []
      acc2 = d.out done: 2, acc: acc1
             d.out done: 3, acc: acc2
    end
    assert [d: 3, d: 2, d: 1] = result

    result = perform! with: ctx do
      acc1 = d.out done: 1, acc: []
      acc2 = d.in  done: 2, acc: acc1
             d.out done: 3, acc: acc2
    end
    assert [d: 3, d: 2, d: 1] = result

    result = perform! with: ctx do
      acc1 = d.out done: 1, acc: []
      acc2 = d.out done: 2, acc: acc1
             d.in  done: 3, acc: acc2
    end
    assert [d: 3, d: 2, d: 1] = result

    result = perform! with: ctx do
      acc1 = d.in  done: 1, acc: []
      acc2 = d.in  done: 2, acc: acc1
             d.out done: 3, acc: acc2
    end
    assert [d: 3, d: 2, d: 1] = result

    result = perform! with: ctx do
      acc1 = d.in  done: 1, acc: []
      acc2 = d.out done: 2, acc: acc1
             d.in  done: 3, acc: acc2
    end
    assert [d: 3, d: 2, d: 1] = result

    result = perform! with: ctx do
      acc1 = d.out done: 1, acc: []
      acc2 = d.in  done: 2, acc: acc1
             d.in  done: 3, acc: acc2
    end
    assert [d: 3, d: 2, d: 1] = result
  end


  test "successful multi service A operation pipelines with delay", %{ctx: ctx} do
    result = perform! with: ctx, monitoring_delay: @mon_delay do
      acc1 = a.out delay: @delay, done: 1, acc: []
      acc2 = a.out delay: @delay, done: 2, acc: acc1
             a.out delay: @delay, done: 3, acc: acc2
    end
    assert [a: 3, a: 2, a: 1] = result

    result = perform! with: ctx, monitoring_delay: @mon_delay do
      acc1 = a.in  delay: @delay, done: 1, acc: []
      acc2 = a.out delay: @delay, done: 2, acc: acc1
             a.out delay: @delay, done: 3, acc: acc2
    end
    assert [a: 3, a: 2, a: 1] = result

    result = perform! with: ctx, monitoring_delay: @mon_delay do
      acc1 = a.out delay: @delay, done: 1, acc: []
      acc2 = a.in  delay: @delay, done: 2, acc: acc1
             a.out delay: @delay, done: 3, acc: acc2
    end
    assert [a: 3, a: 2, a: 1] = result

    result = perform! with: ctx, monitoring_delay: @mon_delay do
      acc1 = a.out delay: @delay, done: 1, acc: []
      acc2 = a.out delay: @delay, done: 2, acc: acc1
             a.in  delay: @delay, done: 3, acc: acc2
    end
    assert [a: 3, a: 2, a: 1] = result

    result = perform! with: ctx, monitoring_delay: @mon_delay do
      acc1 = a.in  delay: @delay, done: 1, acc: []
      acc2 = a.in  delay: @delay, done: 2, acc: acc1
             a.out delay: @delay, done: 3, acc: acc2
    end
    assert [a: 3, a: 2, a: 1] = result

    result = perform! with: ctx, monitoring_delay: @mon_delay do
      acc1 = a.in  delay: @delay, done: 1, acc: []
      acc2 = a.out delay: @delay, done: 2, acc: acc1
             a.in  delay: @delay, done: 3, acc: acc2
    end
    assert [a: 3, a: 2, a: 1] = result

    result = perform! with: ctx, monitoring_delay: @mon_delay do
      acc1 = a.out delay: @delay, done: 1, acc: []
      acc2 = a.in  delay: @delay, done: 2, acc: acc1
             a.in  delay: @delay, done: 3, acc: acc2
    end
    assert [a: 3, a: 2, a: 1] = result
  end


  test "successful multi service C operation pipelines with delay", %{ctx: ctx} do
    result = perform! with: ctx, monitoring_delay: @mon_delay do
      acc1 = c.out.out delay: @delay, done: 1, acc: []
      acc2 = c.out.out delay: @delay, done: 2, acc: acc1
             c.out.out delay: @delay, done: 3, acc: acc2
    end
    assert [c: 3, c: 2, c: 1] = result

    result = perform! with: ctx, monitoring_delay: @mon_delay do
      acc1 = c.in.out  delay: @delay, done: 1, acc: []
      acc2 = c.out.out delay: @delay, done: 2, acc: acc1
             c.out.out delay: @delay, done: 3, acc: acc2
    end
    assert [c: 3, c: 2, c: 1] = result

    result = perform! with: ctx, monitoring_delay: @mon_delay do
      acc1 = c.out.out delay: @delay, done: 1, acc: []
      acc2 = c.in.out  delay: @delay, done: 2, acc: acc1
             c.out.out delay: @delay, done: 3, acc: acc2
    end
    assert [c: 3, c: 2, c: 1] = result

    result = perform! with: ctx, monitoring_delay: @mon_delay do
      acc1 = c.out.out delay: @delay, done: 1, acc: []
      acc2 = c.out.out delay: @delay, done: 2, acc: acc1
             c.in.out  delay: @delay, done: 3, acc: acc2
    end
    assert [c: 3, c: 2, c: 1] = result

    result = perform! with: ctx, monitoring_delay: @mon_delay do
      acc1 = c.in.out  delay: @delay, done: 1, acc: []
      acc2 = c.in.out  delay: @delay, done: 2, acc: acc1
             c.out.out delay: @delay, done: 3, acc: acc2
    end
    assert [c: 3, c: 2, c: 1] = result

    result = perform! with: ctx, monitoring_delay: @mon_delay do
      acc1 = c.in.out  delay: @delay, done: 1, acc: []
      acc2 = c.out.out delay: @delay, done: 2, acc: acc1
             c.in.out  delay: @delay, done: 3, acc: acc2
    end
    assert [c: 3, c: 2, c: 1] = result

    result = perform! with: ctx, monitoring_delay: @mon_delay do
      acc1 = c.out.out delay: @delay, done: 1, acc: []
      acc2 = c.in.out  delay: @delay, done: 2, acc: acc1
             c.in.out  delay: @delay, done: 3, acc: acc2
    end
    assert [c: 3, c: 2, c: 1] = result

    result = perform! with: ctx, monitoring_delay: @mon_delay do
      acc1 = c.out.in  delay: @delay, done: 1, acc: []
      acc2 = c.out.in  delay: @delay, done: 2, acc: acc1
             c.out.in  delay: @delay, done: 3, acc: acc2
    end
    assert [c: 3, c: 2, c: 1] = result

    result = perform! with: ctx, monitoring_delay: @mon_delay do
      acc1 = c.in.in   delay: @delay, done: 1, acc: []
      acc2 = c.out.in  delay: @delay, done: 2, acc: acc1
             c.out.in  delay: @delay, done: 3, acc: acc2
    end
    assert [c: 3, c: 2, c: 1] = result

    result = perform! with: ctx, monitoring_delay: @mon_delay do
      acc1 = c.out.in  delay: @delay, done: 1, acc: []
      acc2 = c.in.in   delay: @delay, done: 2, acc: acc1
             c.out.in  delay: @delay, done: 3, acc: acc2
    end
    assert [c: 3, c: 2, c: 1] = result

    result = perform! with: ctx, monitoring_delay: @mon_delay do
      acc1 = c.out.in  delay: @delay, done: 1, acc: []
      acc2 = c.out.in  delay: @delay, done: 2, acc: acc1
             c.in.in   delay: @delay, done: 3, acc: acc2
    end
    assert [c: 3, c: 2, c: 1] = result

    result = perform! with: ctx, monitoring_delay: @mon_delay do
      acc1 = c.in.in   delay: @delay, done: 1, acc: []
      acc2 = c.in.in   delay: @delay, done: 2, acc: acc1
             c.out.in  delay: @delay, done: 3, acc: acc2
    end
    assert [c: 3, c: 2, c: 1] = result

    result = perform! with: ctx, monitoring_delay: @mon_delay do
      acc1 = c.in.in   delay: @delay, done: 1, acc: []
      acc2 = c.out.in  delay: @delay, done: 2, acc: acc1
             c.in.in   delay: @delay, done: 3, acc: acc2
    end
    assert [c: 3, c: 2, c: 1] = result

    result = perform! with: ctx, monitoring_delay: @mon_delay do
      acc1 = c.out.in  delay: @delay, done: 1, acc: []
      acc2 = c.in.in   delay: @delay, done: 2, acc: acc1
             c.in.in   delay: @delay, done: 3, acc: acc2
    end
    assert [c: 3, c: 2, c: 1] = result
  end


  test "successful multi service D operation pipelines with delay", %{ctx: ctx} do
    result = perform! with: ctx, monitoring_delay: @mon_delay do
      acc1 = d.out delay: @delay, done: 1, acc: []
      acc2 = d.out delay: @delay, done: 2, acc: acc1
             d.out delay: @delay, done: 3, acc: acc2
    end
    assert [d: 3, d: 2, d: 1] = result

    result = perform! with: ctx, monitoring_delay: @mon_delay do
      acc1 = d.in  delay: @delay, done: 1, acc: []
      acc2 = d.out delay: @delay, done: 2, acc: acc1
             d.out delay: @delay, done: 3, acc: acc2
    end
    assert [d: 3, d: 2, d: 1] = result

    result = perform! with: ctx, monitoring_delay: @mon_delay do
      acc1 = d.out delay: @delay, done: 1, acc: []
      acc2 = d.in  delay: @delay, done: 2, acc: acc1
             d.out delay: @delay, done: 3, acc: acc2
    end
    assert [d: 3, d: 2, d: 1] = result

    result = perform! with: ctx, monitoring_delay: @mon_delay do
      acc1 = d.out delay: @delay, done: 1, acc: []
      acc2 = d.out delay: @delay, done: 2, acc: acc1
             d.in  delay: @delay, done: 3, acc: acc2
    end
    assert [d: 3, d: 2, d: 1] = result

    result = perform! with: ctx, monitoring_delay: @mon_delay do
      acc1 = d.in  delay: @delay, done: 1, acc: []
      acc2 = d.in  delay: @delay, done: 2, acc: acc1
             d.out delay: @delay, done: 3, acc: acc2
    end
    assert [d: 3, d: 2, d: 1] = result

    result = perform! with: ctx, monitoring_delay: @mon_delay do
      acc1 = d.in  delay: @delay, done: 1, acc: []
      acc2 = d.out delay: @delay, done: 2, acc: acc1
             d.in  delay: @delay, done: 3, acc: acc2
    end
    assert [d: 3, d: 2, d: 1] = result

    result = perform! with: ctx, monitoring_delay: @mon_delay do
      acc1 = d.out delay: @delay, done: 1, acc: []
      acc2 = d.in  delay: @delay, done: 2, acc: acc1
             d.in  delay: @delay, done: 3, acc: acc2
    end
    assert [d: 3, d: 2, d: 1] = result
  end


  test "successful complex pipelines", %{ctx: ctx} do
    result = perform! with: ctx do
      acc1  = a.out     done:  1, acc: []
      acc2  = b.in      done:  2, acc: acc1
      acc3  = b.in      done:  3, acc: acc2
      acc4  = d.out     done:  4, acc: acc3
      acc5  = b.out     done:  5, acc: acc4
      acc6  = b.out     done:  6, acc: acc5
      acc7  = c.out.in  done:  7, acc: acc6
      acc8  = c.in.in   done:  8, acc: acc7
      acc9  = d.in      done:  9, acc: acc8
      acc10 = a.in      done: 10, acc: acc9
      acc11 = c.out.out done: 11, acc: acc10
      acc12 = c.out.in  done: 12, acc: acc11
      acc13 = c.in.out  done: 13, acc: acc12
      acc14 = c.in.out  done: 14, acc: acc13
      acc15 = c.in.in   done: 15, acc: acc14
      acc16 = a.in      done: 16, acc: acc15
      acc17 = d.in      done: 17, acc: acc16
      acc18 = d.out     done: 18, acc: acc17
      acc19 = c.out.out done: 19, acc: acc18
      acc20 = a.out     done: 20, acc: acc19
    end
    assert [a: 20, c: 19, d: 18, d: 17, a: 16, c: 15,
            c: 14, c: 13, c: 12, c: 11, a: 10, d: 9,
            c: 8, c: 7, b: 6, b: 5, d: 4, b: 3, b: 2, a: 1] = result

    result = perform! with: ctx do
      acc1  = c.in.in   done:  1, acc: []
      acc2  = b.in      done:  2, acc: acc1
      acc3  = c.in.out  done:  3, acc: acc2
      acc4  = c.in.in   done:  4, acc: acc3
      acc5  = a.out     done:  5, acc: acc4
      acc6  = c.in.out  done:  6, acc: acc5
      acc7  = c.out.in  done:  7, acc: acc6
      acc8  = d.out     done:  8, acc: acc7
      acc9  = d.out     done:  9, acc: acc8
      acc10 = a.in      done: 10, acc: acc9
      acc11 = c.out.in  done: 11, acc: acc10
      acc12 = b.out     done: 12, acc: acc11
      acc13 = c.out.out done: 13, acc: acc12
      acc14 = b.out     done: 14, acc: acc13
      acc15 = c.out.out done: 15, acc: acc14
      acc16 = d.in      done: 16, acc: acc15
      acc17 = d.in      done: 17, acc: acc16
      acc18 = a.in      done: 18, acc: acc17
      acc19 = a.out     done: 19, acc: acc18
              b.in      done: 20, acc: acc19
    end
    assert [b: 20, a: 19, a: 18, d: 17, d: 16, c: 15,
            b: 14, c: 13, b: 12, c: 11, a: 10, d: 9,
            d: 8, c: 7, c: 6, a: 5, c: 4, c: 3, b: 2, c: 1] = result
  end


  test "successful complex pipelines with delay", %{ctx: ctx} do
    result = perform! with: ctx, monitoring_delay: @mon_delay do
      acc1  = d.in      delay: @delay, done:  1, acc: []
      acc2  = d.out                    done:  2, acc: acc1
      acc3  = d.in                     done:  3, acc: acc2
      acc4  = b.in      delay: @delay, done:  4, acc: acc3
      acc5  = b.out                    done:  5, acc: acc4
      acc6  = c.in.out  delay: @delay, done:  6, acc: acc5
      acc7  = c.out.out                done:  7, acc: acc6
      acc8  = a.in      delay: @delay, done:  8, acc: acc7
      acc9  = c.in.in                  done:  9, acc: acc8
      acc10 = c.out.in  delay: @delay, done: 10, acc: acc9
      acc11 = a.out                    done: 11, acc: acc10
      acc12 = c.out.in                 done: 12, acc: acc11
      acc13 = c.out.out delay: @delay, done: 13, acc: acc12
      acc14 = d.out     delay: @delay, done: 14, acc: acc13
      acc15 = c.in.in   delay: @delay, done: 15, acc: acc14
      acc16 = b.in                     done: 16, acc: acc15
      acc17 = c.in.out                 done: 17, acc: acc16
      acc18 = a.out     delay: @delay, done: 18, acc: acc17
      acc19 = a.in                     done: 19, acc: acc18
      acc20 = b.out     delay: @delay, done: 20, acc: acc19
    end
    assert [b: 20, a: 19, a: 18, c: 17, b: 16, c: 15,
            d: 14, c: 13, c: 12, a: 11, c: 10, c: 9,
            a: 8, c: 7, c: 6, b: 5, b: 4, d: 3, d: 2, d: 1] = result


    result = perform! with: ctx, monitoring_delay: @mon_delay do
      acc1  = c.in.in   delay: @delay, done:  1, acc: []
      acc2  = a.in                     done:  2, acc: acc1
      acc3  = c.out.out                done:  3, acc: acc2
      acc4  = a.out                    done:  4, acc: acc3
      acc5  = d.in                     done:  5, acc: acc4
      acc6  = d.out     delay: @delay, done:  6, acc: acc5
      acc7  = c.in.out  delay: @delay, done:  7, acc: acc6
      acc8  = b.in      delay: @delay, done:  8, acc: acc7
      acc9  = c.in.in                  done:  9, acc: acc8
      acc10 = b.out                    done: 10, acc: acc9
      acc11 = c.out.in  delay: @delay, done: 11, acc: acc10
      acc12 = b.out     delay: @delay, done: 12, acc: acc11
      acc13 = c.out.in                 done: 13, acc: acc12
      acc14 = a.out     delay: @delay, done: 14, acc: acc13
      acc15 = c.in.out                 done: 15, acc: acc14
      acc16 = a.in      delay: @delay, done: 16, acc: acc15
      acc17 = d.out                    done: 17, acc: acc16
      acc18 = d.in      delay: @delay, done: 18, acc: acc17
      acc19 = b.in                     done: 19, acc: acc18
      acc20 = c.out.out delay: @delay, done: 20, acc: acc19
    end
    assert [c: 20, b: 19, d: 18, d: 17, a: 16, c: 15,
            a: 14, c: 13, b: 12, c: 11, b: 10, c: 9,
            b: 8, c: 7, d: 6, d: 5, a: 4, c: 3, a: 2, c: 1] = result
  end


  test "failed single operation pipelines", %{ctx: ctx} do
    assert {:error, :foo0} = perform(ctx, a.out(fail: :foo0))
    assert {:error, :foo1} = perform(ctx, a.in(fail: :foo1))
    assert {:error, :foo2} = perform(ctx, b.out(fail: :foo2))
    assert {:error, :foo3} = perform(ctx, b.in(fail: :foo3))
    assert {:error, :foo4} = perform(ctx, c.out.out(fail: :foo4))
    assert {:error, :foo5} = perform(ctx, c.out.in(fail: :foo5))
    assert {:error, :foo6} = perform(ctx, c.in.out(fail: :foo6))
    assert {:error, :foo7} = perform(ctx, c.in.in(fail: :foo7))
    assert {:error, :foo8} = perform(ctx, d.out(fail: :foo8))
    assert {:error, :foo9} = perform(ctx, d.in(fail: :foo9))
  end


  test "failed multi operation pipelines", %{ctx: ctx} do
    result = perform with: ctx do
      acc1  = c.in.in   done:  1, acc: []
      acc2  = d.out     done:  2, acc: acc1
      acc3  = b.in      fail: :foo0
    end
    assert {:error, :foo0} = result

    result = perform with: ctx do
      acc1  = c.in.out  done:  1, acc: []
      acc2  = c.out.in  done:  2, acc: acc1
      acc3  = d.in      fail: :foo1
    end
    assert {:error, :foo1} = result

    result = perform with: ctx do
      acc1  = a.in      done:  1, acc: []
      acc2  = c.out.out done:  2, acc: acc1
      acc3  = c.in.out  fail: :foo2
    end
    assert {:error, :foo2} = result

    result = perform with: ctx do
      acc1  = a.out     done:  1, acc: []
      acc2  = c.in.in   done:  2, acc: acc1
      acc3  = c.out.in  fail: :foo3
    end
    assert {:error, :foo3} = result

    result = perform with: ctx do
      acc1  = c.out.in  done:  1, acc: []
      acc2  = d.out     done:  2, acc: acc1
      acc3  = a.in      fail: :foo4
    end
    assert {:error, :foo4} = result

    result = perform with: ctx do
      acc1  = d.in      done:  1, acc: []
      acc2  = b.out     done:  2, acc: acc1
      acc3  = c.out.out fail: :foo5
    end
    assert {:error, :foo5} = result

    result = perform with: ctx do
      acc1  = b.out     done:  1, acc: []
      acc2  = a.in      done:  2, acc: acc1
      acc3  = d.out     fail: :foo6
    end
    assert {:error, :foo6} = result

    result = perform with: ctx do
      acc1  = a.in      done:  1, acc: []
      acc2  = a.out     done:  2, acc: acc1
      acc3  = b.out     fail: :foo7
    end
    assert {:error, :foo7} = result

    result = perform with: ctx do
      acc1  = c.out.in  done:  1, acc: []
      acc2  = b.out     done:  2, acc: acc1
      acc3  = a.out     fail: :foo8
    end
    assert {:error, :foo8} = result

    result = perform with: ctx do
      acc1  = a.out     done:  1, acc: []
      acc2  = d.in      done:  2, acc: acc1
      acc3  = c.in.in   fail: :foo9
    end
    assert {:error, :foo9} = result


    result = perform with: ctx do
      acc1  = c.in.out  done:  1, acc: []
      acc2  = b.in      done:  2, acc: acc1
      acc3  = d.out     delegate_fail: :foo10
    end
    assert {:error, :foo10} = result

    result = perform with: ctx do
      acc1  = a.in      done:  1, acc: []
      acc2  = c.out.in  done:  2, acc: acc1
      acc3  = d.in      delegate_fail: :foo11
    end
    assert {:error, :foo11} = result
  end


  test "single operation pipeline with killd process", %{ctx: ctx} do
    # Service D is started for every operation
    assert {:error, :noproc} = perform(ctx, d.in(die: :kill))
    assert {:ok, [d: 1]} = perform(ctx, d.out(done: 1, acc: []))

    # Should kill service C:
    assert {:error, :noproc} = perform(ctx, c.in.in(die: :kill))

    # Should kill service B
    assert {:error, :noproc} = perform(ctx, b.in(die: :kill))

    # Should kill service A:
    assert {:error, :noproc} = perform(ctx, a.in(die: :kill))

    # Services A, B and C should be dead
    assert {:error, :noproc} = perform(ctx, a.in(done: 1, acc: []))
    assert {:error, :noproc} = perform(ctx, b.in(done: 1, acc: []))
    assert {:error, :noproc} = perform(ctx, c.in.in(done: 1, acc: []))
    assert {:error, :noproc} = perform(ctx, c.in.out(done: 1, acc: []))
    assert {:error, :noproc} = perform(ctx, c.out.in(done: 1, acc: []))
  end


  test "single operation pipeline with killd process (inline)", %{ctx: ctx} do
    # Should kill service A:
    assert {:error, :noproc} = perform(ctx, c.in.out(die: :kill))
    assert {:error, :noproc} = perform(ctx, a.in(done: 1, acc: []))

    # Service C should still be running
    assert {:ok, [c: 1]} = perform(ctx, c.out.in(done: 1, acc: []))

    # Should kill service C:
    assert {:error, :noproc} = perform(ctx, c.out.in(die: :kill))
    assert {:error, :noproc} = perform(ctx, c.out.in(done: 1, acc: []))
  end


  test "single operation pipeline with killd process with delay", %{ctx: ctx} do
    # Service D is started for every operation
    result = perform with: ctx, monitoring_delay: @mon_delay do
      d.in die: :kill, delay: @delay
    end
    assert {:error, :noproc} = result
    assert {:ok, [d: 1]} = perform(ctx, d.out(done: 1, acc: []))

    # Should kill service C:
    result = perform with: ctx, monitoring_delay: @mon_delay do
      c.in.in die: :kill, delay: @delay
    end
    assert {:error, :noproc} = result

    # Should kill service B
    result = perform with: ctx, monitoring_delay: @mon_delay do
      b.in die: :kill, delay: @delay
    end
    assert {:error, :noproc} = result

    # Should kill service A:
    result = perform with: ctx, monitoring_delay: @mon_delay do
      a.in die: :kill, delay: @delay
    end
    assert {:error, :noproc} = result

    # Services A, B and C should be dead
    assert {:error, :noproc} = perform(ctx, a.in(done: 1, acc: []))
    assert {:error, :noproc} = perform(ctx, b.in(done: 1, acc: []))
    assert {:error, :noproc} = perform(ctx, c.in.in(done: 1, acc: []))
    assert {:error, :noproc} = perform(ctx, c.in.out(done: 1, acc: []))
    assert {:error, :noproc} = perform(ctx, c.out.in(done: 1, acc: []))
  end


  test "complex pipeline with dead process", %{ctx: ctx} do
    assert {:ok, [a: 1]} = perform(ctx, a.in(done: 1, acc: []))
    assert {:ok, [b: 1]} = perform(ctx, b.in(done: 1, acc: []))
    assert {:ok, [c: 1]} = perform(ctx, c.out.in(done: 1, acc: []))
    result = perform with: ctx do
      acc1  = c.out.in  done:  1, acc: []
      acc2  = a.out     done:  2, acc: acc1
      acc3  = d.out     done:  3, acc: acc2
      acc4  = d.in      done:  4, acc: acc3
      acc5  = c.in.in   done:  5, acc: acc4
      acc6  = a.in      die: :kill
    end
    assert {:error, :noproc} = result
    assert {:error, :noproc}  = perform(ctx, a.in(done: 1, acc: []))
    assert {:ok, [b: 1]} = perform(ctx, b.in(done: 1, acc: []))
    assert {:ok, [c: 1]} = perform(ctx, c.out.in(done: 1, acc: []))
    # Restart Service A
    ServiceA.start()

    assert {:ok, [a: 1]} = perform(ctx, a.in(done: 1, acc: []))
    assert {:ok, [b: 1]} = perform(ctx, b.in(done: 1, acc: []))
    assert {:ok, [c: 1]} = perform(ctx, c.out.in(done: 1, acc: []))
    result = perform with: ctx do
      acc1  = c.in.in   done:  1, acc: []
      acc2  = a.out     done:  2, acc: acc1
      acc3  = c.out.in  done:  3, acc: acc2
      acc4  = c.in.out  done:  4, acc: acc3
      acc5  = b.out     done:  5, acc: acc4
      acc6  = b.in      die: :kill
    end
    assert {:error, :noproc} = result
    assert {:ok, [a: 1]} = perform(ctx, a.in(done: 1, acc: []))
    assert {:error, :noproc} = perform(ctx, b.in(done: 1, acc: []))
    assert {:ok, [c: 1]} = perform(ctx, c.out.in(done: 1, acc: []))
    # Restart Service B
    ServiceB.start()

    assert {:ok, [a: 1]} = perform(ctx, a.in(done: 1, acc: []))
    assert {:ok, [b: 1]} = perform(ctx, b.in(done: 1, acc: []))
    assert {:ok, [c: 1]} = perform(ctx, c.out.in(done: 1, acc: []))
    result = perform with: ctx do
      acc1  = d.in      done:  1, acc: []
      acc2  = d.out     done:  2, acc: acc1
      acc3  = c.in.in   done:  3, acc: acc2
      acc4  = b.in      done:  4, acc: acc3
      acc5  = a.in      done:  5, acc: acc4
      acc6  = c.out.in  die: :kill
    end
    assert {:error, :noproc} = result
    assert {:ok, [a: 1]} = perform(ctx, a.in(done: 1, acc: []))
    assert {:ok, [b: 1]} = perform(ctx, b.in(done: 1, acc: []))
    assert {:error, :noproc} = perform(ctx, c.out.in(done: 1, acc: []))
    # Restart Service C
    ServiceC.start()

    assert {:ok, [a: 1]} = perform(ctx, a.in(done: 1, acc: []))
    assert {:ok, [b: 1]} = perform(ctx, b.in(done: 1, acc: []))
    assert {:ok, [c: 1]} = perform(ctx, c.out.in(done: 1, acc: []))
    result = perform with: ctx do
      acc1  = c.out.in  done:  1, acc: []
      acc2  = a.out     done:  2, acc: acc1
      acc3  = d.in      done:  3, acc: acc2
      acc4  = c.out.out done:  4, acc: acc3
      acc5  = d.out     done:  5, acc: acc4
      acc6  = c.in.in   die: :kill
    end
    assert {:error, :noproc} = result
    assert {:ok, [a: 1]} = perform(ctx, a.in(done: 1, acc: []))
    assert {:ok, [b: 1]} = perform(ctx, b.in(done: 1, acc: []))
    assert {:error, :noproc} = perform(ctx, c.out.in(done: 1, acc: []))
    # Restart Service C
    ServiceC.start()

    assert {:ok, [a: 1]} = perform(ctx, a.in(done: 1, acc: []))
    assert {:ok, [b: 1]} = perform(ctx, b.in(done: 1, acc: []))
    assert {:ok, [c: 1]} = perform(ctx, c.out.in(done: 1, acc: []))
    result = perform with: ctx do
      acc1  = a.in      done:  1, acc: []
      acc2  = d.out     done:  2, acc: acc1
      acc3  = c.in.out  done:  3, acc: acc2
      acc4  = c.out.in  done:  4, acc: acc3
      acc5  = b.out     done:  5, acc: acc4
      acc6  = d.in      die: :kill
    end
    assert {:error, :noproc} = result
    assert {:ok, [a: 1]} = perform(ctx, a.in(done: 1, acc: []))
    assert {:ok, [b: 1]} = perform(ctx, b.in(done: 1, acc: []))
    assert {:ok, [c: 1]} = perform(ctx, c.out.in(done: 1, acc: []))
  end


  test "pipeline timeout", %{ctx: ctx} do
    # Pipeline cannot be completly inline,
    # it would fail with `context_not_discharged`

    result = perform with: ctx, timeout: 200 do
      acc1  = d.in      done:  1, acc: []
      acc2  = d.out     done:  2, acc: acc1
      acc3  = a.in      bug: :lost
    end
    assert {:error, :timeout} = result

    result = perform with: ctx, timeout: 200 do
      acc1  = b.out     done:  1, acc: []
      acc2  = c.out.in  done:  2, acc: acc1
      acc3  = a.out     bug: :lost
    end
    assert {:error, :timeout} = result

    result = perform with: ctx, timeout: 200 do
      acc1  = c.in.out  done:  1, acc: []
      acc2  = c.out.out done:  2, acc: acc1
      acc3  = b.in      bug: :lost
    end
    assert {:error, :timeout} = result

    result = perform with: ctx, timeout: 200 do
      acc1  = a.in      done:  1, acc: []
      acc2  = a.out     done:  2, acc: acc1
      acc3  = c.in.in   bug: :lost
    end
    assert {:error, :timeout} = result

    result = perform with: ctx, timeout: 200 do
      acc1  = d.in      done:  1, acc: []
      acc2  = a.out     done:  2, acc: acc1
      acc3  = c.in.out  bug: :lost
    end
    assert {:error, :timeout} = result

    result = perform with: ctx, timeout: 200 do
      acc1  = c.out.out done:  1, acc: []
      acc2  = d.out     done:  2, acc: acc1
      acc3  = c.out.in  bug: :lost
    end
    assert {:error, :timeout} = result

    result = perform with: ctx, timeout: 200 do
      acc1  = c.out.in  done:  1, acc: []
      acc2  = a.out     done:  2, acc: acc1
      acc3  = c.out.out bug: :lost
    end
    assert {:error, :timeout} = result

    # If the last process the pipeline is delegated to is temporary service D,
    # the pipeline will fail when the process terminate.

    result = perform with: ctx, timeout: 500 do
      acc1  = d.in      done:  1, acc: []
      acc2  = c.out.out done:  2, acc: acc1
      acc3  = b.out     bug: :lost
    end
    assert {:error, :noproc} = result

    result = perform with: ctx, timeout: 500 do
      acc1  = b.in      done:  1, acc: []
      acc2  = c.out.out done:  2, acc: acc1
      acc3  = d.in      bug: :lost
    end
    assert {:error, :noproc} = result

    result = perform with: ctx, timeout: 500 do
      acc1  = b.out     done:  1, acc: []
      acc2  = c.in.in   done:  2, acc: acc1
      acc3  = d.out     bug: :lost
    end
    assert {:error, :noproc} = result
  end

end
