defmodule Convex.ProcessStraightTest do

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

  import Convex.Pipeline, only: [prepare: 1]

  alias Convex.Test.ProcessHandlerProc, as: PH
  alias Convex.Test.ServiceA
  alias Convex.Test.ServiceB
  alias Convex.Test.ServiceC
  alias Convex.ProcessStraightTest.TestDirector


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
    normal = PH.start(director: TestDirector)
    t200 = PH.start(director: TestDirector, timeout: 200)
    t500 = PH.start(director: TestDirector, timeout: 500)
    low = PH.start(director: TestDirector, monitoring_delay: @mon_delay)
    on_exit fn ->
      PH.stop(t200)
      PH.stop(t500)
      PH.stop(normal)
      PH.stop(low)
      ServiceA.stop()
      ServiceB.stop()
      ServiceC.stop()
    end
    {:ok, [normal: normal, low: low, t200: t200, t500: t500]}
  end


  #===========================================================================
  # Test Cases
  #===========================================================================

  test "successful single operation pipelines", %{normal: pid} do
    assert {:done, [a: 1]} = PH.perform pid, prepare a.out(done: 1, acc: [])
    assert {:done, [a: 2]} = PH.perform pid, prepare a.in(done: 2, acc: [])
    assert {:done, [b: 3]} = PH.perform pid, prepare b.out(done: 3, acc: [])
    assert {:done, [b: 4]} = PH.perform pid, prepare b.in(done: 4, acc: [])
    assert {:done, [c: 5]} = PH.perform pid, prepare c.out.out(done: 5, acc: [])
    assert {:done, [c: 6]} = PH.perform pid, prepare c.out.in(done: 6, acc: [])
    assert {:done, [c: 7]} = PH.perform pid, prepare c.in.out(done: 7, acc: [])
    assert {:done, [c: 8]} = PH.perform pid, prepare c.in.in(done: 8, acc: [])
    assert {:done, [d: 9]} = PH.perform pid, prepare d.out(done: 9, acc: [])
    assert {:done, [d: 10]} = PH.perform pid, prepare d.in(done: 10, acc: [])
  end


  test "successful multi service A operation pipelines", %{normal: pid} do
    pipe = prepare do
      acc1 = a.out done: 1, acc: []
      acc2 = a.out done: 2, acc: acc1
             a.out done: 3, acc: acc2
    end
    assert {:done, [a: 3, a: 2, a: 1]} = PH.perform pid, pipe

    pipe = prepare do
      acc1 = a.in  done: 1, acc: []
      acc2 = a.out done: 2, acc: acc1
             a.out done: 3, acc: acc2
    end
    assert {:done, [a: 3, a: 2, a: 1]} = PH.perform pid, pipe

    pipe = prepare do
      acc1 = a.out done: 1, acc: []
      acc2 = a.in  done: 2, acc: acc1
             a.out done: 3, acc: acc2
    end
    assert {:done, [a: 3, a: 2, a: 1]} = PH.perform pid, pipe

    pipe = prepare do
      acc1 = a.out done: 1, acc: []
      acc2 = a.out done: 2, acc: acc1
             a.in  done: 3, acc: acc2
    end
    assert {:done, [a: 3, a: 2, a: 1]} = PH.perform pid, pipe

    pipe = prepare do
      acc1 = a.in  done: 1, acc: []
      acc2 = a.in  done: 2, acc: acc1
             a.out done: 3, acc: acc2
    end
    assert {:done, [a: 3, a: 2, a: 1]} = PH.perform pid, pipe

    pipe = prepare do
      acc1 = a.in  done: 1, acc: []
      acc2 = a.out done: 2, acc: acc1
             a.in  done: 3, acc: acc2
    end
    assert {:done, [a: 3, a: 2, a: 1]} = PH.perform pid, pipe

    pipe = prepare do
      acc1 = a.out done: 1, acc: []
      acc2 = a.in  done: 2, acc: acc1
             a.in  done: 3, acc: acc2
    end
    assert {:done, [a: 3, a: 2, a: 1]} = PH.perform pid, pipe
  end


  test "successful multi service C operation pipelines", %{normal: pid} do
    pipe = prepare do
      acc1 = c.out.out done: 1, acc: []
      acc2 = c.out.out done: 2, acc: acc1
             c.out.out done: 3, acc: acc2
    end
    assert {:done, [c: 3, c: 2, c: 1]} = PH.perform pid, pipe

    pipe = prepare do
      acc1 = c.in.out  done: 1, acc: []
      acc2 = c.out.out done: 2, acc: acc1
             c.out.out done: 3, acc: acc2
    end
    assert {:done, [c: 3, c: 2, c: 1]} = PH.perform pid, pipe

    pipe = prepare do
      acc1 = c.out.out done: 1, acc: []
      acc2 = c.in.out  done: 2, acc: acc1
             c.out.out done: 3, acc: acc2
    end
    assert {:done, [c: 3, c: 2, c: 1]} = PH.perform pid, pipe

    pipe = prepare do
      acc1 = c.out.out done: 1, acc: []
      acc2 = c.out.out done: 2, acc: acc1
             c.in.out  done: 3, acc: acc2
    end
    assert {:done, [c: 3, c: 2, c: 1]} = PH.perform pid, pipe

    pipe = prepare do
      acc1 = c.in.out  done: 1, acc: []
      acc2 = c.in.out  done: 2, acc: acc1
             c.out.out done: 3, acc: acc2
    end
    assert {:done, [c: 3, c: 2, c: 1]} = PH.perform pid, pipe

    pipe = prepare do
      acc1 = c.in.out  done: 1, acc: []
      acc2 = c.out.out done: 2, acc: acc1
             c.in.out  done: 3, acc: acc2
    end
    assert {:done, [c: 3, c: 2, c: 1]} = PH.perform pid, pipe

    pipe = prepare do
      acc1 = c.out.out done: 1, acc: []
      acc2 = c.in.out  done: 2, acc: acc1
             c.in.out  done: 3, acc: acc2
    end
    assert {:done, [c: 3, c: 2, c: 1]} = PH.perform pid, pipe

    pipe = prepare do
      acc1 = c.out.in  done: 1, acc: []
      acc2 = c.out.in  done: 2, acc: acc1
             c.out.in  done: 3, acc: acc2
    end
    assert {:done, [c: 3, c: 2, c: 1]} = PH.perform pid, pipe

    pipe = prepare do
      acc1 = c.in.in   done: 1, acc: []
      acc2 = c.out.in  done: 2, acc: acc1
             c.out.in  done: 3, acc: acc2
    end
    assert {:done, [c: 3, c: 2, c: 1]} = PH.perform pid, pipe

    pipe = prepare do
      acc1 = c.out.in  done: 1, acc: []
      acc2 = c.in.in   done: 2, acc: acc1
             c.out.in  done: 3, acc: acc2
    end
    assert {:done, [c: 3, c: 2, c: 1]} = PH.perform pid, pipe

    pipe = prepare do
      acc1 = c.out.in  done: 1, acc: []
      acc2 = c.out.in  done: 2, acc: acc1
             c.in.in   done: 3, acc: acc2
    end
    assert {:done, [c: 3, c: 2, c: 1]} = PH.perform pid, pipe

    pipe = prepare do
      acc1 = c.in.in   done: 1, acc: []
      acc2 = c.in.in   done: 2, acc: acc1
             c.out.in  done: 3, acc: acc2
    end
    assert {:done, [c: 3, c: 2, c: 1]} = PH.perform pid, pipe

    pipe = prepare do
      acc1 = c.in.in   done: 1, acc: []
      acc2 = c.out.in  done: 2, acc: acc1
             c.in.in   done: 3, acc: acc2
    end
    assert {:done, [c: 3, c: 2, c: 1]} = PH.perform pid, pipe

    pipe = prepare do
      acc1 = c.out.in  done: 1, acc: []
      acc2 = c.in.in   done: 2, acc: acc1
             c.in.in   done: 3, acc: acc2
    end
    assert {:done, [c: 3, c: 2, c: 1]} = PH.perform pid, pipe
  end


  test "successful multi service D operation pipelines", %{normal: pid} do
    pipe = prepare do
      acc1 = d.out done: 1, acc: []
      acc2 = d.out done: 2, acc: acc1
             d.out done: 3, acc: acc2
    end
    assert {:done, [d: 3, d: 2, d: 1]} = PH.perform pid, pipe

    pipe = prepare do
      acc1 = d.in  done: 1, acc: []
      acc2 = d.out done: 2, acc: acc1
             d.out done: 3, acc: acc2
    end
    assert {:done, [d: 3, d: 2, d: 1]} = PH.perform pid, pipe

    pipe = prepare do
      acc1 = d.out done: 1, acc: []
      acc2 = d.in  done: 2, acc: acc1
             d.out done: 3, acc: acc2
    end
    assert {:done, [d: 3, d: 2, d: 1]} = PH.perform pid, pipe

    pipe = prepare do
      acc1 = d.out done: 1, acc: []
      acc2 = d.out done: 2, acc: acc1
             d.in  done: 3, acc: acc2
    end
    assert {:done, [d: 3, d: 2, d: 1]} = PH.perform pid, pipe

    pipe = prepare do
      acc1 = d.in  done: 1, acc: []
      acc2 = d.in  done: 2, acc: acc1
             d.out done: 3, acc: acc2
    end
    assert {:done, [d: 3, d: 2, d: 1]} = PH.perform pid, pipe

    pipe = prepare do
      acc1 = d.in  done: 1, acc: []
      acc2 = d.out done: 2, acc: acc1
             d.in  done: 3, acc: acc2
    end
    assert {:done, [d: 3, d: 2, d: 1]} = PH.perform pid, pipe

    pipe = prepare do
      acc1 = d.out done: 1, acc: []
      acc2 = d.in  done: 2, acc: acc1
             d.in  done: 3, acc: acc2
    end
    assert {:done, [d: 3, d: 2, d: 1]} = PH.perform pid, pipe
  end


  test "successful multi service A operation pipelines with delay", %{low: pid} do
    pipe = prepare do
      acc1 = a.out delay: @delay, done: 1, acc: []
      acc2 = a.out delay: @delay, done: 2, acc: acc1
             a.out delay: @delay, done: 3, acc: acc2
    end
    assert {:done, [a: 3, a: 2, a: 1]} = PH.perform pid, pipe

    pipe = prepare do
      acc1 = a.in  delay: @delay, done: 1, acc: []
      acc2 = a.out delay: @delay, done: 2, acc: acc1
             a.out delay: @delay, done: 3, acc: acc2
    end
    assert {:done, [a: 3, a: 2, a: 1]} = PH.perform pid, pipe

    pipe = prepare do
      acc1 = a.out delay: @delay, done: 1, acc: []
      acc2 = a.in  delay: @delay, done: 2, acc: acc1
             a.out delay: @delay, done: 3, acc: acc2
    end
    assert {:done, [a: 3, a: 2, a: 1]} = PH.perform pid, pipe

    pipe = prepare do
      acc1 = a.out delay: @delay, done: 1, acc: []
      acc2 = a.out delay: @delay, done: 2, acc: acc1
             a.in  delay: @delay, done: 3, acc: acc2
    end
    assert {:done, [a: 3, a: 2, a: 1]} = PH.perform pid, pipe

    pipe = prepare do
      acc1 = a.in  delay: @delay, done: 1, acc: []
      acc2 = a.in  delay: @delay, done: 2, acc: acc1
             a.out delay: @delay, done: 3, acc: acc2
    end
    assert {:done, [a: 3, a: 2, a: 1]} = PH.perform pid, pipe

    pipe = prepare do
      acc1 = a.in  delay: @delay, done: 1, acc: []
      acc2 = a.out delay: @delay, done: 2, acc: acc1
             a.in  delay: @delay, done: 3, acc: acc2
    end
    assert {:done, [a: 3, a: 2, a: 1]} = PH.perform pid, pipe

    pipe = prepare do
      acc1 = a.out delay: @delay, done: 1, acc: []
      acc2 = a.in  delay: @delay, done: 2, acc: acc1
             a.in  delay: @delay, done: 3, acc: acc2
    end
    assert {:done, [a: 3, a: 2, a: 1]} = PH.perform pid, pipe
  end


  test "successful multi service C operation pipelines with delay", %{low: pid} do
    pipe = prepare do
      acc1 = c.out.out delay: @delay, done: 1, acc: []
      acc2 = c.out.out delay: @delay, done: 2, acc: acc1
             c.out.out delay: @delay, done: 3, acc: acc2
    end
    assert {:done, [c: 3, c: 2, c: 1]} = PH.perform pid, pipe

    pipe = prepare do
      acc1 = c.in.out  delay: @delay, done: 1, acc: []
      acc2 = c.out.out delay: @delay, done: 2, acc: acc1
             c.out.out delay: @delay, done: 3, acc: acc2
    end
    assert {:done, [c: 3, c: 2, c: 1]} = PH.perform pid, pipe

    pipe = prepare do
      acc1 = c.out.out delay: @delay, done: 1, acc: []
      acc2 = c.in.out  delay: @delay, done: 2, acc: acc1
             c.out.out delay: @delay, done: 3, acc: acc2
    end
    assert {:done, [c: 3, c: 2, c: 1]} = PH.perform pid, pipe

    pipe = prepare do
      acc1 = c.out.out delay: @delay, done: 1, acc: []
      acc2 = c.out.out delay: @delay, done: 2, acc: acc1
             c.in.out  delay: @delay, done: 3, acc: acc2
    end
    assert {:done, [c: 3, c: 2, c: 1]} = PH.perform pid, pipe

    pipe = prepare do
      acc1 = c.in.out  delay: @delay, done: 1, acc: []
      acc2 = c.in.out  delay: @delay, done: 2, acc: acc1
             c.out.out delay: @delay, done: 3, acc: acc2
    end
    assert {:done, [c: 3, c: 2, c: 1]} = PH.perform pid, pipe

    pipe = prepare do
      acc1 = c.in.out  delay: @delay, done: 1, acc: []
      acc2 = c.out.out delay: @delay, done: 2, acc: acc1
             c.in.out  delay: @delay, done: 3, acc: acc2
    end
    assert {:done, [c: 3, c: 2, c: 1]} = PH.perform pid, pipe

    pipe = prepare do
      acc1 = c.out.out delay: @delay, done: 1, acc: []
      acc2 = c.in.out  delay: @delay, done: 2, acc: acc1
             c.in.out  delay: @delay, done: 3, acc: acc2
    end
    assert {:done, [c: 3, c: 2, c: 1]} = PH.perform pid, pipe

    pipe = prepare do
      acc1 = c.out.in  delay: @delay, done: 1, acc: []
      acc2 = c.out.in  delay: @delay, done: 2, acc: acc1
             c.out.in  delay: @delay, done: 3, acc: acc2
    end
    assert {:done, [c: 3, c: 2, c: 1]} = PH.perform pid, pipe

    pipe = prepare do
      acc1 = c.in.in   delay: @delay, done: 1, acc: []
      acc2 = c.out.in  delay: @delay, done: 2, acc: acc1
             c.out.in  delay: @delay, done: 3, acc: acc2
    end
    assert {:done, [c: 3, c: 2, c: 1]} = PH.perform pid, pipe

    pipe = prepare do
      acc1 = c.out.in  delay: @delay, done: 1, acc: []
      acc2 = c.in.in   delay: @delay, done: 2, acc: acc1
             c.out.in  delay: @delay, done: 3, acc: acc2
    end
    assert {:done, [c: 3, c: 2, c: 1]} = PH.perform pid, pipe

    pipe = prepare do
      acc1 = c.out.in  delay: @delay, done: 1, acc: []
      acc2 = c.out.in  delay: @delay, done: 2, acc: acc1
             c.in.in   delay: @delay, done: 3, acc: acc2
    end
    assert {:done, [c: 3, c: 2, c: 1]} = PH.perform pid, pipe

    pipe = prepare do
      acc1 = c.in.in   delay: @delay, done: 1, acc: []
      acc2 = c.in.in   delay: @delay, done: 2, acc: acc1
             c.out.in  delay: @delay, done: 3, acc: acc2
    end
    assert {:done, [c: 3, c: 2, c: 1]} = PH.perform pid, pipe

    pipe = prepare do
      acc1 = c.in.in   delay: @delay, done: 1, acc: []
      acc2 = c.out.in  delay: @delay, done: 2, acc: acc1
             c.in.in   delay: @delay, done: 3, acc: acc2
    end
    assert {:done, [c: 3, c: 2, c: 1]} = PH.perform pid, pipe

    pipe = prepare do
      acc1 = c.out.in  delay: @delay, done: 1, acc: []
      acc2 = c.in.in   delay: @delay, done: 2, acc: acc1
             c.in.in   delay: @delay, done: 3, acc: acc2
    end
    assert {:done, [c: 3, c: 2, c: 1]} = PH.perform pid, pipe
  end


  test "successful multi service D operation pipelines with delay", %{low: pid} do
    pipe = prepare do
      acc1 = d.out delay: @delay, done: 1, acc: []
      acc2 = d.out delay: @delay, done: 2, acc: acc1
             d.out delay: @delay, done: 3, acc: acc2
    end
    assert {:done, [d: 3, d: 2, d: 1]} = PH.perform pid, pipe

    pipe = prepare do
      acc1 = d.in  delay: @delay, done: 1, acc: []
      acc2 = d.out delay: @delay, done: 2, acc: acc1
             d.out delay: @delay, done: 3, acc: acc2
    end
    assert {:done, [d: 3, d: 2, d: 1]} = PH.perform pid, pipe

    pipe = prepare do
      acc1 = d.out delay: @delay, done: 1, acc: []
      acc2 = d.in  delay: @delay, done: 2, acc: acc1
             d.out delay: @delay, done: 3, acc: acc2
    end
    assert {:done, [d: 3, d: 2, d: 1]} = PH.perform pid, pipe

    pipe = prepare do
      acc1 = d.out delay: @delay, done: 1, acc: []
      acc2 = d.out delay: @delay, done: 2, acc: acc1
             d.in  delay: @delay, done: 3, acc: acc2
    end
    assert {:done, [d: 3, d: 2, d: 1]} = PH.perform pid, pipe

    pipe = prepare do
      acc1 = d.in  delay: @delay, done: 1, acc: []
      acc2 = d.in  delay: @delay, done: 2, acc: acc1
             d.out delay: @delay, done: 3, acc: acc2
    end
    assert {:done, [d: 3, d: 2, d: 1]} = PH.perform pid, pipe

    pipe = prepare do
      acc1 = d.in  delay: @delay, done: 1, acc: []
      acc2 = d.out delay: @delay, done: 2, acc: acc1
             d.in  delay: @delay, done: 3, acc: acc2
    end
    assert {:done, [d: 3, d: 2, d: 1]} = PH.perform pid, pipe

    pipe = prepare do
      acc1 = d.out delay: @delay, done: 1, acc: []
      acc2 = d.in  delay: @delay, done: 2, acc: acc1
             d.in  delay: @delay, done: 3, acc: acc2
    end
    assert {:done, [d: 3, d: 2, d: 1]} = PH.perform pid, pipe
  end


  test "successful complex pipelines", %{normal: pid} do
    pipe = prepare do
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
    result = PH.perform pid, pipe
    assert {:done, [a: 20, c: 19, d: 18, d: 17, a: 16, c: 15,
                    c: 14, c: 13, c: 12, c: 11, a: 10, d: 9,
                    c: 8, c: 7, b: 6, b: 5, d: 4, b: 3, b: 2, a: 1]} = result

    pipe = prepare do
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
    result = PH.perform pid, pipe
    assert {:done, [b: 20, a: 19, a: 18, d: 17, d: 16, c: 15,
                    b: 14, c: 13, b: 12, c: 11, a: 10, d: 9,
                    d: 8, c: 7, c: 6, a: 5, c: 4, c: 3, b: 2, c: 1]} = result
  end


  test "successful complex pipelines with delay", %{low: pid} do
    pipe = prepare do
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
    result = PH.perform pid, pipe
    assert {:done, [b: 20, a: 19, a: 18, c: 17, b: 16, c: 15,
                    d: 14, c: 13, c: 12, a: 11, c: 10, c: 9,
                    a: 8, c: 7, c: 6, b: 5, b: 4, d: 3, d: 2, d: 1]} = result

    pipe = prepare do
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
    result = PH.perform pid, pipe
    assert {:done, [c: 20, b: 19, d: 18, d: 17, a: 16, c: 15,
                    a: 14, c: 13, b: 12, c: 11, b: 10, c: 9,
                    b: 8, c: 7, d: 6, d: 5, a: 4, c: 3, a: 2, c: 1]} = result
  end


  test "failed single operation pipelines", %{normal: pid} do
    assert {:failed, :foo0} = PH.perform pid, prepare a.out(fail: :foo0)
    assert {:failed, :foo1} = PH.perform pid, prepare a.in(fail: :foo1)
    assert {:failed, :foo2} = PH.perform pid, prepare b.out(fail: :foo2)
    assert {:failed, :foo3} = PH.perform pid, prepare b.in(fail: :foo3)
    assert {:failed, :foo4} = PH.perform pid, prepare c.out.out(fail: :foo4)
    assert {:failed, :foo5} = PH.perform pid, prepare c.out.in(fail: :foo5)
    assert {:failed, :foo6} = PH.perform pid, prepare c.in.out(fail: :foo6)
    assert {:failed, :foo7} = PH.perform pid, prepare c.in.in(fail: :foo7)
    assert {:failed, :foo8} = PH.perform pid, prepare d.out(fail: :foo8)
    assert {:failed, :foo9} = PH.perform pid, prepare d.in(fail: :foo9)
  end


  test "failed multi operation pipelines", %{normal: pid} do
    pipe = prepare do
      acc1  = c.in.in   done:  1, acc: []
      acc2  = d.out     done:  2, acc: acc1
      acc3  = b.in      fail: :foo0
    end
    assert {:failed, :foo0} = PH.perform pid, pipe

    pipe = prepare do
      acc1  = c.in.out  done:  1, acc: []
      acc2  = c.out.in  done:  2, acc: acc1
      acc3  = d.in      fail: :foo1
    end
    assert {:failed, :foo1} = PH.perform pid, pipe

    pipe = prepare do
      acc1  = a.in      done:  1, acc: []
      acc2  = c.out.out done:  2, acc: acc1
      acc3  = c.in.out  fail: :foo2
    end
    assert {:failed, :foo2} = PH.perform pid, pipe

    pipe = prepare do
      acc1  = a.out     done:  1, acc: []
      acc2  = c.in.in   done:  2, acc: acc1
      acc3  = c.out.in  fail: :foo3
    end
    assert {:failed, :foo3} = PH.perform pid, pipe

    pipe = prepare do
      acc1  = c.out.in  done:  1, acc: []
      acc2  = d.out     done:  2, acc: acc1
      acc3  = a.in      fail: :foo4
    end
    assert {:failed, :foo4} = PH.perform pid, pipe

    pipe = prepare do
      acc1  = d.in      done:  1, acc: []
      acc2  = b.out     done:  2, acc: acc1
      acc3  = c.out.out fail: :foo5
    end
    assert {:failed, :foo5} = PH.perform pid, pipe

    pipe = prepare do
      acc1  = b.out     done:  1, acc: []
      acc2  = a.in      done:  2, acc: acc1
      acc3  = d.out     fail: :foo6
    end
    assert {:failed, :foo6} = PH.perform pid, pipe

    pipe = prepare do
      acc1  = a.in      done:  1, acc: []
      acc2  = a.out     done:  2, acc: acc1
      acc3  = b.out     fail: :foo7
    end
    assert {:failed, :foo7} = PH.perform pid, pipe

    pipe = prepare do
      acc1  = c.out.in  done:  1, acc: []
      acc2  = b.out     done:  2, acc: acc1
      acc3  = a.out     fail: :foo8
    end
    assert {:failed, :foo8} = PH.perform pid, pipe

    pipe = prepare do
      acc1  = a.out     done:  1, acc: []
      acc2  = d.in      done:  2, acc: acc1
      acc3  = c.in.in   fail: :foo9
    end
    assert {:failed, :foo9} = PH.perform pid, pipe

    pipe = prepare do
      acc1  = c.in.out  done:  1, acc: []
      acc2  = b.in      done:  2, acc: acc1
      acc3  = d.out     delegate_fail: :foo10
    end
    assert {:failed, :foo10} = PH.perform pid, pipe

    pipe = prepare do
      acc1  = a.in      done:  1, acc: []
      acc2  = c.out.in  done:  2, acc: acc1
      acc3  = d.in      delegate_fail: :foo11
    end
    assert {:failed, :foo11} = PH.perform pid, pipe
  end


  test "single operation pipeline with killed process", %{normal: pid} do
    # Service D is started for every operation
    assert {:failed, :noproc} = PH.perform pid, prepare(d.in(die: :kill))
    assert {:done, [d: 1]} = PH.perform pid, prepare(d.out(done: 1, acc: []))

    # Should kill service C:
    assert {:failed, :noproc} = PH.perform pid, prepare(c.in.in(die: :kill))

    # Should kill service B
    assert {:failed, :noproc} = PH.perform pid, prepare(b.in(die: :kill))

    # Should kill service A:
    assert {:failed, :noproc} = PH.perform pid, prepare(a.in(die: :kill))

    # Services A, B and C should be dead
    assert {:failed, :noproc} = PH.perform pid, prepare(a.in(done: 1, acc: []))
    assert {:failed, :noproc} = PH.perform pid, prepare(b.in(done: 1, acc: []))
    assert {:failed, :noproc} = PH.perform pid, prepare(c.in.in(done: 1, acc: []))
    assert {:failed, :noproc} = PH.perform pid, prepare(c.in.out(done: 1, acc: []))
    assert {:failed, :noproc} = PH.perform pid, prepare(c.out.in(done: 1, acc: []))
  end


  test "single operation pipeline with killd process (inline)", %{normal: pid} do
    # Should kill service A:
    assert {:failed, :noproc} = PH.perform pid, prepare(c.in.out(die: :kill))
    assert {:failed, :noproc} = PH.perform pid, prepare(a.in(done: 1, acc: []))

    # Service C should still be running
    assert {:done, [c: 1]} = PH.perform pid, prepare(c.out.in(done: 1, acc: []))

    # Should kill service C:
    assert {:failed, :noproc} = PH.perform pid, prepare(c.out.in(die: :kill))
    assert {:failed, :noproc} = PH.perform pid, prepare(c.out.in(done: 1, acc: []))
  end


  test "single operation pipeline with killd process with delay", %{low: lpid, normal: npid} do
    # Service D is started for every operation
    pipe = prepare do
      d.in die: :kill, delay: @delay
    end
    assert {:failed, :noproc} = PH.perform lpid, pipe
    assert {:done, [d: 1]} = PH.perform npid, prepare(d.out(done: 1, acc: []))

    # Should kill service C:
    pipe = prepare do
      c.in.in die: :kill, delay: @delay
    end
    assert {:failed, :noproc} = PH.perform lpid, pipe

    # Should kill service B
    pipe = prepare do
      b.in die: :kill, delay: @delay
    end
    assert {:failed, :noproc} = PH.perform lpid, pipe

    # Should kill service A:
    pipe = prepare do
      a.in die: :kill, delay: @delay
    end
    assert {:failed, :noproc} = PH.perform lpid, pipe

    # Services A, B and C should be dead
    assert {:failed, :noproc} = PH.perform npid, prepare(a.in(done: 1, acc: []))
    assert {:failed, :noproc} = PH.perform npid, prepare(b.in(done: 1, acc: []))
    assert {:failed, :noproc} = PH.perform npid, prepare(c.in.in(done: 1, acc: []))
    assert {:failed, :noproc} = PH.perform npid, prepare(c.in.out(done: 1, acc: []))
    assert {:failed, :noproc} = PH.perform npid, prepare(c.out.in(done: 1, acc: []))
  end


  test "complex pipeline with dead process", %{normal: pid} do
    assert {:done, [a: 1]} = PH.perform pid, prepare(a.in(done: 1, acc: []))
    assert {:done, [b: 1]} = PH.perform pid, prepare(b.in(done: 1, acc: []))
    assert {:done, [c: 1]} = PH.perform pid, prepare(c.out.in(done: 1, acc: []))
    pipe = prepare do
      acc1  = c.out.in  done:  1, acc: []
      acc2  = a.out     done:  2, acc: acc1
      acc3  = d.out     done:  3, acc: acc2
      acc4  = d.in      done:  4, acc: acc3
      acc5  = c.in.in   done:  5, acc: acc4
      acc6  = a.in      die: :kill
    end
    assert {:failed, :noproc} = PH.perform pid, pipe
    assert {:failed, :noproc}  = PH.perform pid, prepare(a.in(done: 1, acc: []))
    assert {:done, [b: 1]} = PH.perform pid, prepare(b.in(done: 1, acc: []))
    assert {:done, [c: 1]} = PH.perform pid, prepare(c.out.in(done: 1, acc: []))
    # Restart Service A
    ServiceA.start()

    assert {:done, [a: 1]} = PH.perform pid, prepare(a.in(done: 1, acc: []))
    assert {:done, [b: 1]} = PH.perform pid, prepare(b.in(done: 1, acc: []))
    assert {:done, [c: 1]} = PH.perform pid, prepare(c.out.in(done: 1, acc: []))
    pipe = prepare do
      acc1  = c.in.in   done:  1, acc: []
      acc2  = a.out     done:  2, acc: acc1
      acc3  = c.out.in  done:  3, acc: acc2
      acc4  = c.in.out  done:  4, acc: acc3
      acc5  = b.out     done:  5, acc: acc4
      acc6  = b.in      die: :kill
    end
    assert {:failed, :noproc} = PH.perform pid, pipe
    assert {:done, [a: 1]} = PH.perform pid, prepare(a.in(done: 1, acc: []))
    assert {:failed, :noproc} = PH.perform pid, prepare(b.in(done: 1, acc: []))
    assert {:done, [c: 1]} = PH.perform pid, prepare(c.out.in(done: 1, acc: []))
    # Restart Service B
    ServiceB.start()

    assert {:done, [a: 1]} = PH.perform pid, prepare(a.in(done: 1, acc: []))
    assert {:done, [b: 1]} = PH.perform pid, prepare(b.in(done: 1, acc: []))
    assert {:done, [c: 1]} = PH.perform pid, prepare(c.out.in(done: 1, acc: []))
    pipe = prepare do
      acc1  = d.in      done:  1, acc: []
      acc2  = d.out     done:  2, acc: acc1
      acc3  = c.in.in   done:  3, acc: acc2
      acc4  = b.in      done:  4, acc: acc3
      acc5  = a.in      done:  5, acc: acc4
      acc6  = c.out.in  die: :kill
    end
    assert {:failed, :noproc} = PH.perform pid, pipe
    assert {:done, [a: 1]} = PH.perform pid, prepare(a.in(done: 1, acc: []))
    assert {:done, [b: 1]} = PH.perform pid, prepare(b.in(done: 1, acc: []))
    assert {:failed, :noproc} = PH.perform pid, prepare(c.out.in(done: 1, acc: []))
    # Restart Service C
    ServiceC.start()

    assert {:done, [a: 1]} = PH.perform pid, prepare(a.in(done: 1, acc: []))
    assert {:done, [b: 1]} = PH.perform pid, prepare(b.in(done: 1, acc: []))
    assert {:done, [c: 1]} = PH.perform pid, prepare(c.out.in(done: 1, acc: []))
    pipe = prepare do
      acc1  = c.out.in  done:  1, acc: []
      acc2  = a.out     done:  2, acc: acc1
      acc3  = d.in      done:  3, acc: acc2
      acc4  = c.out.out done:  4, acc: acc3
      acc5  = d.out     done:  5, acc: acc4
      acc6  = c.in.in   die: :kill
    end
    assert {:failed, :noproc} = PH.perform pid, pipe
    assert {:done, [a: 1]} = PH.perform pid, prepare(a.in(done: 1, acc: []))
    assert {:done, [b: 1]} = PH.perform pid, prepare(b.in(done: 1, acc: []))
    assert {:failed, :noproc} = PH.perform pid, prepare(c.out.in(done: 1, acc: []))
    # Restart Service C
    ServiceC.start()

    assert {:done, [a: 1]} = PH.perform pid, prepare(a.in(done: 1, acc: []))
    assert {:done, [b: 1]} = PH.perform pid, prepare(b.in(done: 1, acc: []))
    assert {:done, [c: 1]} = PH.perform pid, prepare(c.out.in(done: 1, acc: []))
    pipe = prepare do
      acc1  = a.in      done:  1, acc: []
      acc2  = d.out     done:  2, acc: acc1
      acc3  = c.in.out  done:  3, acc: acc2
      acc4  = c.out.in  done:  4, acc: acc3
      acc5  = b.out     done:  5, acc: acc4
      acc6  = d.in      die: :kill
    end
    assert {:failed, :noproc} = PH.perform pid, pipe
    assert {:done, [a: 1]} = PH.perform pid, prepare(a.in(done: 1, acc: []))
    assert {:done, [b: 1]} = PH.perform pid, prepare(b.in(done: 1, acc: []))
    assert {:done, [c: 1]} = PH.perform pid, prepare(c.out.in(done: 1, acc: []))
  end


  test "pipeline timeout", %{t200: t200, t500: t500} do
    # Pipeline cannot be completly inline,
    # it would fail with `context_not_discharged`

    pipe = prepare do
      acc1  = d.in      done:  1, acc: []
      acc2  = d.out     done:  2, acc: acc1
      acc3  = a.in      bug: :lost
    end
    assert {:failed, :timeout} = PH.perform t200, pipe

    pipe = prepare do
      acc1  = b.out     done:  1, acc: []
      acc2  = c.out.in  done:  2, acc: acc1
      acc3  = a.out     bug: :lost
    end
    assert {:failed, :timeout} = PH.perform t200, pipe

    pipe = prepare do
      acc1  = c.in.out  done:  1, acc: []
      acc2  = c.out.out done:  2, acc: acc1
      acc3  = b.in      bug: :lost
    end
    assert {:failed, :timeout} = PH.perform t200, pipe

    pipe = prepare do
      acc1  = a.in      done:  1, acc: []
      acc2  = a.out     done:  2, acc: acc1
      acc3  = c.in.in   bug: :lost
    end
    assert {:failed, :timeout} = PH.perform t200, pipe

    pipe = prepare do
      acc1  = d.in      done:  1, acc: []
      acc2  = a.out     done:  2, acc: acc1
      acc3  = c.in.out  bug: :lost
    end
    assert {:failed, :timeout} = PH.perform t200, pipe

    pipe = prepare do
      acc1  = c.out.out done:  1, acc: []
      acc2  = d.out     done:  2, acc: acc1
      acc3  = c.out.in  bug: :lost
    end
    assert {:failed, :timeout} = PH.perform t200, pipe

    pipe = prepare do
      acc1  = c.out.in  done:  1, acc: []
      acc2  = a.out     done:  2, acc: acc1
      acc3  = c.out.out bug: :lost
    end
    assert {:failed, :timeout} = PH.perform t200, pipe

    # If the last process the pipeline is delegated to is temporary service D,
    # the pipeline will fail when the process terminate.

    pipe = prepare do
      acc1  = d.in      done:  1, acc: []
      acc2  = c.out.out done:  2, acc: acc1
      acc3  = b.out     bug: :lost
    end
    assert {:failed, :noproc} = PH.perform t500, pipe

    pipe = prepare do
      acc1  = b.in      done:  1, acc: []
      acc2  = c.out.out done:  2, acc: acc1
      acc3  = d.in      bug: :lost
    end
    assert {:failed, :noproc} = PH.perform t500, pipe

    pipe = prepare do
      acc1  = b.out     done:  1, acc: []
      acc2  = c.in.in   done:  2, acc: acc1
      acc3  = d.out     bug: :lost
    end
    assert {:failed, :noproc} = PH.perform t500, pipe
  end

end
