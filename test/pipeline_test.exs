defmodule Convex.PipelineTest do

  #===========================================================================
  # Sub-Modules
  #===========================================================================

  defmodule DummyDirector do
    alias Convex.Context, as: Ctx
    def perform(ctx, [:dummy, :identity], %{value: val}) do
      Ctx.done(ctx, val)
    end
    def perform(ctx, [:dummy, :push], %{stack: stack, value: val}) do
      Ctx.done(ctx, [val | stack])
    end
    def perform(ctx, _op, _args), do: Ctx.failed(ctx, :unknown_operation)
  end


  defmodule DummyStruct do

    @behaviour Access

    defstruct [:spam, :bacon]

    def fetch(this, key), do: Map.fetch(this, key)

    def get(this, key, default), do: Map.get(this, key, default)

    def get_and_update(this, key, fun) do
      value = Map.get(this, key)
      case fun.(value) do
        {get_value, new_value} -> {get_value, Map.put(this, key, new_value)}
        :pop -> {value, Map.put(this, key, nil)}
      end
    end

    def pop(this, key), do: {Map.get(this, key), Map.put(this, key, nil)}

  end


  #===========================================================================
  # Includes
  #===========================================================================

  use ExUnit.Case, async: true

  import Convex.Pipeline

  alias Convex.Context, as: Ctx
  alias Convex.Context.Sync


  #===========================================================================
  # Test Cases
  #===========================================================================

  test "unpacking" do
    ctx = Sync.new(director: DummyDirector)

    result = perform! ctx do
      {a, b, c} = dummy.identity value: {1, 2, 3}
      {d, e} = dummy.identity value: {4, 5}
      {a, b, c, d, e}
    end
    assert {1, 2, 3, 4, 5} = result

    result = perform! ctx do
      {a, _, c} = dummy.identity value: {1, 2, 3}
      {_, e} = dummy.identity value: {4, 5}
      {a, c, e}
    end
    assert {1, 3, 5} = result

    result = perform! ctx do
      [a, b, c] = dummy.identity value: [1, 2, 3]
      [d, e] = dummy.identity value: [4, 5]
      [a, b, c, d, e]
    end
    assert [1, 2, 3, 4, 5] = result

    result = perform! ctx do
      [_, b, _] = dummy.identity value: [1, 2, 3]
      [d, _] = dummy.identity value: [4, 5]
      [b, d]
    end
    assert [2, 4] = result

    result = perform! ctx do
      %{a: a, b: b, c: c} = dummy.identity value: %{a: 1, b: 2, c: 3, x: 42, y: 33}
      %{d: d, e: e} = dummy.identity value: %{d: 4, e: 5, z: 18}
      %{a: a, b: b, c: c, d: d, e: e}
    end
    assert %{a: 1, b: 2, c: 3, d: 4, e: 5} = result

    result = perform! ctx do
      %{a: a, b: _, c: c} = dummy.identity value: %{a: 1, b: 2, c: 3, x: 42, y: 33}
      %{d: _, e: e} = dummy.identity value: %{d: 4, e: 5, z: 18}
      %{a: a, c: c, e: e}
    end
    assert %{a: 1, c: 3, e: 5} = result

    result = perform! ctx do
      {a, [b, c], %{foo: d, bar: {e}}} = dummy.identity value: {1, [2, 3], %{foo: 4, bar: {5}}}
      {a, b, c, d, e}
    end
    assert {1, 2, 3, 4, 5} = result

    result = perform! ctx do
      {a, [_, c], %{foo: _, bar: {e}}} = dummy.identity value: {1, [2, 3], %{foo: 4, bar: {5}}}
      {a, c, e}
    end
    assert {1, 3, 5} = result

    result = perform ctx do
      {a, b, c} = dummy.identity value: {1, 2}
    end
    assert {:error, {:invalid_result, _}} = result
  end


  test "packing" do
    ctx = Sync.new(director: DummyDirector)

    result = perform ctx do
      a = dummy.identity value: 123
      b = dummy.identity value: 456
      {a, b}
    end
    assert {:ok, {123, 456}} = result

    result = perform ctx do
      a = dummy.identity value: 123
      b = dummy.identity value: 456
      c = dummy.identity value: 789
      {a, b, c}
    end
    assert {:ok, {123, 456, 789}} = result

    result = perform ctx do
      a = dummy.identity value: 123
      b = dummy.identity value: 456
      c = dummy.identity value: 789
      [b, a, c]
    end
    assert {:ok, [456, 123, 789]} = result

    result = perform ctx do
      a = dummy.identity value: 123
      b = dummy.identity value: 456
      c = dummy.identity value: 789
      %{a: a, b: b, c: c}
    end
    assert {:ok, %{a: 123, b: 456, c: 789}} = result

    result = perform ctx do
      a = dummy.identity value: 123
      b = dummy.identity value: 456
      c = dummy.identity value: 789
      %{a: {a, b, c}, b: [c, b, a], c: %{e: b, f: a, g: c}}
    end
    assert {:ok, %{a: {123, 456, 789}, b: [789, 456, 123], c: %{e: 456, f: 123, g: 789}}} = result

    result = perform ctx do
      a = dummy.identity value: 123
      b = dummy.identity value: 456
      c = dummy.identity value: 789
      %{"foo" => a, "bar" => b, "buz" => c}
    end
    assert {:ok, %{"foo" => 123, "bar" => 456, "buz" => 789}} = result

    result = perform ctx do
      a = dummy.identity value: 123
      b = dummy.identity value: 456
      c = dummy.identity value: 789
      {:a, a, 42, b, "foo", c, 3.14}
    end
    assert {:ok, {:a, 123, 42, 456, "foo", 789, 3.14}} = result

    result = perform ctx do
      a1 = dummy.identity value: 123
      a2 = [a: a1]
      b1 = dummy.identity value: 456
      b2 = {:b, b1}
      stack = dummy.push stack: a2, value: b2
      c1 = dummy.identity value: 789
      c2 = {:c, c1}
      stack = dummy.push stack: stack, value: c2
    end
    assert {:ok, [c: 789, b: 456, a: 123]} = result
  end


  test "merge arguments" do
    map = %{a: 33, b: 42}
    val = 1
    res = prepare do
      toto = foo.bar
      tata = bar.buz %{^map | b: toto, c: ^val, d: "boz"}
    end
    assert res == [{[:foo, :bar], %{}, [], :toto},
                   {[:bar, :buz], %{c: 1, d: "boz"},
                    [{:store, :b, :toto, []}, {:merge, map}], :tata}]
  end


  test "else statement" do
    res = perform with: :ctx, custom: 1, handler: Convex.PipelineTest.dummy_handler do
      foo
      bar
    else
      1 -> :ok
      _ -> :error
    end
    assert :ok = res

    res = perform with: :ctx, custom: 2, handler: Convex.PipelineTest.dummy_handler do
      foo
      bar
    else
      1 -> :ok
      _ -> :error
    end
    assert :error = res
  end


  test "simple pipeline" do
    map = %{a: 33, b: 42}
    kw = [a: 33, b: 42]

    res = prepare foo
    assert res == [{[:foo], %{}, [], nil}]
    res = prepare foo.bar.buz
    assert res == [{[:foo, :bar, :buz], %{}, [], nil}]
    res = prepare foo.bar()
    assert res == [{[:foo, :bar], %{}, [], nil}]
    res = prepare foo.bar(^map)
    assert res == [{[:foo, :bar], map, [], nil}]
    res = prepare foo.bar(^kw)
    assert res == [{[:foo, :bar], map, [], nil}]
    res = prepare foo.bar(foo: 1)
    assert res == [{[:foo, :bar], %{foo: 1}, [], nil}]
    res = prepare foo.bar(foo: "spam", bar: :bacon)
    assert res == [{[:foo, :bar], %{foo: "spam", bar: :bacon}, [], nil}]
    res = prepare foo.bar(foo: ^map, bar: ^kw)
    assert res == [{[:foo, :bar], %{foo: map, bar: kw}, [], nil}]

    res = prepare [foo.bar, bar.buz(), spam(^map), bacon(^kw), eggs(foo: 1)]
    assert res == [
      {[:foo, :bar], %{}, [], nil},
      {[:bar, :buz], %{}, [], nil},
      {[:spam], map, [], nil},
      {[:bacon], map, [], nil},
      {[:eggs], %{foo: 1}, [], nil}
    ]

    res = prepare do
      foo
      bar()
      spam.bacon(^map)
      bacon.eggs(^kw)
      eggs.spam(foo: ^map, bar: ^kw, buz: 1)
    end
    assert res == [
      {[:foo], %{}, [], nil},
      {[:bar], %{}, [], nil},
      {[:spam, :bacon], map, [], nil},
      {[:bacon, :eggs], map, [], nil},
      {[:eggs, :spam], %{foo: map, bar: kw, buz: 1}, [], nil}
    ]
  end


  test "parametrized pipeline" do
    toto = [1, 2, 3]
    res = prepare do
      a1 = spam.bacon(foo: 1)
      bacon.eggs(bar: ^toto)
      a2 = eggs.spam(foo: 4, buz: a1)
      spam(foo: a1, bar: a2)
      bacon(foo: ctx.auth, bar: ctx.sess)
    end
    assert res == [
      {[:spam, :bacon], %{foo: 1}, [], :a1},
      {[:bacon, :eggs], %{bar: toto}, [], nil},
      {[:eggs, :spam], %{foo: 4}, [{:store, :buz, :a1, []}], :a2},
      {[:spam], %{}, [{:store, :foo, :a1, []}, {:store, :bar, :a2, []}], nil},
      {[:bacon], %{}, [{:ctx, :foo, [:auth]}, {:ctx, :bar, [:sess]}], nil}
    ]
  end


  test "deep operation parameters" do
    res = prepare do
      foo(a: [])
      foo(a: [1, 2, 3])
      foo(a: [1, [:a], 3])
      foo(a: [1, [:a, :b], 3])
      foo(a: [1, [:a, :b, :c], 3])
      foo(a: [1, {:a}, 3])
      foo(a: [1, {:a, :b}, 3])
      foo(a: [1, {:a, :b, :c}, 3])
      foo(a: [1, %{a: :b}, 3])

      foo(a: {})
      foo(a: {1, 2, 3})
      foo(a: {1, [:a], 3})
      foo(a: {1, [:a, :b], 3})
      foo(a: {1, [:a, :b, :c], 3})
      foo(a: {1, {:a}, 3})
      foo(a: {1, {:a, :b}, 3})
      foo(a: {1, {:a, :b, :c}, 3})
      foo(a: {1, %{a: :b}, 3})

      foo(a: %{})
      foo(a: %{x: 1, y: 2, z: 3})
      foo(a: %{x: 1, y: [:a], z: 3})
      foo(a: %{x: 1, y: [:a, :b], z: 3})
      foo(a: %{x: 1, y: [:a, :b, :c], z: 3})
      foo(a: %{x: 1, y: {:a}, z: 3})
      foo(a: %{x: 1, y: {:a, :b}, z: 3})
      foo(a: %{x: 1, y: {:a, :b, :c}, z: 3})
      foo(a: %{x: 1, y: %{a: :b}, z: 3})
    end
    assert res == [
      {[:foo], %{a: []}, [], nil},
      {[:foo], %{a: [1, 2, 3]}, [], nil},
      {[:foo], %{a: [1, [:a], 3]}, [], nil},
      {[:foo], %{a: [1, [:a, :b], 3]}, [], nil},
      {[:foo], %{a: [1, [:a, :b, :c], 3]}, [], nil},
      {[:foo], %{a: [1, {:a}, 3]}, [], nil},
      {[:foo], %{a: [1, {:a, :b}, 3]}, [], nil},
      {[:foo], %{a: [1, {:a, :b, :c}, 3]}, [], nil},
      {[:foo], %{a: [1, %{a: :b}, 3]}, [], nil},

      {[:foo], %{a: {}}, [], nil},
      {[:foo], %{a: {1, 2, 3}}, [], nil},
      {[:foo], %{a: {1, [:a], 3}}, [], nil},
      {[:foo], %{a: {1, [:a, :b], 3}}, [], nil},
      {[:foo], %{a: {1, [:a, :b, :c], 3}}, [], nil},
      {[:foo], %{a: {1, {:a}, 3}}, [], nil},
      {[:foo], %{a: {1, {:a, :b}, 3}}, [], nil},
      {[:foo], %{a: {1, {:a, :b, :c}, 3}}, [], nil},
      {[:foo], %{a: {1, %{a: :b}, 3}}, [], nil},

      {[:foo], %{a: %{}}, [], nil},
      {[:foo], %{a: %{x: 1, y: 2, z: 3}}, [], nil},
      {[:foo], %{a: %{x: 1, y: [:a], z: 3}}, [], nil},
      {[:foo], %{a: %{x: 1, y: [:a, :b], z: 3}}, [], nil},
      {[:foo], %{a: %{x: 1, y: [:a, :b, :c], z: 3}}, [], nil},
      {[:foo], %{a: %{x: 1, y: {:a}, z: 3}}, [], nil},
      {[:foo], %{a: %{x: 1, y: {:a, :b}, z: 3}}, [], nil},
      {[:foo], %{a: %{x: 1, y: {:a, :b, :c}, z: 3}}, [], nil},
      {[:foo], %{a: %{x: 1, y: %{a: :b},z:  3}}, [], nil}
    ]
  end


  test "deep operation references" do
    foo = [1, :spam, "bacon"]
    bar = {2, :eggs, "beans"}
    res = prepare do
      foo(a: [1, ^foo, 3])
      foo(a: [1, [^foo], ^bar])
      foo(a: [1, [^foo, ^bar], 3])
      foo(a: [1, [^foo, :b, ^bar], 3])
      foo(a: [1, {^foo}, ^bar])
      foo(a: [1, {^foo, ^bar}, 3])
      foo(a: [1, {^foo, :b, ^bar}, 3])
      foo(a: [1, %{a: ^foo}, ^bar])

      foo(a: {1, ^foo, 3})
      foo(a: {1, [^foo], ^bar})
      foo(a: {1, [^foo, ^bar], 3})
      foo(a: {1, [^foo, :b, ^bar], 3})
      foo(a: {1, {^foo}, ^bar})
      foo(a: {1, {^foo, ^bar}, 3})
      foo(a: {1, {^foo, :b, ^bar}, 3})
      foo(a: {1, %{a: ^foo}, ^bar})

      foo(a: %{x: 1, y: ^foo, z: 3})
      foo(a: %{x: 1, y: [^foo], z: ^bar})
      foo(a: %{x: 1, y: [^foo, ^bar], z: 3})
      foo(a: %{x: 1, y: [^foo, :b, ^bar], z: 3})
      foo(a: %{x: 1, y: {^foo}, z: ^bar})
      foo(a: %{x: 1, y: {^foo, ^bar}, z: 3})
      foo(a: %{x: 1, y: {^foo, :b, ^bar}, z: 3})
      foo(a: %{x: 1, y: %{a: ^foo}, z: ^bar})
    end
    assert res == [
      {[:foo], %{a: [1, foo, 3]}, [], nil},
      {[:foo], %{a: [1, [foo], bar]}, [], nil},
      {[:foo], %{a: [1, [foo, bar], 3]}, [], nil},
      {[:foo], %{a: [1, [foo, :b, bar], 3]}, [], nil},
      {[:foo], %{a: [1, {foo}, bar]}, [], nil},
      {[:foo], %{a: [1, {foo, bar}, 3]}, [], nil},
      {[:foo], %{a: [1, {foo, :b, bar}, 3]}, [], nil},
      {[:foo], %{a: [1, %{a: foo}, bar]}, [], nil},

      {[:foo], %{a: {1, foo, 3}}, [], nil},
      {[:foo], %{a: {1, [foo], bar}}, [], nil},
      {[:foo], %{a: {1, [foo, bar], 3}}, [], nil},
      {[:foo], %{a: {1, [foo, :b, bar], 3}}, [], nil},
      {[:foo], %{a: {1, {foo}, bar}}, [], nil},
      {[:foo], %{a: {1, {foo, bar}, 3}}, [], nil},
      {[:foo], %{a: {1, {foo, :b, bar}, 3}}, [], nil},
      {[:foo], %{a: {1, %{a: foo}, bar}}, [], nil},

      {[:foo], %{a: %{x: 1, y: foo, z: 3}}, [], nil},
      {[:foo], %{a: %{x: 1, y: [foo], z: bar}}, [], nil},
      {[:foo], %{a: %{x: 1, y: [foo, bar], z: 3}}, [], nil},
      {[:foo], %{a: %{x: 1, y: [foo, :b, bar], z: 3}}, [], nil},
      {[:foo], %{a: %{x: 1, y: {foo}, z: bar}}, [], nil},
      {[:foo], %{a: %{x: 1, y: {foo, bar}, z: 3}}, [], nil},
      {[:foo], %{a: %{x: 1, y: {foo, :b, bar}, z: 3}}, [], nil},
      {[:foo], %{a: %{x: 1, y: %{a: foo}, z: bar}}, [], nil}
    ]
  end


  test "deep context references" do
    policy = %DummyStruct{spam: :a, bacon: :b}
    ctx = Sync.new(director: DummyDirector)
      |> Ctx.authenticate(%{foo: 42, bar: 33}, policy)
      |> Ctx.attach(%{toto: "toto", tata: "tata"})

    result = perform! ctx do
      dummy.identity value: ctx.auth
    end
    assert %{foo: 42, bar: 33} == result

    result = perform! ctx do
      dummy.identity value: ctx.auth.foo
    end
    assert 42 == result

    result = perform! ctx do
      dummy.identity value: ctx.auth.bar
    end
    assert 33 == result

    result = perform! ctx do
      dummy.identity value: ctx.auth.buz
    end
    assert nil == result

    result = perform! ctx do
      dummy.identity value: ctx.sess
    end
    assert %{toto: "toto", tata: "tata"} = result

    result = perform! ctx do
      dummy.identity value: ctx.sess.toto
    end
    assert "toto" == result

    result = perform! ctx do
      dummy.identity value: ctx.sess.tata
    end
    assert "tata" == result

    result = perform! ctx do
      dummy.identity value: ctx.sess.tutu
    end
    assert nil == result

    result = perform! ctx do
      dummy.identity value: ctx.policy
    end
    assert policy == result

    result = perform! ctx do
      dummy.identity value: ctx.policy.spam
    end
    assert :a == result

    result = perform! ctx do
      dummy.identity value: ctx.policy.bacon
    end
    assert :b == result

    result = perform! ctx do
      dummy.identity value: ctx.policy.eggs
    end
    assert nil == result
  end


  test "deep store references" do
    ctx = Sync.new(director: DummyDirector)

    result = perform! ctx do
      val = dummy.identity value: %{a: %{b: %{c: :ok}}}
      dummy.identity value: val.a.b.c
    end
    assert :ok == result

    result = perform! ctx do
      val = dummy.identity value: %{a: %{b: %{c: :ok}}}
      dummy.identity value: val.a.b.x
    end
    assert nil == result

    result = perform! ctx do
      val = dummy.identity value: %{a: %{b: %{c: :ok}}}
      dummy.identity value: val.a.x.c
    end
    assert nil == result

    args = %{value: %{a: %DummyStruct{spam: %{c: :ok}}}}
    result = perform! ctx do
      val = dummy.identity ^args
      dummy.identity value: val.a.spam.c
    end
    assert :ok == result
  end


  test "return single store value" do
    ctx = Sync.new(director: DummyDirector)

    result = perform! ctx do
      val = dummy.identity value: :ok
      val
    end
    assert :ok == result

    result = perform! ctx do
      val = dummy.identity value: %{a: %{b: %{c: :ok}}}
      val.a.b.c
    end
    assert :ok == result

    result = perform! ctx do
      %{a: val} = dummy.identity value: %{a: %{b: %{c: :ok}}}
      val.b.c
    end
    assert :ok == result

    result = perform! ctx do
      val = dummy.identity value: %{a: %{b: %{c: :ok}}}
      val.a.x.c
    end
    assert nil == result
  end


  test "perform as" do
    ctx = Sync.new(director: DummyDirector)

    result = perform! with: ctx, as: "foo" do
      dummy.identity value: ctx.auth
    end
    assert "foo" == result

    result = perform! with: ctx, as: "foo" do
      dummy.identity value: ctx.policy
    end
    assert nil == result

    result = perform! with: ctx, as: "foo", policy: "bar" do
      dummy.identity value: ctx.auth
    end
    assert "foo" == result

    result = perform! with: ctx, as: "foo", policy: "bar" do
      dummy.identity value: ctx.policy
    end
    assert "bar" == result
  end


  #===========================================================================
  # Dummy Handler Functions
  #===============q============================================================

  def dummy_handler(ctx, pipeline, opts) do
    assert :ctx = ctx
    assert [{[:foo], %{}, [], nil}, {[:bar], %{}, [], nil}] = pipeline
    assert Keyword.has_key?(opts, :custom)
    Keyword.get(opts, :custom)
  end

end
