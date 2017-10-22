defmodule Convex.DirectorTest do

  #===========================================================================
  # Sub-Modules
  #===========================================================================

  defmodule Error do
    defexception [:message]
  end

  defmodule DummyDirector do
    use Convex.Director

    perform op = "handler04.*", args, do: {:done, {op, args}}
    perform "*" = op, args, do: {:done, {op, args}}
  end


  defmodule BasicDirector do
    use Convex.Director

    perform "basic.handler1.*", do: {:done, :handler_basic_1}

    perform "basic.handler2.spam", do: {:failed, :handler_basic_2}

    perform "basic.handler3", do: :done
  end

  defmodule ParamDirector do
    use Convex.Director
    alias Convex.OperationError
    alias Convex.Context, as: Ctx

    perform "param.handler1.*", a: 1, do: {:done, :handler_param_1_a}

    perform "param.handler1.*", a: 2 do
      {:done, :handler_param_1_b}
    end

    perform "param.handler1.*", %{a: 3}, do: {:done, :handler_param_1_c}

    perform "param.handler1.*", %{a: 4} do
      {:done, :handler_param_1_d}
    end

    perform "param.handler2.*", %Ctx{auth: true}, a: 1 do
      {:done, :handler_param_2_a}
    end

    perform "param.handler2.*", _ctx, a: 1 do
      {:done, :handler_param_2_b}
    end

    perform "param.handler2.*", %Ctx{auth: true}, %{a: 2} do
      {:done, :handler_param_2_c}
    end

    perform "param.handler2.*", _ctx, a: 2 do
      {:done, :handler_param_2_d}
    end

    perform "param.handler2.*", %Ctx{auth: auth}, %{a: 3} do
      {:done, {:handler_param_2_e, auth}}
    end

    perform "param.handler2.*", %Ctx{auth: auth}, %{a: 4}, do: {:done, {:handler_param_2_f, auth}}

    perform "param.handler2.*", %Ctx{auth: true} = ctx, %{a: 5} do
      {:done, {:handler_param_2_g, ctx.sess}}
    end

    perform "param.handler2.*", %Ctx{auth: true} = ctx, %{a: 6}, do: {:done, {:handler_param_2_h, ctx.sess}}

    perform "param.handler2.*", %Ctx{auth: auth}, %{a: 7}, do: {:done, {:handler_param_2_i, auth}}


    perform "param.handler2.*", %Ctx{auth: true}, a: 8, do: {:done, :handler_param_2_j}

    perform "param.handler2.*", _ctx, a: 8, do: {:done, :handler_param_2_k}

    def perform(ctx, [:param, :handler3], %{a: a})
      when is_binary(a) do
        Ctx.done(ctx, {:handler_param_3, String.upcase(a)})
    end

    def perform(ctx, [:param, :handler4], %{a: a}) when is_binary(a), do: Ctx.done(ctx, {:handler_param_4, String.upcase(a)})

    def perform(ctx, [:param, :handler5], %{a: a}) do
        Ctx.done(ctx, {:handler_param_5, a})
    end

    def perform(ctx, [:param, :handler6], %{a: a}), do: Ctx.done(ctx, {:handler_param_6, a})

    perform "param.handler7.spam", action: action do
      case action do
        :noresult -> :done
        :ok -> {:done, :handler_param_7}
        :error -> {:failed, :handler_param_7_a}
        :raise ->
          raise OperationError,
            message: "Handler 7 Error",
            reason: :handler_param_7_b
      end
    end

    perform "param.handler8", a: %{b: %{c: [{_, _}, {d}]}} do
      {:done, {:handler_param_8, d}}
    end
  end

  defmodule MainDirector do
    use Convex.Director
    alias Convex.DirectorTest.BasicDirector

    delegate "basic.*", BasicDirector
    delegate "param.*", Convex.DirectorTest.ParamDirector
  end

  defmodule StrictValidateDirector do
    use Convex.Director
    alias Convex.DirectorTest.DummyDirector
    alias Convex.OperationError
    alias Convex.Context, as: Ctx

    @enforce_validation true

    delegate "*", DummyDirector

    validate "handler01", do: :ok
    validate "handler02", do: {:error, :handler02_error}
    validate "handler03", do: raise OperationError,
      message: "Handler 3 Error", reason: :handler03_error

    validate "handler04.*", args, do: Map.put(args, :x, :foo)
    validate "handler05.*", _args, do: {:error, :handler05_error}
    validate "handler06.*", _args, do: raise OperationError,
      message: "Handler 6 Error", reason: :handler06_error

    validate "handler07.*", %{} = args, do: Map.put(args, :x, :bar)
    validate "handler08.*", %{}, do: {:error, :handler08_error}
    validate "handler09.*", %{}, do: raise OperationError,
      message: "Handler 9 Error", reason: :handler09_error

    validate "handler10", %{a: 1}, do: :ok
    validate "handler10", %{a: 2} = args, do: Map.put(args, :x, :buz)
    validate "handler10", %{a: 3}, do: {:error, :handler10_error_1_a}
    validate "handler10", %{a: _}, do: raise OperationError,
      message: "Handler 10 Error", reason: :handler10_error_1_b
    validate "handler10", _, do: {:error, :handler10_error_2}

    validate "handler11", a: 1, do: :ok
    validate "handler11", a: 2, do: {:error, :handler11_error_1_a}
    validate "handler11", a: _, do: raise OperationError,
      message: "Handler 11 Error", reason: :handler11_error_1_b
    validate "handler11", _, do: {:error, :handler11_error_2}

    validate "handler12", %Ctx{auth: true} = ctx, %{a: 1} = args, do: Map.put(args, :x, ctx.sess)
    validate "handler12", %Ctx{}, %{a: 1}, do: {:error, :handler12_error_a}
    validate "handler12", %Ctx{auth: nil}, %{a: 2}, do: :ok
    validate "handler12", _, %{a: 2}, do: raise OperationError,
      message: "Handler 12 Error", reason: :handler12_error_b

    validate "handler13", %Ctx{auth: true}, a: 1, do: :ok
    validate "handler13", %Ctx{}, a: 1, do: {:error, :handler13_error_a}
    validate "handler13", %Ctx{auth: nil}, a: 2, do: :ok
    validate "handler13", _, a: 2, do: raise OperationError,
      message: "Handler 13 Error", reason: :handler13_error_b

    validate "handler14", %{action: action} = args do
      case action do
        :ok -> :ok
        :args -> args
        :ok_args -> {:ok, args}
        :error -> {:error, :handler14_error_a}
        :raise ->
          raise OperationError,
            message: "Handler 14 Error", reason: :handler14_error_b
      end
    end
  end

  defmodule LaxValidateDirector1 do
    use Convex.Director
    alias Convex.DirectorTest.DummyDirector
    alias Convex.OperationError
    alias Convex.Context, as: Ctx

    delegate "*", DummyDirector

    validate "handler01", do: :ok
    validate "handler02", do: {:error, :handler02_error}
    validate "handler03", do: raise OperationError,
      message: "Handler 3 Error", reason: :handler03_error

    validate "handler04.*", args, do: Map.put(args, :x, :foo)
    validate "handler05.*", _args, do: {:error, :handler05_error}
    validate "handler06.*", _args, do: raise OperationError,
      message: "Handler 6 Error", reason: :handler06_error

    validate "handler07.*", %{} = args, do: Map.put(args, :x, :bar)
    validate "handler08.*", %{}, do: {:error, :handler08_error}
    validate "handler09.*", %{}, do: raise OperationError,
      message: "Handler 9 Error", reason: :handler09_error

    validate "handler10", %{a: 1}, do: :ok
    validate "handler10", %{a: 2} = args, do: Map.put(args, :x, :buz)
    validate "handler10", %{a: 3}, do: {:error, :handler10_error_1_a}
    validate "handler10", %{a: _}, do: raise OperationError,
      message: "Handler 10 Error", reason: :handler10_error_1_b
    validate "handler10", _, do: {:error, :handler10_error_2}

    validate "handler11", a: 1, do: :ok
    validate "handler11", a: 2, do: {:error, :handler11_error_1_a}
    validate "handler11", a: _, do: raise OperationError,
      message: "Handler 11 Error", reason: :handler11_error_1_b
    validate "handler11", _, do: {:error, :handler11_error_2}

    validate "handler12", %Ctx{auth: true} = ctx, %{a: 1} = args, do: Map.put(args, :x, ctx.sess)
    validate "handler12", %Ctx{}, %{a: 1}, do: {:error, :handler12_error_a}
    validate "handler12", %Ctx{auth: nil}, %{a: 2}, do: :ok
    validate "handler12", _, %{a: 2}, do: raise OperationError,
      message: "Handler 12 Error", reason: :handler12_error_b

    validate "handler13", %Ctx{auth: true}, a: 1, do: :ok
    validate "handler13", %Ctx{}, a: 1, do: {:error, :handler13_error_a}
    validate "handler13", %Ctx{auth: nil}, a: 2, do: :ok
    validate "handler13", _, a: 2, do: raise OperationError,
      message: "Handler 13 Error", reason: :handler13_error_b

    validate "handler14", %{action: action} = args do
      case action do
        :ok -> :ok
        :args -> args
        :ok_args -> {:ok, args}
        :error -> {:error, :handler14_error_a}
        :raise ->
            raise OperationError,
              message: "Handler 14 Error", reason: :handler14_error_b
      end
    end
  end

  defmodule LaxValidateDirector2 do
    use Convex.Director
    alias Convex.DirectorTest.DummyDirector
    alias Convex.OperationError
    alias Convex.Context, as: Ctx

    delegate "*", DummyDirector

    validate "handler01" do
      :ok
    end

    validate "handler02" do
      {:error, :handler02_error}
    end

    validate "handler03" do
      raise OperationError,
        message: "Handler 3 Error", reason: :handler03_error
    end

    validate "handler04.*", args do
      Map.put(args, :x, :foo)
    end

    validate "handler05.*", _args do
      {:error, :handler05_error}
    end

    validate "handler06.*", _args do
      raise OperationError,
        message: "Handler 6 Error", reason: :handler06_error
    end


    validate "handler07.*", %{} = args do
      Map.put(args, :x, :bar)
    end

    validate "handler08.*", %{} do
      {:error, :handler08_error}
    end

    validate "handler09.*", %{} do
      raise OperationError,
        message: "Handler 9 Error", reason: :handler09_error
    end


    validate "handler10", %{a: 1} do
      :ok
    end

    validate "handler10", %{a: 2} = args do
      Map.put(args, :x, :buz)
    end

    validate "handler10", %{a: 3} do
      {:error, :handler10_error_1_a}
    end

    validate "handler10", %{a: _} do
      raise OperationError,
        message: "Handler 10 Error", reason: :handler10_error_1_b
    end

    validate "handler10", _ do
      {:error, :handler10_error_2}
    end


    validate "handler11", a: 1 do
      :ok
    end

    validate "handler11", a: 2 do
      {:error, :handler11_error_1_a}
    end

    validate "handler11", a: _ do
      raise OperationError,
        message: "Handler 11 Error", reason: :handler11_error_1_b
    end

    validate "handler11", _ do
      {:error, :handler11_error_2}
    end


    validate "handler12", %Ctx{auth: true} = ctx, %{a: 1} = args do
      Map.put(args, :x, ctx.sess)
    end

    validate "handler12", %Ctx{}, %{a: 1} do
      {:error, :handler12_error_a}
    end

    validate "handler12", %Ctx{auth: nil}, %{a: 2} do
      :ok
    end

    validate "handler12", _, %{a: 2} do
      raise OperationError,
        message: "Handler 12 Error", reason: :handler12_error_b
    end


    validate "handler13", %Ctx{auth: true}, a: 1 do
      :ok
    end

    validate "handler13", %Ctx{}, a: 1 do
      {:error, :handler13_error_a}
    end

    validate "handler13", %Ctx{auth: nil}, a: 2 do
      :ok
    end

    validate "handler13", _, a: 2 do
      raise OperationError,
        message: "Handler 13 Error", reason: :handler13_error_b
    end

    validate "handler14", %{action: action} = args do
      case action do
        :ok -> :ok
        :args -> args
        :ok_args -> {:ok, args}
        :error -> {:error, :handler14_error_a}
        :raise ->
          raise OperationError,
            message: "Handler 14 Error", reason: :handler14_error_b
      end
    end
  end

  defmodule DefValidateDirector do
    use Convex.Director

    delegate "*", DummyDirector

    validate "foo.bar", a: 1, do: :ok

    def validate(_ctx, [:foo, :bar], %{a: 2} = args), do: {:ok, Map.put(args, :x, :foo)}

    def validate(_ctx, [:foo, :bar], %{a: 3} = args) do
      {:ok, Map.put(args, :x, :bar)}
    end

    def validate(_ctx, [:foo, :bar], %{a: n} = args)
      when is_number(n), do: {:ok, Map.put(args, :a, n * 2)}

    def validate(_ctx, [:foo, :bar], %{a: s} = args) when is_binary(s) do
      {:ok, Map.put(args, :a, String.upcase(s))}
    end

    validate "foo.bar", args, do: Map.put(args, :x, :buz)
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

  test "basic" do
    ctx = Sync.new(director: MainDirector)

    res = perform with: ctx do
      basic.handler1.spam
    end
    assert {:ok, :handler_basic_1} == res

    res = perform with: ctx do
      basic.handler1.bacon
    end
    assert {:ok, :handler_basic_1} == res

    res = perform with: ctx do
      basic.handler1.bacon.eggs
    end
    assert {:ok, :handler_basic_1} == res

    res = perform with: ctx do
      basic.handler1
    end
    assert {:ok, :handler_basic_1} == res

    res = perform with: ctx do
      basic.handler2.spam
    end
    assert {:error, :handler_basic_2} == res

    res = perform with: ctx do
      basic.handler2.bacon
    end
    assert {:error, :unknown_operation} == res

    res = perform with: ctx do
      basic.handler3
    end
    assert {:ok, nil} == res

    res = perform with: ctx do
      basic.handler3.spam
    end
    assert {:error, :unknown_operation} == res

    res = perform with: ctx do
      basic.handler4
    end
    assert {:error, :unknown_operation} == res
  end


  test "parameters" do
    anon_ctx = Sync.new(director: MainDirector)
    auth_ctx = Sync.new(director: MainDirector)
      |> Ctx.authenticate(true, nil)
      |> Ctx.attach(:foo)


    res = perform with: anon_ctx do
      param.handler1.spam a: 1
    end
    assert {:ok, :handler_param_1_a} == res

    res = perform with: anon_ctx do
      param.handler1.bacon a: 2
    end
    assert {:ok, :handler_param_1_b} == res

    res = perform with: anon_ctx do
      param.handler1.eggs.sausage a: 3
    end
    assert {:ok, :handler_param_1_c} == res

    res = perform with: anon_ctx do
      param.handler1 a: 4
    end
    assert {:ok, :handler_param_1_d} == res

    res = perform with: anon_ctx do
      param.handler1.foo a: 5
    end
    assert {:error, :unknown_operation} == res


    res = perform with: auth_ctx do
      param.handler2.spam a: 1
    end
    assert {:ok, :handler_param_2_a} == res

    res = perform with: anon_ctx do
      param.handler2.bacon a: 1
    end
    assert {:ok, :handler_param_2_b} == res

    res = perform with: auth_ctx do
      param.handler2.eggs.sausage a: 2
    end
    assert {:ok, :handler_param_2_c} == res

    res = perform with: anon_ctx do
      param.handler2 a: 2
    end
    assert {:ok, :handler_param_2_d} == res

    res = perform with: auth_ctx do
      param.handler2.spam a: 3
    end
    assert {:ok, {:handler_param_2_e, true}} == res

    res = perform with: anon_ctx do
      param.handler2.bacon a: 3
    end
    assert {:ok, {:handler_param_2_e, nil}} == res

    res = perform with: auth_ctx do
      param.handler2.bacon a: 4
    end
    assert {:ok, {:handler_param_2_f, true}} == res

    res = perform with: anon_ctx do
      param.handler2.eggs.sausage a: 4
    end
    assert {:ok, {:handler_param_2_f, nil}} == res

    res = perform with: auth_ctx do
      param.handler2 a: 5
    end
    assert {:ok, {:handler_param_2_g, :foo}} == res

    res = perform with: anon_ctx do
      param.handler2.spam a: 5
    end
    assert {:error, :unknown_operation} == res

    res = perform with: auth_ctx do
      param.handler2.bacon a: 6
    end
    assert {:ok, {:handler_param_2_h, :foo}} == res

    res = perform with: anon_ctx do
      param.handler2.eggs.bacon a: 6
    end
    assert {:error, :unknown_operation} == res

    res = perform with: auth_ctx do
      param.handler2 a: 7
    end
    assert {:ok, {:handler_param_2_i, true}} == res

    res = perform with: anon_ctx do
      param.handler2.spam a: 7
    end
    assert {:ok, {:handler_param_2_i, nil}} == res

    res = perform with: auth_ctx do
      param.handler2 a: 8
    end
    assert {:ok, :handler_param_2_j} == res

    res = perform with: anon_ctx do
      param.handler2.spam a: 8
    end
    assert {:ok, :handler_param_2_k} == res


    res = perform with: anon_ctx do
      param.handler3 a: "test"
    end
    assert {:ok, {:handler_param_3, "TEST"}} == res

    res = perform with: anon_ctx do
      param.handler3 a: :test
    end
    assert {:error, :unknown_operation} == res

    res = perform with: anon_ctx do
      param.handler3.foo a: "test"
    end
    assert {:error, :unknown_operation} == res

    res = perform with: anon_ctx do
      param.handler4 a: "test"
    end
    assert {:ok, {:handler_param_4, "TEST"}} == res

    res = perform with: anon_ctx do
      param.handler4 a: :test
    end
    assert {:error, :unknown_operation} == res

    res = perform with: anon_ctx do
      param.handler4.foo a: "test"
    end
    assert {:error, :unknown_operation} == res

    res = perform with: anon_ctx do
      param.handler5 a: :test
    end
    assert {:ok, {:handler_param_5, :test}} == res

    res = perform with: anon_ctx do
      param.handler5.bar a: :test
    end
    assert {:error, :unknown_operation} == res

    res = perform with: anon_ctx do
      param.handler6 a: 42
    end
    assert {:ok, {:handler_param_6, 42}} == res

    res = perform with: anon_ctx do
      param.handler6.bar a: 42
    end
    assert {:error, :unknown_operation} == res

    res = perform with: anon_ctx do
      param.handler7.spam action: :noresult
    end
    assert {:ok, nil} == res

    res = perform with: anon_ctx do
      param.handler7.spam action: :ok
    end
    assert {:ok, :handler_param_7} == res

    res = perform with: anon_ctx do
      param.handler7.spam action: :error
    end
    assert {:error, :handler_param_7_a} == res

    res = perform with: anon_ctx do
      param.handler7.spam action: :raise
    end
    assert {:error, :handler_param_7_b} == res

    res = perform with: anon_ctx do
      param.handler7 action: :ok
    end
    assert {:error, :unknown_operation} == res


    res = perform with: anon_ctx do
      param.handler8 a: %{b: %{c: [{1, 2}, {:foo}]}}
    end
    assert {:ok, {:handler_param_8, :foo}} == res

    res = perform with: anon_ctx do
      param.handler8 a: %{b: %{c: [{:foo}, {1, 2}]}}
    end
    assert {:error, :unknown_operation} == res
  end


  test "validate" do
    anon_ctx1 = Sync.new(director: StrictValidateDirector)
    auth_ctx1 = Sync.new(director: StrictValidateDirector)
      |> Ctx.authenticate(true, nil)
      |> Ctx.attach("sid")

    anon_ctx2 = Sync.new(director: LaxValidateDirector1)
    auth_ctx2 = Sync.new(director: LaxValidateDirector1)
      |> Ctx.authenticate(true, nil)
      |> Ctx.attach("sid")

    anon_ctx3 = Sync.new(director: LaxValidateDirector2)
    auth_ctx3 = Sync.new(director: LaxValidateDirector2)
      |> Ctx.authenticate(true, nil)
      |> Ctx.attach("sid")

    config = [
      {StrictValidateDirector, anon_ctx1, auth_ctx1},
      {LaxValidateDirector1, anon_ctx2, auth_ctx2},
      {LaxValidateDirector2, anon_ctx3, auth_ctx3}
    ]
    for {mod, ctx, auth_ctx} <- config do

      assert {:ok, %{}} == mod.validate(ctx, [:handler01], %{})
      assert {:ok, %{foo: 42}} == mod.validate(ctx, [:handler01], %{foo: 42})
      res = perform ctx, do: handler01
      assert {:ok, {[:handler01], %{}}} == res
      res = perform ctx, do: handler01 bar: 42
      assert {:ok, {[:handler01], %{bar: 42}}} == res

      assert {:error, :handler02_error} == mod.validate(ctx, [:handler02], %{})
      assert {:error, :handler02_error} == mod.validate(ctx, [:handler02], %{foo: 42})
      res = perform ctx, do: handler02
      assert {:error, :handler02_error} == res
      res = perform ctx, do: handler02 bar: 42
      assert {:error, :handler02_error} == res

      assert {:error, :handler03_error} == mod.validate(ctx, [:handler03], %{})
      assert {:error, :handler03_error} == mod.validate(ctx, [:handler03], %{foo: 42})
      res = perform ctx, do: handler03
      assert {:error, :handler03_error} == res
      res = perform ctx, do: handler03 bar: 42
      assert {:error, :handler03_error} == res

      assert {:ok, %{x: :foo}} == mod.validate(ctx, [:handler04, :spam], %{})
      assert {:ok, %{x: :foo, a: 42}} == mod.validate(ctx, [:handler04, :bacon], %{a: 42})
      res = perform ctx, do: handler04.eggs.sausage
      assert {:ok, {[:handler04, :eggs, :sausage], %{x: :foo}}} == res
      res = perform ctx, do: handler04 bar: 42
      assert {:ok, {[:handler04], %{x: :foo, bar: 42}}} == res

      assert {:error, :handler05_error} == mod.validate(ctx, [:handler05, :spam], %{})
      assert {:error, :handler05_error} == mod.validate(ctx, [:handler05, :bacon], %{foo: 42})
      res = perform ctx, do: handler05.eggs.sausage
      assert {:error, :handler05_error} == res
      res = perform ctx, do: handler05 bar: 42
      assert {:error, :handler05_error} == res

      assert {:error, :handler06_error} == mod.validate(ctx, [:handler06, :spam], %{})
      assert {:error, :handler06_error} == mod.validate(ctx, [:handler06, :bacon], %{foo: 42})
      res = perform ctx, do: handler06.eggs.sausage
      assert {:error, :handler06_error} == res
      res = perform ctx, do: handler06 bar: 42
      assert {:error, :handler06_error} == res

      assert {:ok, %{x: :bar}} == mod.validate(ctx, [:handler07, :spam], %{})
      assert {:ok, %{x: :bar, a: 42}} == mod.validate(ctx, [:handler07, :bacon], %{a: 42})
      res = perform ctx, do: handler07.eggs.sausage
      assert {:ok, {[:handler07, :eggs, :sausage], %{x: :bar}}} == res
      res = perform ctx, do: handler07 bar: 42
      assert {:ok, {[:handler07], %{x: :bar, bar: 42}}} == res

      assert {:error, :handler08_error} == mod.validate(ctx, [:handler08, :spam], %{})
      assert {:error, :handler08_error} == mod.validate(ctx, [:handler08, :bacon], %{foo: 42})
      res = perform ctx, do: handler08.eggs.sausage
      assert {:error, :handler08_error} == res
      res = perform ctx, do: handler08 bar: 42
      assert {:error, :handler08_error} == res

      assert {:error, :handler09_error} == mod.validate(ctx, [:handler09, :spam], %{})
      assert {:error, :handler09_error} == mod.validate(ctx, [:handler09, :bacon], %{foo: 42})
      res = perform ctx, do: handler09.eggs.sausage
      assert {:error, :handler09_error} == res
      res = perform ctx, do: handler09 bar: 42
      assert {:error, :handler09_error} == res

      assert {:ok, %{a: 1, b: 42}} == mod.validate(ctx, [:handler10], %{a: 1, b: 42})
      assert {:ok, %{a: 2, b: 42, x: :buz}} == mod.validate(ctx, [:handler10], %{a: 2, b: 42})
      assert {:error, :handler10_error_1_a} == mod.validate(ctx, [:handler10], %{a: 3, b: 42})
      assert {:error, :handler10_error_1_b} == mod.validate(ctx, [:handler10], %{a: :x, b: 42})
      assert {:error, :handler10_error_2} == mod.validate(ctx, [:handler10], %{b: 42})
      res = perform ctx, do: handler10 a: 1, b: {}
      assert {:ok, {[:handler10], %{a: 1, b: {}}}} == res
      res = perform ctx, do: handler10 a: 2, b: {}
      assert {:ok, {[:handler10], %{a: 2, b: {}, x: :buz}}} == res
      res = perform ctx, do: handler10 a: 3, b: {}
      assert {:error, :handler10_error_1_a} == res
      res = perform ctx, do: handler10 a: :x, b: {}
      assert {:error, :handler10_error_1_b} == res
      res = perform ctx, do: handler10 b: {}
      assert {:error, :handler10_error_2} == res

      assert {:ok, %{a: 1, b: 33}} == mod.validate(ctx, [:handler11], %{a: 1, b: 33})
      assert {:error, :handler11_error_1_a} == mod.validate(ctx, [:handler11], %{a: 2, b: 33})
      assert {:error, :handler11_error_1_b} == mod.validate(ctx, [:handler11], %{a: "", b: 33})
      assert {:error, :handler11_error_2} == mod.validate(ctx, [:handler11], %{b: 33})
      res = perform ctx, do: handler11 a: 1, b: ""
      assert {:ok, {[:handler11], %{a: 1, b: ""}}} == res
      res = perform ctx, do: handler11 a: 2, b: ""
      assert {:error, :handler11_error_1_a} == res
      res = perform ctx, do: handler11 a: "", b: ""
      assert {:error, :handler11_error_1_b} == res
      res = perform ctx, do: handler11 b: ""
      assert {:error, :handler11_error_2} == res

      assert {:ok, %{a: 1, b: [], x: "sid"}} == mod.validate(auth_ctx, [:handler12], %{a: 1, b: []})
      assert {:error, :handler12_error_a} == mod.validate(ctx, [:handler12], %{a: 1, b: []})
      assert {:ok, %{a: 2, b: []}} == mod.validate(ctx, [:handler12], %{a: 2, b: []})
      assert {:error, :handler12_error_b} == mod.validate(auth_ctx, [:handler12], %{a: 2, b: []})
      res = perform auth_ctx, do: handler12 a: 1, b: ""
      assert {:ok, {[:handler12], %{a: 1, b: "", x: "sid"}}} == res
      res = perform ctx, do: handler12 a: 1, b: ""
      assert {:error, :handler12_error_a} == res
      res = perform ctx, do: handler12 a: 2, b: ""
      assert {:ok, {[:handler12], %{a: 2, b: ""}}} == res
      res = perform auth_ctx, do: handler12 a: 2, b: ""
      assert {:error, :handler12_error_b} == res

      assert {:ok, %{a: 1, b: []}} == mod.validate(auth_ctx, [:handler13], %{a: 1, b: []})
      assert {:error, :handler13_error_a} == mod.validate(ctx, [:handler13], %{a: 1, b: []})
      assert {:ok, %{a: 2, b: []}} == mod.validate(ctx, [:handler13], %{a: 2, b: []})
      assert {:error, :handler13_error_b} == mod.validate(auth_ctx, [:handler13], %{a: 2, b: []})
      res = perform auth_ctx, do: handler13 a: 1, b: ""
      assert {:ok, {[:handler13], %{a: 1, b: ""}}} == res
      res = perform ctx, do: handler13 a: 1, b: ""
      assert {:error, :handler13_error_a} == res
      res = perform ctx, do: handler13 a: 2, b: ""
      assert {:ok, {[:handler13], %{a: 2, b: ""}}} == res
      res = perform auth_ctx, do: handler13 a: 2, b: ""
      assert {:error, :handler13_error_b} == res

      assert {:ok, %{action: :ok, foo: 1}} == mod.validate(ctx, [:handler14], %{action: :ok, foo: 1})
      assert {:ok, %{action: :args, foo: 2}} == mod.validate(ctx, [:handler14], %{action: :args, foo: 2})
      assert {:ok, %{action: :ok_args, foo: 3}} == mod.validate(ctx, [:handler14], %{action: :ok_args, foo: 3})
      assert {:error, :handler14_error_a} == mod.validate(ctx, [:handler14], %{action: :error, foo: 4})
      assert {:error, :handler14_error_b} == mod.validate(ctx, [:handler14], %{action: :raise, foo: 5})
      res = perform ctx, do: handler14 action: :ok, foo: 1
      assert {:ok, {[:handler14], %{action: :ok, foo: 1}}} == res
      res = perform ctx, do: handler14 action: :args, foo: 2
      assert {:ok, {[:handler14], %{action: :args, foo: 2}}} == res
      res = perform ctx, do: handler14 action: :ok_args, foo: 3
      assert {:ok, {[:handler14], %{action: :ok_args, foo: 3}}} == res
      res = perform ctx, do: handler14 action: :error, foo: 4
      assert {:error, :handler14_error_a} == res
      res = perform ctx, do: handler14 action: :raise, foo: 4
      assert {:error, :handler14_error_b} == res
    end
  end


  test "validate enforcement" do
    strict_ctx = Sync.new(director: StrictValidateDirector)
    lax_ctx = Sync.new(director: LaxValidateDirector1)

    res = perform lax_ctx, do: handler15 toto: 18
    assert {:ok, {[:handler15], %{toto: 18}}} == res

    res = perform strict_ctx, do: handler15 toto: 18
    assert {:error, :unknown_operation} == res
  end


  test "validate direct function definition" do
    mod = DefValidateDirector
    ctx = Sync.new(director: mod)

    assert {:ok, %{a: 1}} = mod.validate(ctx, [:foo, :bar], %{a: 1})
    assert {:ok, %{a: 2, x: :foo}} = mod.validate(ctx, [:foo, :bar], %{a: 2})
    assert {:ok, %{a: 3, x: :bar}} = mod.validate(ctx, [:foo, :bar], %{a: 3})
    assert {:ok, %{a: 16}} = mod.validate(ctx, [:foo, :bar], %{a: 8})
    assert {:ok, %{a: "TEST"}} = mod.validate(ctx, [:foo, :bar], %{a: "test"})
    assert {:ok, %{a: :test, x: :buz}} = mod.validate(ctx, [:foo, :bar], %{a: :test})
  end

end