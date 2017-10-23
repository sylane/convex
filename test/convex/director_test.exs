defmodule Convex.DirectorTest do

  #===========================================================================
  # Sub-Modules
  #===========================================================================

  defmodule Error do
    defexception [:message]
  end

  defmodule DummyDirector do
    use Convex.Director

    def perform(op = "handler04.*", args), do: {:ok, {op, args}}
    def perform("*" = op, args), do: {:ok, {op, args}}
  end


  defmodule BasicDirector do
    use Convex.Director

    def perform(~o"basic.handler1.*"), do: {:ok, :handler_basic_1}

    def perform(~o"basic.handler2.spam"), do: {:error, :handler_basic_2}

    def perform("basic.handler3"), do: :ok
  end

  defmodule ParamDirector do
    use Convex.Director
    alias Convex.OperationError
    alias Convex.Context, as: Ctx

    def perform("param.handler1.*", %{a: 1}), do: {:ok, :handler_param_1_a}

    def perform("param.handler1.*", %{a: 2}) do
      {:ok, :handler_param_1_b}
    end

    def perform("param.handler1.*", %{a: 3}), do: {:ok, :handler_param_1_c}

    def perform("param.handler1.*", %{a: 4}) do
      {:ok, :handler_param_1_d}
    end

    def perform(%Ctx{auth: true}, "param.handler2.*", %{a: 1}) do
      {:ok, :handler_param_2_a}
    end

    def perform(_ctx, "param.handler2.*", %{a: 1}) do
      {:ok, :handler_param_2_b}
    end

    def perform(%Ctx{auth: true}, "param.handler2.*", %{a: 2}) do
      {:ok, :handler_param_2_c}
    end

    def perform("param.handler2.*", %{a: 2}) do
      {:ok, :handler_param_2_d}
    end

    def perform(%Ctx{auth: auth}, "param.handler2.*", %{a: 3}) do
      {:ok, {:handler_param_2_e, auth}}
    end

    def perform(%Ctx{auth: auth}, "param.handler2.*", %{a: 4}), do: {:ok, {:handler_param_2_f, auth}}

    def perform(%Ctx{auth: true} = ctx, "param.handler2.*", %{a: 5}) do
      {:ok, {:handler_param_2_g, ctx.sess}}
    end

    def perform(%Ctx{auth: true} = ctx, "param.handler2.*", %{a: 6}), do: {:ok, {:handler_param_2_h, ctx.sess}}

    def perform(%Ctx{auth: auth}, "param.handler2.*", %{a: 7}), do: {:ok, {:handler_param_2_i, auth}}


    def perform(%Ctx{auth: true}, "param.handler2.*", %{a: 8}), do: {:ok, :handler_param_2_j}

    def perform("param.handler2.*", %{a: 8}), do: {:ok, :handler_param_2_k}

    def perform([:param, :handler3], %{a: a})
      when is_binary(a) do
        {:ok, {:handler_param_3, String.upcase(a)}}
    end

    def perform(ctx, [:param, :handler4], %{a: a}) when is_binary(a), do: Ctx.done(ctx, {:handler_param_4, String.upcase(a)})

    def perform([:param, :handler5], %{a: a}) do
        {:ok, {:handler_param_5, a}}
    end

    def perform(ctx, [:param, :handler6], %{a: a}), do: Ctx.done(ctx, {:handler_param_6, a})

    def perform("param.handler7.spam", %{action: action}) do
      case action do
        :noresult -> :ok
        :ok -> {:ok, :handler_param_7}
        :error -> {:error, :handler_param_7_a}
        :raise ->
          raise OperationError,
            message: "Handler 7 Error",
            reason: :handler_param_7_b
      end
    end

    def perform("param.handler8", %{a: %{b: %{c: [{_, _}, {d}]}}}) do
      {:ok, {:handler_param_8, d}}
    end
  end

  defmodule MainDirector do
    use Convex.Director
    alias Convex.DirectorTest.BasicDirector

    def delegate("basic.*"), do: BasicDirector
    def delegate("param.*"), do: Convex.DirectorTest.ParamDirector
  end

  defmodule StrictValidateDirector1 do
    use Convex.Director
    alias Convex.DirectorTest.DummyDirector
    alias Convex.OperationError
    alias Convex.Context, as: Ctx

    @enforce_validation true

    def delegate("*"), do: DummyDirector

    def validate(~o"handler01"), do: :ok
    def validate("handler02"), do: {:error, :handler02_error}
    def validate("handler03"), do: raise OperationError,
      message: "Handler 3 Error", reason: :handler03_error

    def validate("handler04.*", args), do: Map.put(args, :x, :foo)
    def validate("handler05.*", _args), do: {:error, :handler05_error}
    def validate("handler06.*", _args), do: raise OperationError,
      message: "Handler 6 Error", reason: :handler06_error

    def validate("handler07.*", %{} = args), do: Map.put(args, :x, :bar)
    def validate("handler08.*", %{}), do: {:error, :handler08_error}
    def validate("handler09.*", %{}), do: raise OperationError,
      message: "Handler 9 Error", reason: :handler09_error

    def validate("handler10", %{a: 1}), do: :ok
    def validate("handler10", %{a: 2} = args), do: Map.put(args, :x, :buz)
    def validate("handler10", %{a: 3}), do: {:error, :handler10_error_1_a}
    def validate("handler10", %{a: _}), do: raise OperationError,
      message: "Handler 10 Error", reason: :handler10_error_1_b
    def validate("handler10", _), do: {:error, :handler10_error_2}

    def validate("handler11", %{a: 1}), do: :ok
    def validate("handler11", %{a: 2}), do: {:error, :handler11_error_1_a}
    def validate("handler11", %{a: _}), do: raise OperationError,
      message: "Handler 11 Error", reason: :handler11_error_1_b
    def validate("handler11"), do: {:error, :handler11_error_2}

    def validate(%Ctx{auth: true} = ctx, "handler12", %{a: 1} = args), do: Map.put(args, :x, ctx.sess)
    def validate(%Ctx{}, "handler12", %{a: 1}), do: {:error, :handler12_error_a}
    def validate(%Ctx{auth: nil}, "handler12", %{a: 2}), do: :ok
    def validate("handler12", %{a: 2}), do: raise OperationError,
      message: "Handler 12 Error", reason: :handler12_error_b

    def validate(%Ctx{auth: true}, "handler13", %{a: 1}), do: :ok
    def validate(%Ctx{}, "handler13", %{a: 1}), do: {:error, :handler13_error_a}
    def validate(%Ctx{auth: nil}, "handler13", %{a: 2}), do: :ok
    def validate("handler13", %{a: 2}), do: raise OperationError,
      message: "Handler 13 Error", reason: :handler13_error_b

    def validate("handler14", %{action: action} = args) do
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

  defmodule StrictValidateDirector2 do
    use Convex.Director
    alias Convex.DirectorTest.DummyDirector
    alias Convex.OperationError
    alias Convex.Context, as: Ctx

    def delegate("*"), do: DummyDirector

    def validate("handler01"), do: :ok
    def validate("handler02"), do: {:error, :handler02_error}
    def validate("handler03"), do: raise OperationError,
      message: "Handler 3 Error", reason: :handler03_error

    def validate("handler04.*", args), do: Map.put(args, :x, :foo)
    def validate("handler05.*", _args), do: {:error, :handler05_error}
    def validate("handler06.*", _args), do: raise OperationError,
      message: "Handler 6 Error", reason: :handler06_error

    def validate("handler07.*", %{} = args), do: Map.put(args, :x, :bar)
    def validate("handler08.*", %{}), do: {:error, :handler08_error}
    def validate("handler09.*", %{}), do: raise OperationError,
      message: "Handler 9 Error", reason: :handler09_error

    def validate("handler10", %{a: 1}), do: :ok
    def validate("handler10", %{a: 2} = args), do: Map.put(args, :x, :buz)
    def validate("handler10", %{a: 3}), do: {:error, :handler10_error_1_a}
    def validate("handler10", %{a: _}), do: raise OperationError,
      message: "Handler 10 Error", reason: :handler10_error_1_b
    def validate("handler10", _), do: {:error, :handler10_error_2}

    def validate("handler11", %{a: 1}), do: :ok
    def validate("handler11", %{a: 2}), do: {:error, :handler11_error_1_a}
    def validate("handler11", %{a: _}), do: raise OperationError,
      message: "Handler 11 Error", reason: :handler11_error_1_b
    def validate("handler11"), do: {:error, :handler11_error_2}

    def validate(%Ctx{auth: true} = ctx, "handler12", %{a: 1} = args), do: Map.put(args, :x, ctx.sess)
    def validate(%Ctx{}, "handler12", %{a: 1}), do: {:error, :handler12_error_a}
    def validate(%Ctx{auth: nil}, "handler12", %{a: 2}), do: :ok
    def validate("handler12", %{a: 2}), do: raise OperationError,
      message: "Handler 12 Error", reason: :handler12_error_b

    def validate(%Ctx{auth: true}, "handler13", %{a: 1}), do: :ok
    def validate(%Ctx{}, "handler13", %{a: 1}), do: {:error, :handler13_error_a}
    def validate(%Ctx{auth: nil}, "handler13", %{a: 2}), do: :ok
    def validate("handler13", %{a: 2}), do: raise OperationError,
      message: "Handler 13 Error", reason: :handler13_error_b

    def validate("handler14", %{action: action} = args) do
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

  defmodule LaxValidateDirector do
    use Convex.Director
    alias Convex.DirectorTest.DummyDirector
    alias Convex.OperationError
    alias Convex.Context, as: Ctx

    @enforce_validation false

    def delegate("*"), do: DummyDirector

    def validate("handler01") do
      :ok
    end

    def validate("handler02") do
      {:error, :handler02_error}
    end

    def validate("handler03") do
      raise OperationError,
        message: "Handler 3 Error", reason: :handler03_error
    end

    def validate("handler04.*", args) do
      Map.put(args, :x, :foo)
    end

    def validate("handler05.*", _args) do
      {:error, :handler05_error}
    end

    def validate("handler06.*", _args) do
      raise OperationError,
        message: "Handler 6 Error", reason: :handler06_error
    end


    def validate("handler07.*", %{} = args) do
      Map.put(args, :x, :bar)
    end

    def validate("handler08.*", %{}) do
      {:error, :handler08_error}
    end

    def validate("handler09.*", %{}) do
      raise OperationError,
        message: "Handler 9 Error", reason: :handler09_error
    end


    def validate("handler10", %{a: 1}) do
      :ok
    end

    def validate("handler10", %{a: 2} = args) do
      Map.put(args, :x, :buz)
    end

    def validate("handler10", %{a: 3}) do
      {:error, :handler10_error_1_a}
    end

    def validate("handler10", %{a: _}) do
      raise OperationError,
        message: "Handler 10 Error", reason: :handler10_error_1_b
    end

    def validate("handler10") do
      {:error, :handler10_error_2}
    end


    def validate("handler11", %{a: 1}) do
      :ok
    end

    def validate("handler11", %{a: 2}) do
      {:error, :handler11_error_1_a}
    end

    def validate("handler11", %{a: _}) do
      raise OperationError,
        message: "Handler 11 Error", reason: :handler11_error_1_b
    end

    def validate("handler11") do
      {:error, :handler11_error_2}
    end


    def validate(%Ctx{auth: true} = ctx, "handler12", %{a: 1} = args) do
      Map.put(args, :x, ctx.sess)
    end

    def validate(%Ctx{}, "handler12", %{a: 1}) do
      {:error, :handler12_error_a}
    end

    def validate(%Ctx{auth: nil}, "handler12", %{a: 2}) do
      :ok
    end

    def validate("handler12", %{a: 2}) do
      raise OperationError,
        message: "Handler 12 Error", reason: :handler12_error_b
    end


    def validate(%Ctx{auth: true}, "handler13", %{a: 1}) do
      :ok
    end

    def validate(%Ctx{}, "handler13", %{a: 1}) do
      {:error, :handler13_error_a}
    end

    def validate(%Ctx{auth: nil}, "handler13", %{a: 2}) do
      :ok
    end

    def validate("handler13", %{a: 2}) do
      raise OperationError,
        message: "Handler 13 Error", reason: :handler13_error_b
    end

    def validate("handler14", %{action: action} = args) do
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

    def delegate("*"), do: DummyDirector

    def validate("foo.bar", %{a: 1}), do: :ok

    def validate([:foo, :bar], %{a: 2} = args), do: {:ok, Map.put(args, :x, :foo)}

    def validate(_ctx, [:foo, :bar], %{a: 3} = args) do
      {:ok, Map.put(args, :x, :bar)}
    end

    def validate(_ctx, [:foo, :bar], %{a: n} = args)
      when is_number(n), do: {:ok, Map.put(args, :a, n * 2)}

    def validate(_ctx, [:foo, :bar], %{a: s} = args) when is_binary(s) do
      {:ok, Map.put(args, :a, String.upcase(s))}
    end

    def validate("foo.bar", args), do: Map.put(args, :x, :buz)
  end


  defmodule RecursiveDirector do
    use Convex.Director

    @enforce_validation false

    def perform(ctx, "toto", args) do
      _perform(ctx, [:tata], Map.put(args, :toto, true))
    end

    def perform(ctx, [:tata], args) do
      _perform(ctx, [:tutu], Map.put(args, :tata, true))
    end

    def delegate("*"), do: DummyDirector

    def validate(ctx, "foo" = op, %{v: 1} = args) do
      args = args
        |> Map.put(:foo1, true)
        |> Map.put(:v, 2)
      _validate(ctx, op, args)
    end

    def validate(ctx, [:foo], %{v: 2} = args) do
      args = args
        |> Map.put(:foo2, true)
        |> Map.put(:v, 3)
      _validate(ctx, [:foo], args)
    end

    def validate("foo", args), do: Map.put(args, :foo3, true)
  end


  defmodule ProduceDirector do
    use Convex.Director

    def perform("double", %{v: v}), do: {:ok, v * 2}
    def perform("foo"), do: {:produce, [1, 2, 3]}
    def perform("bar", %{from: from, to: to}) do
      values = for v <- from..to, do: v
      {:produce, values}
    end
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
    anon_ctx1 = Sync.new(director: StrictValidateDirector1)
    auth_ctx1 = Sync.new(director: StrictValidateDirector1)
      |> Ctx.authenticate(true, nil)
      |> Ctx.attach("sid")

    anon_ctx2 = Sync.new(director: StrictValidateDirector2)
    auth_ctx2 = Sync.new(director: StrictValidateDirector2)
      |> Ctx.authenticate(true, nil)
      |> Ctx.attach("sid")

    anon_ctx3 = Sync.new(director: LaxValidateDirector)
    auth_ctx3 = Sync.new(director: LaxValidateDirector)
      |> Ctx.authenticate(true, nil)
      |> Ctx.attach("sid")

    config = [
      {StrictValidateDirector1, anon_ctx1, auth_ctx1},
      {StrictValidateDirector2, anon_ctx2, auth_ctx2},
      {LaxValidateDirector, anon_ctx3, auth_ctx3}
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
    strict1_ctx = Sync.new(director: StrictValidateDirector1)
    strict2_ctx = Sync.new(director: StrictValidateDirector2)
    lax_ctx = Sync.new(director: LaxValidateDirector)

    res = perform lax_ctx, do: handler15 toto: 18
    assert {:ok, {[:handler15], %{toto: 18}}} == res

    res = perform strict1_ctx, do: handler15 toto: 18
    assert {:error, :unknown_operation} == res

    res = perform strict2_ctx, do: handler15 toto: 18
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


  test "recursive" do
    ctx = Sync.new(director: RecursiveDirector)

    res = perform ctx, do: toto a: 1
    assert {:ok, {[:tutu], %{a: 1, toto: true, tata: true}}} == res

    res = perform ctx, do: tata a: 2
    assert {:ok, {[:tutu], %{a: 2, tata: true}}} == res

    res = perform ctx, do: tutu a: 3
    assert {:ok, {[:tutu], %{a: 3}}} == res

    res = perform ctx, do: foo v: 1
    assert {:ok, {[:foo], %{v: 3, foo1: true, foo2: true, foo3: true}}} == res

    res = perform ctx, do: foo v: 2
    assert {:ok, {[:foo], %{v: 3, foo2: true, foo3: true}}} == res

    res = perform ctx, do: foo v: 3
    assert {:ok, {[:foo], %{v: 3, foo3: true}}} == res
  end


  test "producing" do
    ctx = Sync.new(director: ProduceDirector)

    res = perform ctx do
      v = foo
      double v: v
    end
    assert {:ok, items} = res
    assert [2, 4, 6] == Enum.sort(items)

    res = perform ctx do
      v = bar from: 5, to: 8
      double v: v
    end
    assert {:ok, items} = res
    assert [10, 12, 14, 16] == Enum.sort(items)
  end

end