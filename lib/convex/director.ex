defmodule Convex.Director do

  @moduledoc """
  Define the behaviour a director module **MUST** implement.

  Directors are used by the context/pipeline to route operations to the
  module that knows how to perform them.

  In addition, when *used*, this module instrument the defined functions
  to simplify the writing of director modules.
  The instrumentation allows the definition of functions `delegate`, `perform`
  and `validate` with the following signatures:

    ```
    @spec delegate(String.t | Ctx.op) :: module
    @spec delegate(String.t | Ctx.op, map) :: module
    @spec delegate(Ctx.t, String.t | Ctx.op, map) :: module

    @spec perform(String.t | Ctx.op)
      :: :ok
       | {:ok, result :: term}
       | {:error, reason :: term}
       | {:produce, items :: list}
       | {:delegate, module}
       | {:delegate, module, new_op :: Ctx.op}
       | {:delegate, module, new_op :: Ctx.op, new_args :: map}
       | Ctx.t
    @spec perform(String.t | Ctx.op, map) :: module()
      :: :ok
       | {:ok, result :: term}
       | {:error, reason :: term}
       | {:produce, items :: list}
       | {:delegate, module}
       | {:delegate, module, new_op :: Ctx.op}
       | {:delegate, module, new_op :: Ctx.op, new_args :: map}
       | Ctx.t
    @spec perform(Ctx.t, String.t | Ctx.op, map) :: module()
      :: :ok
       | {:ok, result :: term}
       | {:error, reason :: term}
       | {:produce, items :: list}
       | {:delegate, module}
       | {:delegate, module, new_op :: Ctx.op}
       | {:delegate, module, new_op :: Ctx.op, new_args :: map}
       | Ctx.t

    @spec validate(String.t | Ctx.op)
      :: :ok
       | new_args :: map
       | {:ok, new_args :: map}
       | {:error, reason :: term}
    @spec validate(String.t | Ctx.op, map) :: module()
      :: :ok
       | new_args :: map
       | {:ok, new_args :: map}
       | {:error, reason :: term}
    @spec validate(Ctx.t, String.t | Ctx.op, map) :: module()
      :: :ok
       | new_args :: map
       | {:ok, new_args :: map}
       | {:error, reason :: term}
    ```

  In these functions arguments, the operation can be specified as a string like
  `"foo.bar"` or `"spam.bacon.*"` and it will be converted to the proper
  operation format (a list of tuples). Note that if the operation is stored
  in a variable it will be so in the real format, the string is converted at
  compilation time. Note too that the operation that can be returned from
  the `perfrom` funtions through the `:delegate` return tuple *MUST* be a
  list of tuple (you can use the `~o` sigil to define it as a string,
  See `Convex.Sigils`).

  The funtions `delegate` are used to define operations that should be
  handled by another director.

  The functions `validate` are used to validate operations arguments and
  eventually modify them.

  The functions `perform` are used to execute an operation and return a result
  or an error. In adition, these functions can decide to delegate the opreation
  to another director or produce a list of items
  (See `Convex.Context.produce/2`).

  If `validate` functions are defined, they *MUST* match every allowed
  operations, otherwise the operation will fail with the reason
  `:unknown_operation`. To override this behavior, you can declare the
  module attribute `@enforce_validation false`.

  If these functions need to recursively continue, they can
  call the private functions `_validate/3` or `_perform/2` respectively.

  Using this module will generate two public functions:

    ```
    @spec perform(Ctx.t, Ctx.op, args :: map) :: Ctx.t
    @spec validate(Ctx.t, Ctx.op, args :: map)
      :: {:ok, new_args :: map} | {:error, reason :: term}
    ```

  To debug the generated code, the `@debug true` module attribute can be
  defined.

  e.g.

  ```Elixir
  defmodule MyApp.MyDirector do
    use Convex.Director
    def delegate("foo.*"), do: MyApp.FooDirector
    def delegate("bar.*"), do: MyApp.BarDirector
    def perform("buz.double", %{value: v}) do
      {:ok, V * 2}
    end
  end

  defmodule MyApp.FooDirector do
    use Convex.Director
    def delegate("*"), do: MyApp.MyService
    def validate("foo.spam", %{value: v}) when is_number(v), do: :ok
    def validate("foo.bacon", %{value: v} = args) when is_binary(v) do
      Map.put(args, :value, String.upcase(v))
    end
  end

  defmodule MyApp.BarDirector do
    use Convex.Director
    def perform(%Ctx{auth: auth}, "bar.protected", args) when auth != nil, do
      {:ok, MyApp.Protected.do_it(auth, args)}
    end
  end
  ```

  Roughly equivalent without *using* the director :

  ```Elixir
  defmodule MyApp.MyDirector do
    @behaviour Convex.Director
    def perform(ctx, [:foo | _] = op, args) do
      MyApp.FooDirector.perform(ctx, op, args)
    end
    def perform(ctx, [:bar | _] = op, args) do
      MyApp.BarDirector.perform(ctx, op, args)
    end
    def perform(ctx, [:buz, :double], %{value: v}) do
      Cobnvex.Context.done(ctx,  V * 2)
    end
    def perform(ctx, _op, _args) do
      Convex.Errors.unknown_operation!(ctx)
    end
  end

  defmodule MyApp.FooDirector do
    @behaviour Convex.Director
    def perform(ctx, op, args) do
      MyApp.MyService.perform(ctx, op, validate(op, args))
    end
    def validate(_ctx, [:foo, :spam], %{value: v} = args) when is_number(v), do: args
    def validate(_ctx, [:foo, :bacon], %{value: v} = args) when is_binary(v) do
      Map.put(args, :value, String.upcase(v))
    end
    def validate(ctx, _op, _args) do
      Convex.Errors.unknown_operation!(ctx)
    end
  end

  defmodule MyApp.BarDirector do
    @behaviour Convex.Director
    def perform(%Ctx{auth: auth} = ctx, [:bar, :protected], args) when auth != nil, do
      Convex.Context.done(ctx, MyApp.Protected.do_it(auth, args))
    end
    def perform(ctx, _op, _args) do
      Convex.Errors.unknown_operation!(ctx)
    end
  end
  ```
  """

  #===========================================================================
  # Includes
  #===========================================================================

  alias Convex.Context, as: Ctx


  #===========================================================================
  # Behaviour Definition
  #===========================================================================

  @doc """
  Called by the context to perform an operation.
  """
  @callback perform(context :: Ctx.t, operation :: Ctx.op, arguments :: map)
    :: {:ok, context :: Ctx.t} | {:error, reason :: term}


  #===========================================================================
  # Macros
  #===========================================================================

  defmacro __using__(_opt) do
    Module.register_attribute(__CALLER__.module, :perform_functions, accumulate: true)
    Module.register_attribute(__CALLER__.module, :validate_functions, accumulate: true)
    quote do
      @beaviour Convex.Director

      import Kernel, except: [def: 2]
      import Convex.Director, only: [def: 2]
      import Convex.Sigils, only: [sigil_o: 2]

      @before_compile Convex.Director
    end
  end


  @doc false

  defmacro def({:when, info1, [{:perform, info2, [ctx_ast, op_ast, arg_ast]}, when_ast]}, blocks_ast) do
    {ctx_ast, op_ast, arg_ast, body_ast}
      = parse_perform(__CALLER__, ctx_ast, op_ast, arg_ast, blocks_ast)
    call_ast = {:when, info1, [{:_perform, info2, [ctx_ast, op_ast, arg_ast]}, when_ast]}
    generate_perform(__CALLER__, call_ast, body_ast)
  end

  defmacro def({:perform, info, [ctx_ast, op_ast, arg_ast]}, blocks_ast) do
    {ctx_ast, op_ast, arg_ast, body_ast}
      = parse_perform(__CALLER__, ctx_ast, op_ast, arg_ast, blocks_ast)
    call_ast = {:_perform, info, [ctx_ast, op_ast, arg_ast]}
    generate_perform(__CALLER__, call_ast, body_ast)
  end

  defmacro def({:when, info1, [{:perform, info2, [op_ast, arg_ast]}, when_ast]}, blocks_ast) do
    {ctx_ast, op_ast, arg_ast, body_ast}
      = parse_perform(__CALLER__, nil, op_ast, arg_ast, blocks_ast)
    call_ast = {:when, info1, [{:_perform, info2, [ctx_ast, op_ast, arg_ast]}, when_ast]}
    generate_perform(__CALLER__, call_ast, body_ast)
  end

  defmacro def({:perform, info, [op_ast, arg_ast]}, blocks_ast) do
    {ctx_ast, op_ast, arg_ast, body_ast}
      = parse_perform(__CALLER__, nil, op_ast, arg_ast, blocks_ast)
    call_ast = {:_perform, info, [ctx_ast, op_ast, arg_ast]}
    generate_perform(__CALLER__, call_ast, body_ast)
  end

  defmacro def({:when, info1, [{:perform, info2, [op_ast]}, when_ast]}, blocks_ast) do
    {ctx_ast, op_ast, arg_ast, body_ast}
      = parse_perform(__CALLER__, nil, op_ast, nil, blocks_ast)
    call_ast = {:when, info1, [{:_perform, info2, [ctx_ast, op_ast, arg_ast]}, when_ast]}
    generate_perform(__CALLER__, call_ast, body_ast)
  end

  defmacro def({:perform, info, [op_ast]}, blocks_ast) do
    {ctx_ast, op_ast, arg_ast, body_ast}
      = parse_perform(__CALLER__, nil, op_ast, nil, blocks_ast)
    call_ast = {:_perform, info, [ctx_ast, op_ast, arg_ast]}
    generate_perform(__CALLER__, call_ast, body_ast)
  end

  defmacro def({:when, info1, [{:validate, info2, [ctx_ast, op_ast, arg_ast]}, when_ast]}, blocks_ast) do
    {ctx_ast, op_ast, arg_ast, body_ast}
      = parse_validate(__CALLER__, ctx_ast, op_ast, arg_ast, blocks_ast)
    call_ast = {:when, info1, [{:_validate, info2, [ctx_ast, op_ast, arg_ast]}, when_ast]}
    generate_validate(__CALLER__, call_ast, body_ast)
  end

  defmacro def({:validate, info, [ctx_ast, op_ast, arg_ast]}, blocks_ast) do
    {ctx_ast, op_ast, arg_ast, body_ast}
      = parse_validate(__CALLER__, ctx_ast, op_ast, arg_ast, blocks_ast)
    call_ast = {:_validate, info, [ctx_ast, op_ast, arg_ast]}
    generate_validate(__CALLER__, call_ast, body_ast)
  end

  defmacro def({:when, info1, [{:validate, info2, [op_ast, arg_ast]}, when_ast]}, blocks_ast) do
    {ctx_ast, op_ast, arg_ast, body_ast}
      = parse_validate(__CALLER__, nil, op_ast, arg_ast, blocks_ast)
    call_ast = {:when, info1, [{:_validate, info2, [ctx_ast, op_ast, arg_ast]}, when_ast]}
    generate_validate(__CALLER__, call_ast, body_ast)
  end

  defmacro def({:validate, info, [op_ast, arg_ast]}, blocks_ast) do
    {ctx_ast, op_ast, arg_ast, body_ast}
      = parse_validate(__CALLER__, nil, op_ast, arg_ast, blocks_ast)
    call_ast = {:_validate, info, [ctx_ast, op_ast, arg_ast]}
    generate_validate(__CALLER__, call_ast, body_ast)
  end

  defmacro def({:when, info1, [{:validate, info2, [op_ast]}, when_ast]}, blocks_ast) do
    {ctx_ast, op_ast, arg_ast, body_ast}
      = parse_validate(__CALLER__, nil, op_ast, nil, blocks_ast)
    call_ast = {:when, info1, [{:_validate, info2, [ctx_ast, op_ast, arg_ast]}, when_ast]}
    generate_validate(__CALLER__, call_ast, body_ast)
  end

  defmacro def({:validate, info, [op_ast]}, blocks_ast) do
    {ctx_ast, op_ast, arg_ast, body_ast}
      = parse_validate(__CALLER__, nil, op_ast, nil, blocks_ast)
    call_ast = {:_validate, info, [ctx_ast, op_ast, arg_ast]}
    generate_validate(__CALLER__, call_ast, body_ast)
  end

  defmacro def({:when, info1, [{:delegate, info2, [ctx_ast, op_ast, arg_ast]}, when_ast]}, blocks_ast) do
    {ctx_ast, op_ast, arg_ast, body_ast}
      = parse_delegate(__CALLER__, ctx_ast, op_ast, arg_ast, blocks_ast)
    call_ast = {:when, info1, [{:_delegate, info2, [ctx_ast, op_ast, arg_ast]}, when_ast]}
    generate_perform(__CALLER__, call_ast, body_ast)
  end

  defmacro def({:delegate, info, [ctx_ast, op_ast, arg_ast]}, blocks_ast) do
    {ctx_ast, op_ast, arg_ast, body_ast}
      = parse_delegate(__CALLER__, ctx_ast, op_ast, arg_ast, blocks_ast)
    call_ast = {:_perform, info, [ctx_ast, op_ast, arg_ast]}
    generate_perform(__CALLER__, call_ast, body_ast)
  end

  defmacro def({:when, info1, [{:delegate, info2, [op_ast, arg_ast]}, when_ast]}, blocks_ast) do
    {ctx_ast, op_ast, arg_ast, body_ast}
      = parse_delegate(__CALLER__, nil, op_ast, arg_ast, blocks_ast)
    call_ast = {:when, info1, [{:_perform, info2, [ctx_ast, op_ast, arg_ast]}, when_ast]}
    generate_perform(__CALLER__, call_ast, body_ast)
  end

  defmacro def({:delegate, info, [op_ast, arg_ast]}, blocks_ast) do
    {ctx_ast, op_ast, arg_ast, body_ast}
      = parse_delegate(__CALLER__, nil, op_ast, arg_ast, blocks_ast)
    call_ast = {:_perform, info, [ctx_ast, op_ast, arg_ast]}
    generate_perform(__CALLER__, call_ast, body_ast)
  end

  defmacro def({:when, info1, [{:delegate, info2, [op_ast]}, when_ast]}, blocks_ast) do
    {ctx_ast, op_ast, arg_ast, body_ast}
      = parse_delegate(__CALLER__, nil, op_ast, nil, blocks_ast)
    call_ast = {:when, info1, [{:_perform, info2, [ctx_ast, op_ast, arg_ast]}, when_ast]}
    generate_perform(__CALLER__, call_ast, body_ast)
  end

  defmacro def({:delegate, info, [op_ast]}, blocks_ast) do
    {ctx_ast, op_ast, arg_ast, body_ast}
      = parse_delegate(__CALLER__, nil, op_ast, nil, blocks_ast)
    call_ast = {:_perform, info, [ctx_ast, op_ast, arg_ast]}
    generate_perform(__CALLER__, call_ast, body_ast)
  end

  defmacro def(call_ast, blocks_ast) do
    body_ast = Keyword.fetch!(blocks_ast, :do)
    quote do
      def unquote(call_ast) do
        unquote(body_ast)
      end
    end
  end


  defmacro __before_compile__(_env) do
    enforce? = case Module.get_attribute(__CALLER__.module, :enforce_validation) do
      true -> true
      false -> false
      nil -> true
      value ->
        raise CompileError, description: "invalide value for 'enforce_validation' attribute: #{inspect value}"
    end
    perform_funs = Module.get_attribute(__CALLER__.module, :perform_functions)
      |> Enum.reverse()
    validate_funs = Module.get_attribute(__CALLER__.module, :validate_functions)
    |> Enum.reverse()

    pub_funs = if length(validate_funs) == 0 do
      [
        quote do
          def perform(ctx, op, args) do
            _perform(ctx, op, args)
          end
          def validate(_ctx, _op, args) do
            {:ok, args}
          end
        end
      ]
    else
      [
        quote do
          def perform(ctx, op, args) do
            case _validate(ctx, op, args) do
              {:ok, new_args} -> _perform(ctx, op, new_args)
              {:error, reason} ->
                Convex.Context.failed(ctx, reason)
            end
          end
          def validate(ctx, op, args) do
            try do
              _validate(ctx, op, args)
            rescue
              e -> {:error, Convex.Errors.reason(e)}
            end
          end
        end
      ]
    end
    perform_last = [
      quote do
        defp _perform(ctx, _op, _args), do: Convex.Errors.unknown_operation!(ctx)
      end
    ]
    validate_last = case {length(validate_funs) > 0, enforce?} do
      {true, true} -> [
        quote do
          defp _validate(ctx, _op, _args), do: Convex.Errors.unknown_operation!(ctx)
        end
      ]
      {true, false} -> [
        quote do
          defp _validate(_ctx, _op, args), do: {:ok, args}
        end
      ]
      {false, _} -> []
    end

    all = pub_funs ++ perform_funs ++ perform_last ++ validate_funs ++ validate_last
    ast = quote do: (unquote_splicing(all))

    if true == Module.get_attribute(__CALLER__.module, :debug) do
      IO.puts ">>>>> GENERATED CODE FOR #{__CALLER__.module}\n#{Macro.to_string(ast)}"
    end

    ast
  end


  #===========================================================================
  # Internal Functions
  #===========================================================================


  defp parse_perform(caller, ctx_ast, op_ast, args_ast, blocks_ast) do
    body_ast = Keyword.fetch!(blocks_ast, :do)
    {ctx_var, op_var, args_var, body_ast}
      = generate_perform_body(caller, body_ast)
    ctx_ast = generate_argument(ctx_ast, ctx_var)
    op_ast = convert_operation(caller, op_ast)
    op_ast = generate_argument(op_ast, op_var)
    args_ast = generate_argument(args_ast, args_var)
    {ctx_ast, op_ast, args_ast, body_ast}
  end


  defp generate_perform(caller, call_ast, body_ast) do
    ast = quote do
      defp unquote(call_ast) do
        unquote(body_ast)
      end
    end
    Module.put_attribute(caller.module, :perform_functions, ast)
  end


  defp generate_perform_body(_caller, :ok) do
    ctx_var = Macro.var(:ctx, __MODULE__)
    body_ast = quote do
      Convex.Context.done(unquote(ctx_var))
    end
    {ctx_var, nil, nil, body_ast}
  end

  defp generate_perform_body(_caller, {:ok, result_ast}) do
    ctx_var = Macro.var(:ctx, __MODULE__)
    body_ast = quote do
      Convex.Context.done(unquote(ctx_var), unquote(result_ast))
    end
    {ctx_var, nil, nil, body_ast}
  end

  defp generate_perform_body(_caller, {:error, reason_ast}) do
    ctx_var = Macro.var(:ctx, __MODULE__)
    body_ast = quote do
      Convex.Context.failed(unquote(ctx_var), unquote(reason_ast))
    end
    {ctx_var, nil, nil, body_ast}
  end

  defp generate_perform_body(_caller, {:produce, items_ast}) do
    ctx_var = Macro.var(:ctx, __MODULE__)
    body_ast = quote do
      Convex.Context.produce(unquote(ctx_var), unquote(items_ast))
    end
    {ctx_var, nil, nil, body_ast}
  end

  defp generate_perform_body(_caller, {:delegate, mod_ast}) do
    ctx_var = Macro.var(:ctx, __MODULE__)
    op_var = Macro.var(:op, __MODULE__)
    args_var = Macro.var(:args, __MODULE__)
    body_ast = quote do
      unquote(mod_ast).perform(unquote(ctx_var), unquote(op_var), unquote(args_var))
    end
    {ctx_var, op_var, args_var, body_ast}
  end

  defp generate_perform_body(_caller, {:{}, _, [:delegate, mod_ast, op_ast]}) do
    ctx_var = Macro.var(:ctx, __MODULE__)
    args_var = Macro.var(:args, __MODULE__)
    body_ast = quote do
      unquote(mod_ast).perform(unquote(ctx_var), unquote(op_ast), unquote(args_var))
    end
    {ctx_var, nil, args_var, body_ast}
  end

  defp generate_perform_body(_caller, {:{}, _, [:delegate, mod_ast, op_ast, args_ast]}) do
    ctx_var = Macro.var(:ctx, __MODULE__)
    body_ast = quote do
      unquote(mod_ast).perform(unquote(ctx_var), unquote(op_ast), unquote(args_ast))
    end
    {ctx_var, nil, nil, body_ast}
  end

  defp generate_perform_body(_caller, expr_ast) do
    ctx_var = Macro.var(:ctx, __MODULE__)
    op_var = Macro.var(:op, __MODULE__)
    args_var = Macro.var(:args, __MODULE__)
    body_ast = quote generated: true do
      case unquote(expr_ast) do
        %Convex.Context{} = result -> result
        :ok -> Convex.Context.done(unquote(ctx_var))
        {:ok, result} -> Convex.Context.done(unquote(ctx_var), result)
        {:error, reason} -> Convex.Context.failed(unquote(ctx_var), reason)
        {:produce, items} -> Convex.Context.produce(unquote(ctx_var), items)
        {:delegate, mod} ->
          mod.perform(unquote(ctx_var), unquote(op_var), unquote(args_var))
        {:delegate, mod, op} ->
          mod.perform(unquote(ctx_var), op, unquote(args_var))
        {:delegate, mod, op, args} ->
          mod.perform(unquote(ctx_var), op, args)
      end
    end
    {ctx_var, op_var, args_var, body_ast}
  end


  defp parse_validate(caller, ctx_ast, op_ast, args_ast, blocks_ast) do
    body_ast = Keyword.fetch!(blocks_ast, :do)
    {ctx_var, op_var, args_var, body_ast}
      = generate_validate_body(caller, body_ast)
    ctx_ast = generate_argument(ctx_ast, ctx_var)
    op_ast = convert_operation(caller, op_ast)
    op_ast = generate_argument(op_ast, op_var)
    args_ast = generate_argument(args_ast, args_var)
    {ctx_ast, op_ast, args_ast, body_ast}
  end


  defp generate_validate(caller, call_ast, body_ast) do
    ast = quote do
      defp unquote(call_ast) do
        unquote(body_ast)
      end
    end
    Module.put_attribute(caller.module, :validate_functions, ast)
  end


  defp generate_validate_body(_caller, :ok) do
    args_var = Macro.var(:args, __MODULE__)
    body_ast = quote do
      {:ok, unquote(args_var)}
    end
    {nil, nil, args_var, body_ast}
  end

  defp generate_validate_body(_caller, {:ok, _} = result_ast) do
    {nil, nil, nil, result_ast}
  end

  defp generate_validate_body(_caller, {:error, _} = result_ast) do
    {nil, nil, nil, result_ast}
  end

  defp generate_validate_body(_caller, {:raise, _, [_|_]} = result_ast) do
    {nil, nil, nil, result_ast}
  end

  defp generate_validate_body(_caller, {:%{}, _, _} = result_ast) do
    body_ast = quote do: {:ok, unquote(result_ast)}
    {nil, nil, nil, body_ast}
  end

  defp generate_validate_body(_caller, expr_ast) do
    args_var = Macro.var(:args, __MODULE__)
    body_ast = quote generated: true do
      case unquote(expr_ast) do
        result when is_map(result) -> {:ok, result}
        :ok -> {:ok, unquote(args_var)}
        {:ok, result} when is_map(result) -> {:ok, result}
        {:error, _reason} = error -> error
      end
    end
    {nil, nil, args_var, body_ast}
  end


  defp parse_delegate(caller, ctx_ast, op_ast, args_ast, blocks_ast) do
    body_ast = Keyword.fetch!(blocks_ast, :do)
    {ctx_var, op_var, args_var, body_ast}
      = generate_delegate_body(caller, body_ast)
    ctx_ast = generate_argument(ctx_ast, ctx_var)
    op_ast = convert_operation(caller, op_ast)
    op_ast = generate_argument(op_ast, op_var)
    args_ast = generate_argument(args_ast, args_var)
    {ctx_ast, op_ast, args_ast, body_ast}
  end


  defp generate_delegate_body(_caller, {:__aliases__, _, _} = mod) do
    ctx_var = Macro.var(:ctx, __MODULE__)
    op_var = Macro.var(:op, __MODULE__)
    args_var = Macro.var(:args, __MODULE__)
    body_ast = quote do
      unquote(mod).perform(unquote(ctx_var), unquote(op_var), unquote(args_var))
    end
    {ctx_var, op_var, args_var, body_ast}
  end

  defp generate_delegate_body(_caller, expr_ast) do
    ctx_var = Macro.var(:ctx, __MODULE__)
    op_var = Macro.var(:op, __MODULE__)
    args_var = Macro.var(:args, __MODULE__)
    body_ast = quote generated: true do
      case unquote(expr_ast) do
        mod when is_atom(mod) ->
          mod.perform(unquote(ctx_var), unquote(op_var), unquote(args_var))
        _ ->
          Convex.Errors.unknown_operation!(unquote(ctx_var))
      end
    end
    {ctx_var, op_var, args_var, body_ast}
  end


  defp generate_argument(nil, nil), do: Macro.var(:_, __MODULE__)

  defp generate_argument(var, var), do: var

  defp generate_argument(nil, var), do: var

  defp generate_argument(ast, nil), do: ast

  defp generate_argument(ast, var) do
    quote do: unquote(ast) = unquote(var)
  end


  defp convert_operation(caller, {:=, info, [a, b]}) do
    {:=, info, [convert_operation(caller, a), convert_operation(caller, b)]}
  end

  defp convert_operation(caller, op) when is_binary(op) do
    case Convex.Operation.ast_from_string(op) do
      {:ok, ast} -> ast
      :error ->
        raise CompileError,
              description: "invalide operation #{inspect op}",
              file: caller.file, line: caller.line
    end
  end

  defp convert_operation(_caller, ast), do: ast

end