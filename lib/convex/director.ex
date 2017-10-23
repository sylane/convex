defmodule Convex.Director do

  #===========================================================================
  # Behaviour Definition
  #===========================================================================

  @callback perform(Ctx.t, Ctx.op, map)
    :: {:ok, Ctx.t} | {:error, reason :: term}


  #===========================================================================
  # Attributes
  #===========================================================================

  @operation_rx ~r/^(?:\*|[a-z][a-zA-Z0-9_]*(?:\.[a-z][a-zA-Z0-9_]*)*(?:\.\*)?)$/


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

      @before_compile Convex.Director
    end
  end


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

  defp generate_perform_body(_caller, expr_ast) do
    ctx_var = Macro.var(:ctx, __MODULE__)
    body_ast = quote do
      case unquote(expr_ast) do
        %Convex.Context{} = result -> result
        :ok -> Convex.Context.done(unquote(ctx_var))
        {:ok, result} -> Convex.Context.done(unquote(ctx_var), result)
        {:error, reason} -> Convex.Context.failed(unquote(ctx_var), reason)
        {:produce, items} -> Convex.Context.produce(unquote(ctx_var), items)
      end
    end
    {ctx_var, nil, nil, body_ast}
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
    body_ast = quote do
      case unquote(expr_ast) do
        :ok -> {:ok, unquote(args_var)}
        {:ok, result} when is_map(result) -> {:ok, result}
        {:error, _reason} = error -> error
        result when is_map(result) -> {:ok, result}
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
    body_ast = quote do
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
    case Regex.run(@operation_rx, op) do
      nil -> raise CompileError,
                    description: "invalide operation #{inspect op}",
                    file: caller.file, line: caller.line
      _ ->
        parts = Enum.map(String.split(op, "."), &String.to_atom/1)
        case Enum.split(parts, -2) do
          {[], [:*]} -> quote do: [_|_]
          {rest, [last, :*]} ->
            tail = [{:|, [], [last, {:_, [], nil}]}]
            rest ++ tail
          _ -> parts
        end
    end
  end

  defp convert_operation(_caller, ast), do: ast

end