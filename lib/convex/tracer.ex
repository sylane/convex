defmodule Convex.Tracer do

  @moduledoc false

  #===========================================================================
  # Includes
  #===========================================================================

  alias Convex.Format


  #===========================================================================
  # Macros
  #===========================================================================

    defmacro __using__([]) do
      quote do
        require Logger
        alias Convex.Format
        import unquote(__MODULE__),
          only: [cvx_trace: 3, cvx_trace: 4, cvx_trace: 5, cvx_trace: 6]
      end
    end


    defmacro cvx_trace(tag, ctx, args) do
      state = quote do: unquote(ctx).state
      depth = quote do: unquote(ctx).depth
      forked = quote do: unquote(ctx).forked
      numbers = quote do: [unquote(depth), unquote(forked)]
      op = quote do: Convex.Context.operation(unquote(ctx))
      quote do
        cvx_trace(unquote(tag), unquote(state), unquote(numbers),
                 unquote(op), unquote(args))
      end
    end


    defmacro cvx_trace(tag, ctx, args, result) do
      state = quote do: unquote(ctx).state
      depth = quote do: unquote(ctx).depth
      forked = quote do: unquote(ctx).forked
      numbers = quote do: [unquote(depth), unquote(forked)]
      op = quote do: Convex.Context.operation(unquote(ctx))
      quote do
        cvx_trace(unquote(tag), unquote(state), unquote(numbers),
                 unquote(op), unquote(args), unquote(result))
      end
    end


    defmacro cvx_trace(tag, state, numbers, op, args, result \\ {}) do
      mod = _macro_module(__CALLER__.module)
      case _traced_ops() do
        nil -> :ok
        :all -> _macro_trace(mod, tag, state, numbers, op, args, result)
        ops ->
          statments = for spec <- ops do
            case spec do
              {:value, top} ->
                {:->, [], [[top],
                 _macro_trace(mod, tag, state, numbers, top, args, result)]}
              {:match, match} ->
                var = Macro.var(:op, __MODULE__)
                {:->, [], [[{:=, [], [match, var]}],
                 _macro_trace(mod, tag, state, numbers, var, args, result)]}
            end
          end
          all_statments = statments ++ [{:->, [], [[{:_, [], Elixir}], :ok]}]
          {:case, [], [op, [do: all_statments]]}
      end
    end


  #===========================================================================
  # Internal Functions
  #===========================================================================

  defp _macro_module(mod) do
    case Module.split(mod) do
      [] -> "Unknown"
      parts -> List.last(parts)
    end
  end


  defp _traced_ops() do
    case System.get_env("CVX_TRACE") do
    nil -> nil
    value ->
      case String.downcase(value) do
        disabled when disabled in ["false", "no"] -> nil
        enabled when enabled in ["true", "yes", "all"] -> :all
        opnames ->
          parse_opname = fn parts ->
            case Enum.split(parts, length(parts) - 2) do
              {firsts, [last, :*]} ->
                match = firsts ++ [{:|, [], [last, {:_, [], Elixir}]}]
                {:match, match}
              _ -> {:value, parts}
            end
          end
          opnames
            |> String.split(",")
            |> Enum.map(&String.trim/1)
            |> Enum.map(&String.split(&1, "."))
            |> Enum.map(fn op -> Enum.map(op, &String.to_atom/1) end)
            |> Enum.map(parse_opname)
      end
    end
  end


  defp _macro_format_mod(mod) when is_binary(mod) do
    String.pad_trailing(to_string(mod), 8)
  end


  defp _macro_format_self() do
    quote do: String.pad_trailing(List.to_string(:erlang.pid_to_list(self())), 10)
  end


  defp _macro_format_tag(tag) when is_binary(tag) do
    String.pad_trailing(tag, 10)
  end

  defp _macro_format_tag(tag) do
    quote do: String.pad_trailing(unquote(tag), 10)
  end


  defp _macro_format_numbers(numbers) when is_list(numbers) do
    parts = for n <- numbers do
      ast = quote do: inspect(unquote(n))
      {:::, [], [ast, {:binary, [], Elixir}]}
    end
    ast = {:<<>>, [], Enum.intersperse(parts, ":")}
    quote do
      String.pad_trailing(unquote(ast), 5)
    end
  end

  defp _macro_format_numbers({_, _, _} = num) do
    quote do: String.pad_trailing(inspect(unquote(num)), 5)
  end


  defp _macro_format_state(nil), do: "          "

  defp _macro_format_state(state) do
    quote do: String.pad_trailing(to_string(unquote(state)), 10)
  end


  defp _macro_format_op({_, _, _} = op) do
    quote do: Format.opname(unquote(op))
  end

  defp _macro_format_op([a | _] = op) when is_atom(a) do
    Format.opname(op)
  end


  defp _macro_format_args([]), do: ""

  defp _macro_format_args([{a, _} | _] = kw) when is_atom(a) do
    quote do
      Enum.join(Enum.map(unquote(kw), fn {k, v} -> "#{k}: #{inspect(v)}" end), ", ")
    end
  end

  defp _macro_format_args(args) do
    quote do
      Enum.join(Enum.map(unquote(args), fn {k, v} -> "#{k}: #{inspect(v)}" end), ", ")
    end
  end


  defp _macro_format_result({}), do: ""

  defp _macro_format_result(result) do
    quote do: ": #{inspect(unquote(result))}"
  end


  defp _macro_trace(mod, tag, state, numbers, op, args, result) do
    line = {:<<>>, [], [
      {:::, [], [_macro_format_self(), {:binary, [], Elixir}]}, " ",
      {:::, [], [_macro_format_mod(mod), {:binary, [], Elixir}]}, " ",
      {:::, [], [_macro_format_state(state), {:binary, [], Elixir}]}, " ",
      {:::, [], [_macro_format_numbers(numbers), {:binary, [], Elixir}]}, " ",
      {:::, [], [_macro_format_tag(tag), {:binary, [], Elixir}]}, " ",
      {:::, [], [_macro_format_op(op), {:binary, [], Elixir}]}, "(",
      {:::, [], [_macro_format_args(args), {:binary, [], Elixir}]}, ")",
      {:::, [], [_macro_format_result(result), {:binary, [], Elixir}]}
    ]}

    quote do
      Logger.error(unquote(line))
    end
  end

end
