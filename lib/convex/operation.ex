defmodule Convex.Operation do

  #===========================================================================
  # Attributes
  #===========================================================================

  @operation_rx ~r/^(?:\*|[a-z][a-zA-Z0-9_]*(?:\.[a-z][a-zA-Z0-9_]*)*(?:\.\*)?)$/


  #===========================================================================
  # API Functions
  #===========================================================================

  defmacro from_string(op_ast) do
    case ast_from_string(op_ast) do
      {:ok, ast} -> ast
      :error ->
        raise CompileError,
              description: "invalide operation #{inspect op_ast}",
              file: __CALLER__.file, line: __CALLER__.line
    end
  end


  def ast_from_string({:<<>>, _, [op_str]}), do: ast_from_string(op_str)

  def ast_from_string(op_str) when is_binary(op_str) do
    case Regex.run(@operation_rx, op_str) do
      nil -> :error
      _ ->
        parts = Enum.map(String.split(op_str, "."), &String.to_atom/1)
        case Enum.split(parts, -2) do
          {[], [:*]} -> {:ok, quote do: [_|_]}
          {rest, [last, :*]} ->
            tail = [{:|, [], [last, {:_, [], nil}]}]
            {:ok, rest ++ tail}
          _ -> {:ok, parts}
        end
    end
  end

end