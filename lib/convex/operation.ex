defmodule Convex.Operation do

  @moduledoc """
  Helper module to convert a string to the proper internal format.
  """

  #===========================================================================
  # Attributes
  #===========================================================================

  @operation_rx ~r/^(?:\*|[a-z][a-zA-Z0-9_]*(?:\.[a-z][a-zA-Z0-9_]*)*(?:\.\*)?)$/


  #===========================================================================
  # API Functions
  #===========================================================================

  @spec from_string(opname :: String.t) :: any
  @doc """
  Converts a string to the internal representation (list of atoms).

  It suports partial operation for pattern matching:

  ```
  from_string("foo.bar") -> [:foo, :bar]
  from_string("foo.bar.*") -> [:foo, :bar | _]
  from_string("*") -> [_ | _]
  ```
  """

  defmacro from_string(op_ast) do
    case ast_from_string(op_ast) do
      {:ok, ast} -> ast
      :error ->
        raise CompileError,
              description: "invalide operation #{inspect op_ast}",
              file: __CALLER__.file, line: __CALLER__.line
    end
  end


  @spec ast_from_string(op_ast :: any) :: ast :: any
  @doc """
  Generate the AST for an operation given the AST of a string.

  **ONLY** to be used in macros.
  """

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