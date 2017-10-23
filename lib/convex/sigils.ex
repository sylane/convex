defmodule Convex.Sigils do

  #===========================================================================
  # Includes
  #===========================================================================

  alias Convex.Operation


  #===========================================================================
  # API Functions
  #===========================================================================

  defmacro sigil_o(op_str, []) do
    case Operation.ast_from_string(op_str) do
      {:ok, ast} -> ast
      :error ->
        raise CompileError,
              description: "invalide operation #{inspect op_str}",
              file: __CALLER__.file, line: __CALLER__.line
    end
  end

end
