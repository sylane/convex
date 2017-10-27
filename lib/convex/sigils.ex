defmodule Convex.Sigils do

  @moduledoc """
  Defines `Convex` sigils.

  To use these sigils you need to import this module:
  ```Elixir
  import Convex.Sigils
  ```

  # `o` Sigil

  Converts a string to an operation name at compilation time.
  It supports partial operation for pattern matching.

  e.g.
  ```Elixir
  ~o"foo" = [:foo]
  ~o"foo.bar" = [:foo, :bar]
  ~o"foo.bar.*" = [:foo, :bar]
  ~o"foo.bar.*" = [:foo, :bar, :buz]
  ~o"foo.bar.*" = [:foo, :bar, :buz, :boz]
  ~o"*" = [:spam]
  ```

  Using `~o"foo.bar.*"` will be replaced at compilation time
  by `[:foo, :bar | _]`.
  """

  #===========================================================================
  # Includes
  #===========================================================================

  alias Convex.Operation


  #===========================================================================
  # API Functions
  #===========================================================================

  @spec sigil_o(op_str :: String.t, options :: []) :: any
  @doc """
  Generate an operation from a string.
  See `Convex.Sigils` module documentation.
  """

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
