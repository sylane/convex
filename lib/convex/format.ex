defmodule Convex.Format do

  @moduledoc """
  Helper module to format `Convex` related data.
  """

  #===========================================================================
  # Includes
  #===========================================================================

  alias Convex.Context, as: Ctx


  #===========================================================================
  # API Functions
  #===========================================================================

  @spec opname(op :: Ctx.op) :: String.t
  @doc """
  Formats a `Convex` operation to string.

  ```Elixir
  opname([:foo, :bar, :buz]) = "foo.bar.buz"
  opname(nil) = "invalid"
  ```

  If the opreation is invalid it returns the string `"invalid"`.
  """

  def opname([_ | _] = op), do: Enum.join(op, ".")

  def opname(_), do: "invalid"


  @spec opdesc(op :: Ctx.op, args :: map) :: String.t
  @doc """
  Format an operation and its arguments to string.

  ```Elixir
  opdesc([:foo, :bar], %{}) -> "foo.bar"
  opdesc([:foo, :bar], %{a: 1, b: 2}) -> "foo.bar(a: a, b: 2)"
  opdesc(nil, %{a: 1, b: 2}) -> "invalid(a: a, b: 2)"
  opdesc([:foo, :bar], nil) -> "invalid"
  ```
  """

  def opdesc(op, args) when is_map(args) do
    case for {k, v} <- args, do: "#{k}: #{inspect v}" do
      [] -> opname(op)
      args_items ->
        "#{opname(op)}(#{Enum.join(args_items, ", ")})"
    end
  end

  def opdesc(_op, _args), do: "invalid"

end
