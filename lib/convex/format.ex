defmodule Convex.Format do

  #===========================================================================
  # API Functions
  #===========================================================================

  def opname([_ | _] = op), do: Enum.join(op, ".")

  def opname(_), do: "invalid"


  def opdesc(op, args) do
    args_items = for {k, v} <- args, do: "#{k}: #{inspect v}"
    "#{opname(op)}(#{Enum.join(args_items, ", ")})"
  end

end
