defmodule Convex do

  #===========================================================================
  # API Functions
  #===========================================================================

  def director() do
    case Application.get_env(:convex, :director) do
      nil -> raise Convex.Error, message: "Convex director configuration is missing"
      director -> director
    end
  end


  def default_policy() do
    Application.get_env(:convex, :default_policy)
  end

end
