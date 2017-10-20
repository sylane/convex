defmodule Convex.Config do

  #===========================================================================
  # Behaviour Definition
  #===========================================================================

  @callback default_policy() :: term


  #===========================================================================
  # API Functions
  #===========================================================================

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
    case Application.get_env(:convex, :config_handler) do
      nil -> nil
      mod when is_atom(mod) -> mod.default_policy()
    end
  end

end