defmodule Convex.Config do

  @moduledoc """
  Handle the configuration for Convex library.

  An application can change the behaviour of `Convex` by specifying a
  configuration handler implementing this baviour in its configuration.

  For now, the only configuration is the default policy that will be used
  when performing a pipeline with the `as` argument and without specifying
  any explicit policy.
  """

  #===========================================================================
  # Behaviour Definition
  #===========================================================================

  @doc """
  Returns the default policy to use when performing a pipline with the `as`
  argument but without any explicit `policy`.
  """
  @callback default_policy() :: term


  #===========================================================================
  # API Functions
  #===========================================================================

  @spec director() :: module
  @doc """
  Returns the configured director module.
  """

  def director() do
    case Application.get_env(:convex, :director) do
      nil -> raise Convex.Error, message: "Convex director configuration is missing"
      director -> director
    end
  end


  @spec default_policy() :: any
  @doc """
  Returns the configured default policy.
  """

  def default_policy() do
    case Application.get_env(:convex, :config_handler) do
      nil -> nil
      mod when is_atom(mod) -> mod.default_policy()
    end
  end

end