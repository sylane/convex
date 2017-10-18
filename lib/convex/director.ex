defmodule Convex.Director do

  #===========================================================================
  # Behaviour Definition
  #===========================================================================

  @callback perform(Ctx.t, Ctx.op, Keyword.t)
    :: {:ok, Ctx.t} | {:error, reason :: term}

end