defprotocol Convex.Policy do

  @moduledoc """
  Protocol to let `Convex` print the content of the context `policy` field.

  Default implementation supports string and atom values.
  """

  #===========================================================================
  # Types
  #===========================================================================

  @type t :: any

  #===========================================================================
  # Attributes
  #===========================================================================

  @fallback_to_any true


  #===========================================================================
  # Protocol Interface
  #===========================================================================

  @spec  describe(policy :: any) :: String.t
  @doc """
  This function returns a string description of the specified `policy` value.
  """

  def describe(auth)

end


defimpl Convex.Policy, for: Any do

  def describe(nil), do: "undefined"

  def describe(policy) when is_binary(policy), do: policy

  def describe(policy) when is_atom(policy), do: Atom.to_string(policy)

  def describe(_), do: "unknown"

end
