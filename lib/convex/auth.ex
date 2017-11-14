defprotocol Convex.Auth do

  @moduledoc """
  Protocol to let `Convex` print the content of the context `auth` field.

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

  @spec describe(auth :: any) :: String.t
  @doc """
  This function returns a string description of the specified `auth` value.
  """

  def describe(auth)

end


defimpl Convex.Auth, for: Any do

  @spec describe(any) :: String.t

  def describe(nil), do: "anonymous"

  def describe(auth) when is_binary(auth), do: auth

  def describe(auth) when is_atom(auth), do: Atom.to_string(auth)

  def describe(_), do: "unknown"

end
