defprotocol Convex.Sess do

  @moduledoc """
  Protocol to let `Convex` print the content of the context `sess` field.

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


  @spec describe(sess :: any) :: String.t
  @doc """
  This function returns a string description of the specified `sess` value.
  """

	def describe(sess)

end


defimpl Convex.Sess, for: Any do

  def describe(nil), do: "detached"

  def describe(sess) when is_binary(sess), do: sess

  def describe(sess) when is_atom(sess), do: Atom.to_string(sess)

  def describe(_), do: "unknown"

end
