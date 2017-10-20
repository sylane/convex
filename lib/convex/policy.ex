defprotocol Convex.Policy do

  @fallback_to_any true

  @type t :: any

  def describe(auth)

end


defimpl Convex.Policy, for: Any do

  def describe(nil), do: "undefined"

  def describe(policy) when is_binary(policy), do: policy

  def describe(policy) when is_atom(policy), do: Atom.to_string(policy)

  def describe(_), do: "unknown"

end

