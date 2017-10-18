defprotocol Convex.Auth do

  @fallback_to_any true

  @type t :: any

  def describe(auth)

end


defimpl Convex.Auth, for: Any do

  def describe(nil), do: "anonymous"

  def describe(auth) when is_binary(auth), do: auth

  def describe(_), do: "unknown"

end

