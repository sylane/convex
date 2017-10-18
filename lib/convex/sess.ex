defprotocol Convex.Sess do

  @fallback_to_any true

	@type t :: any

	def describe(auth)

end


defimpl Convex.Sess, for: Any do

  def describe(nil), do: "detached"

  def describe(sess) when is_binary(sess), do: sess

  def describe(_), do: "unknown"

end

