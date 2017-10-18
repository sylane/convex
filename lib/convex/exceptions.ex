defmodule Convex.Error do

  defexception [:message, :reason, :exception]

  def exception(params) do
    %Convex.Error{
      message: Keyword.fetch!(params, :message),
      reason: Keyword.get(params, :reason, :internal_error),
      exception: Keyword.get(params, :exception),
    }
  end

end


defmodule Convex.OperationError do

  defexception [:message, :reason, :context, :debug, :exception]

  def exception(params) do
    %Convex.OperationError{
      message: Keyword.fetch!(params, :message),
      reason: Keyword.get(params, :reason, :internal_error),
      context: Keyword.get(params, :context),
      debug: Keyword.get(params, :debug),
      exception: Keyword.get(params, :exception),
    }
  end

end
