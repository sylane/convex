defmodule Convex.Error do

  @moduledoc """
  Generic exception used to raise `Convex` related errors.

  It contains the following fields:
    - `message`: The exception human-readable error message.
    - `reason`: The exception machine-readable term.
    - `exception`: Any other exception that got encapsulated.
  """

  defexception [:message, :reason, :exception]

  @doc false
  def exception(params) do
    %Convex.Error{
      message: Keyword.fetch!(params, :message),
      reason: Keyword.get(params, :reason, :internal_error),
      exception: Keyword.get(params, :exception),
    }
  end

end


defmodule Convex.OperationError do

  @moduledoc """
  Convex exception used to raise operation-related errors.

  It contains the following fields:
    - `message`: The exception human-readable error message.
    - `reason`: The exception machine-readable term.
    - `context`: The `Convex.Context` at the time the error was raise (id available).
    - `debug`: Any debugging information related to the error.
    - `exception`: Any other exception that got encapsulated.
  """

  defexception [:message, :reason, :context, :debug, :exception]

  @doc false
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
