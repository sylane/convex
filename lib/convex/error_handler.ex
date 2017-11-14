defmodule Convex.ErrorHandler do

  @moduledoc """
  Defines the behaviour the application must implement and specify with the
  configuration key `error_handler` (See `Convex` documentation).

  The error handler is used to decide what to do with errors raise while
  performing operations.
  The handler will be called with non-system exceptions and can decide to:
    - Catch the exception (still logging a message) (`:error` response).
    - Raise the exception (`:raise` response).
    - Silently fail the operation (`:failed` response).
    - Silently succeed the operation (`:done` response).
    - Let the default error handler handle the exception (`:pass` response).
  """

  #===========================================================================
  # Includes
  #===========================================================================

  alias Convex.Context, as: Ctx


  #===========================================================================
  # Behaviour Definition
  #===========================================================================

  @doc """
  Decides what to do with a non-system exception raised while performing
  an operation.

  Possible responses:
    - `{:error, ctx, reason, message, debug}`:

      The error will be logged, and the operation fill fail with given reason.

    - `{:raise, ctx, reason, message, debug}`:

      The error will be logged, and the operation fill fail with given reason
      and the exception will be re-raise possibly bubbling up and killing
      the service.

    - `{:failed, ctx, reason}`:

      The operation will silently fail with the given reason.

    - `{:done, ctx, result}`:

      The operation will succeed with given result.

    - `:pass`:

      The error will be handled by the default handler, it will try to extract
      a reason from the error, the operation will fail with this extracted
      reason and the exception will be re-raised.
  """
  @callback context_error(context :: Ctx.t, error :: term)
    :: {:error, Ctx.t, reason :: term, message :: String.t, debug :: any}
     | {:raise, Ctx.t, reason :: term, message :: String.t, debug :: any}
     | {:failed, Ctx.t, reason :: term}
     | {:done, Ctx.t, result :: term}
     | :pass


  #===========================================================================
  # Attributes
  #===========================================================================

  @system_errors [
    ArgumentError,
    ArithmeticError,
    BadArityError,
    BadBooleanError,
    BadFunctionError,
    BadMapError,
    BadStructError,
    CaseClauseError,
    CondClauseError,
    Enum.EmptyError,
    Enum.OutOfBoundsError,
    FunctionClauseError,
    IO.StreamError,
    Inspect.Error,
    KeyError,
    MatchError,
    Protocol.UndefinedError,
    Regex.CompileError,
    RuntimeError,
    SyntaxError,
    TryClauseError,
    UndefinedFunctionError,
    WithClauseError,
  ]


  #===========================================================================
  # API Functions
  #===========================================================================

  @doc false
  def handle(ctx, %type{__exception__: true} = error) when type in @system_errors do
    raise_error(ctx, error)
  end

  def handle(ctx, error) do
    case Application.get_env(:convex, :error_handler) do
      nil -> raise_error(ctx, error)
      handler ->
        try do
          handler.context_error(ctx, error)
        rescue
          _ -> raise_error(ctx, error)
        else
          :pass -> raise_error(ctx, error)
          result -> result
        end
    end
  end


  #===========================================================================
  # Internal Function
  #===========================================================================

  defp raise_error(ctx, %type{__exception__: true} = error) do
    desc = Map.get(error, :message, Exception.message(error))
    message = "#{to_string(type)}:  #{desc}"
    reason = type
    debug = "#{Exception.format_stacktrace(System.stacktrace())}"
    {:raise, ctx, reason, message, debug}
  end

end
