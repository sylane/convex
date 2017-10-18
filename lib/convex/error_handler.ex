defmodule Convex.ErrorHandler do

  #===========================================================================
  # Includes
  #===========================================================================

  alias Convex.Context, as: Ctx


  #===========================================================================
  # Behaviour Definition
  #===========================================================================

  @callback context_error(Ctx.t, error :: term)
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