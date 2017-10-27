defmodule Convex.Errors do

  @moduledoc """
  Helper module defining and raising common operation errors.

  In addition it provided functions to log exceptions.
  """

  #===========================================================================
  # Includes
  #===========================================================================

  require Logger

  alias Convex.Context, as: Ctx
  alias Convex.Error
  alias Convex.OperationError
  alias Convex.Auth
  alias Convex.Sess
  alias Convex.Format


  #===========================================================================
  # API Functions
  #===========================================================================

  @spec not_implemented!(context :: Ctx.t) :: no_return
  @spec not_implemented!(context :: Ctx.t, message :: String.t) :: no_return
  @doc """
  Raises the error created by `not_implemented/1` or `not_implemented/2`.
  """

  def not_implemented!(ctx, message \\ "not implemented"),
    do: do_raise(not_implemented(ctx, message))


  @spec not_implemented(context :: Ctx.t) :: {:error, struct}
  @spec not_implemented(context :: Ctx.t, message :: String.t) :: {:error, struct}
  @doc """
  Creates an operation error for operations not yet implemented.
  """

  def not_implemented(ctx, message \\ "not implemented") do
    Convex.OperationError.exception(
      reason: :not_implemented, context: ctx,
      debug: Ctx.arguments(ctx), message: message)
      |> wrap_in_error
  end


  @spec unknown_operation!(context :: Ctx.t) :: no_return
  @doc """
  Raises the error created by `unknown_operation/1`.
  """

  def unknown_operation!(ctx),
    do: do_raise(unknown_operation(ctx))


  @spec unknown_operation(context :: Ctx.t) :: {:error, struct}
  @doc """
  Creates an operation error for trying to perform an unknown operation.
  """

  def unknown_operation(ctx) do
    desc = Format.opdesc(Ctx.operation(ctx), Ctx.arguments(ctx))
    Convex.OperationError.exception(
      reason: :unknown_operation, context: ctx,
      debug: Ctx.arguments(ctx),
      message: "unknown operation #{desc}")
      |> wrap_in_error
  end


  @spec invalid_operation!(context :: Ctx.t, message :: String.t) :: no_return
  @doc """
  Raises the error created by `invalid_operation/2`.
  """

  def invalid_operation!(ctx, msg),
    do: do_raise(invalid_operation(ctx, msg))


  @spec invalid_operation(context :: Ctx.t, message :: String.t) :: {:error, struct}
  @doc """
  Creates an operation error for an invalid operation.
  """

  def invalid_operation(ctx, msg) do
    Convex.OperationError.exception(
      reason: :invalid_operation, context: ctx,
      debug: Ctx.arguments(ctx), message: msg)
      |> wrap_in_error
  end



  @spec process_not_found!(context :: Ctx.t, proc_ref :: term) :: no_return
  @doc """
  Raises the error created by `process_not_found/2`.
  """

  def process_not_found!(ctx, proc_ref),
    do: do_raise(process_not_found(ctx, proc_ref))


  @spec process_not_found(context :: Ctx.t, proc_ref :: term) :: {:error, struct}
  @doc """
  Creates an operation error due to a process not being found.
  """

  def process_not_found(ctx, proc_ref) do
    Convex.OperationError.exception(
      reason: :process_not_found, context: ctx,
      message: "process #{inspect proc_ref} not found")
      |> wrap_in_error
  end


  @spec not_authenticated!(context :: Ctx.t) :: no_return
  @spec not_authenticated!(context :: Ctx.t, message :: String.t) :: no_return
  @doc """
  Raises the error created by `not_authenticated/1` or `not_authenticated/2`.
  """

  def not_authenticated!(ctx, msg \\ "not authenticated"),
    do: do_raise(not_authenticated(ctx, msg))


  @spec not_authenticated(context :: Ctx.t) :: {:error, struct}
  @spec not_authenticated(context :: Ctx.t, message :: String.t) :: {:error, struct}
  @doc """
  Creates an operation error due to the context not being authenticated.
  """

  def not_authenticated(ctx, message \\ "not authenticated") do
    Convex.OperationError.exception(
      reason: :not_authenticated, context: ctx,
      debug: Ctx.arguments(ctx), message: message)
      |> wrap_in_error
  end


  @spec not_attached!(context :: Ctx.t) :: no_return
  @spec not_attached!(context :: Ctx.t, message :: String.t) :: no_return
  @doc """
  Raises the error created by `not_attached/1` or `not_attached/2`.
  """

  def not_attached!(ctx, msg \\ "not attached"),
    do: do_raise(not_attached(ctx, msg))


  @spec not_attached(context :: Ctx.t) :: {:error, struct}
  @spec not_attached(context :: Ctx.t, message :: String.t) :: {:error, struct}
  @doc """
  Creates an operation error due the context not being attached to any session.
  """

  def not_attached(ctx, message \\ "not attached") do
    Convex.OperationError.exception(
      reason: :not_attached, context: ctx,
      debug: Ctx.arguments(ctx), message: message)
      |> wrap_in_error
  end



  @spec already_authenticated!(context :: Ctx.t, curr_auth :: Auth.t) :: no_return
  @doc """
  Raises the error created by `already_authenticated/2`.
  """

  def already_authenticated!(ctx, auth),
    do: do_raise(already_authenticated(ctx, auth))


  @spec already_authenticated(context :: Ctx.t, curr_auth :: Auth.t) :: {:error, struct}
  @doc """
  Creates an operation error due the context being already authenticated.
  """

  def already_authenticated(ctx, auth) do
    Convex.OperationError.exception(
      reason: :already_authenticated, context: ctx,
      message: "context already authenticated:\nCurrent Authentication: #{Auth.describe(ctx.auth)}\nNew Authentication: #{Auth.describe(auth)}")
      |> wrap_in_error
  end



  @spec already_attached!(context :: Ctx.t, curr_sess :: Sess.t) :: no_return
  @doc """
  Raises the error created by `already_attached/2`.
  """

  def already_attached!(ctx, sess),
    do: do_raise(already_attached(ctx, sess))


  @spec already_attached(context :: Ctx.t, curr_sess :: Sess.t) :: {:error, struct}
  @doc """
  Creates an operation error due the context being already attached to a session.
  """

  def already_attached(ctx, sess) do
    Convex.OperationError.exception(
      reason: :already_attached, context: ctx,
      message: "context already attached:\nAuthentication: #{Auth.describe(ctx.auth)}\nCurrent Session: #{Sess.describe(ctx.sess)}\nNew Session: #{Sess.describe(sess)}")
      |> wrap_in_error
  end


  @spec forbidden!(context :: Ctx.t) :: no_return
  @spec forbidden!(context :: Ctx.t, message :: String.t) :: no_return
  @doc """
  Raises the error created by `forbidden/2`.
  """

  def forbidden!(ctx, msg \\ "access forbidden"),
    do: do_raise(forbidden(ctx, msg))


  @spec forbidden(context :: Ctx.t) :: {:error, struct}
  @spec forbidden(context :: Ctx.t, message :: String.t) :: {:error, struct}
  @doc """
  Creates an error due the operation execution being forbidden.
  """

  def forbidden(ctx, message \\ "access forbidden") do
    Convex.OperationError.exception(
      reason: :forbidden, context: ctx,
      debug: Ctx.arguments(ctx), message: message)
      |> wrap_in_error
  end


  @spec internal!(context :: Ctx.t) :: no_return
  @spec internal!(context :: Ctx.t, message :: String.t) :: no_return
  @doc """
  Raises the error created by `internal/2`.
  """

  def internal!(ctx, msg \\ "internal error"),
    do: do_raise(internal(ctx, msg))


  @spec internal(context :: Ctx.t) :: {:error, struct}
  @spec internal(context :: Ctx.t, message :: String.t) :: {:error, struct}
  @doc """
  Creates an operation error due some internal error.
  """

  def internal(ctx, message \\ "internal error") do
    Convex.OperationError.exception(
      reason: :internal_error, context: ctx,
      debug: Ctx.arguments(ctx), message: message)
      |> wrap_in_error
  end


  @spec log_exception(message :: nil | String.t, error :: any, trace :: nil | list) :: :ok
  @spec log_exception(message :: nil | String.t, error :: any, trace :: nil | list, tags :: Keyword.t) :: :ok
  @doc """
  Log and exception trying to extract the most information out of it.

  A custom message and an external stack trace can be specified.
  Extra tags can be given to log some context information.
  """

  def log_exception(msg, error, trace, tags \\ []) do
    m = message(error)
    e = msg || m
    c = class(error)
    r = reason(error)
    t = stacktrace(error) || trace
    if t == nil do
      Logger.error "#{e} (#{inspect r})"
    else
      # Only log the tags if there is a stacktrace.
      extra = for {k, v} <- tags do
        "#{String.capitalize(Atom.to_string(k))}: #{inspect v}\n"
      end
      Logger.error "#{e} (#{inspect r})\n#{Enum.join(extra)}** (#{c}) #{m}\n#{Exception.format_stacktrace(t)}"
    end
  end


  @doc false

  def log_trace({_, [{m, f, _, _} | _] = trace}) when is_atom(m) and is_atom(f) do
    Logger.debug("stacktrace:\n#{Exception.format_stacktrace(trace)}")
  end

  def log_trace(_), do: :ok


  @doc false

  def stacktrace({_, [{m, f, _, _} | _] = trace}) when is_atom(m) and is_atom(f), do: trace

  def stacktrace(_), do: nil


  @doc false

  def error({error, [{m, f, _, _} | _]}) when is_atom(m) and is_atom(f), do: error

  def error(error), do: error


  @doc false
  def class({error, [{m, f, _, _} | _]}) when is_atom(m) and is_atom(f), do: class(error)

  def class(%{__struct__: struct}), do: struct

  def class(_), do: :unknown


  @doc false
  def reason({error, [{m, f, _, _} | _]}) when is_atom(m) and is_atom(f), do: reason(error)

  def reason(%Error{reason: reason}), do: reason

  def reason(%OperationError{reason: reason, debug: nil}), do: reason

  def reason(%OperationError{reason: reason, debug: debug}), do: {reason, debug}

  def reason(%{__exception__: true, __struct__: _, reason: reason}), do: reason

  def reason(%{__exception__: true, __struct__: struct}), do: struct

  def reason(reason), do: reason


  @doc false
  def message({error, [{m, f, _, _} | _]}) when is_atom(m) and is_atom(f), do: message(error)

  def message(%{__exception__: true, __struct__: struct, message: nil}), do: to_string(struct)

  def message(%{__exception__: true, message: message}), do: message

  def message(%{__exception__: true} = error), do: Exception.message(error)

  def message(error), do: "#{inspect error}"


  #===========================================================================
  # Internal Functions
  #===========================================================================

  @spec do_raise({:error, struct}) :: no_return

  defp do_raise({:error, exception}), do: raise exception


  @spec wrap_in_error(arg) :: {:error, arg} when arg: var

  defp wrap_in_error(ex), do: {:error, ex}

end
