defmodule Convex.Errors do

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

  @spec not_implemented!(Ctx.t) :: no_return
  @spec not_implemented!(Ctx.t, String.t) :: no_return

  def not_implemented!(ctx, message \\ "not implemented"),
  do: do_raise(not_implemented(ctx, message))


  @spec not_implemented(Ctx.t) :: {:error, struct}
  @spec not_implemented(Ctx.t, String.t) :: {:error, struct}

  def not_implemented(ctx, message \\ "not implemented") do
    Convex.OperationError.exception(
      reason: :not_implemented, context: ctx,
      debug: Ctx.arguments(ctx), message: message)
      |> wrap_in_error
  end


  @spec unknown_operation!(Ctx.t) :: no_return

  def unknown_operation!(ctx), do: do_raise(unknown_operation(ctx))


  @spec unknown_operation(Ctx.t) :: {:error, struct}

  def unknown_operation(ctx) do
    desc = Format.opdesc(Ctx.operation(ctx), Ctx.arguments(ctx))
    Convex.OperationError.exception(
      reason: :unknown_operation, context: ctx,
      debug: Ctx.arguments(ctx),
      message: "unknown operation #{desc}")
      |> wrap_in_error
  end


  @spec invalid_operation!(Ctx.t, String.t) :: no_return

  def invalid_operation!(ctx, msg), do: do_raise(invalid_operation(ctx, msg))


  @spec invalid_operation(Ctx.t, String.t) :: {:error, struct}

  def invalid_operation(ctx, msg) do
    Convex.OperationError.exception(
      reason: :invalid_operation, context: ctx,
      debug: Ctx.arguments(ctx), message: msg)
      |> wrap_in_error
  end



  @spec process_not_found!(Ctx.t, term) :: no_return

  def process_not_found!(ctx, proc_ref),
  do: do_raise(process_not_found(ctx, proc_ref))


  @spec process_not_found(Ctx.t, term) :: {:error, struct}

  def process_not_found(ctx, proc_ref) do
    Convex.OperationError.exception(
      reason: :process_not_found, context: ctx,
      message: "process #{inspect proc_ref} not found")
      |> wrap_in_error
  end


  @spec not_authenticated!(Ctx.t) :: no_return
  @spec not_authenticated!(Ctx.t, String.t) :: no_return

  def not_authenticated!(ctx, msg \\ "not authenticated"),
  do: do_raise(not_authenticated(ctx, msg))


  @spec not_authenticated(Ctx.t) :: {:error, struct}
  @spec not_authenticated(Ctx.t, String.t) :: {:error, struct}

  def not_authenticated(ctx, message \\ "not authenticated") do
    Convex.OperationError.exception(
      reason: :not_authenticated, context: ctx,
      debug: Ctx.arguments(ctx), message: message)
      |> wrap_in_error
  end


  @spec not_attached!(Ctx.t) :: no_return
  @spec not_attached!(Ctx.t, String.t) :: no_return

  def not_attached!(ctx, msg \\ "not attached"),
  do: do_raise(not_attached(ctx, msg))


  @spec not_attached(Ctx.t) :: {:error, struct}
  @spec not_attached(Ctx.t, String.t) :: {:error, struct}

  def not_attached(ctx, message \\ "not attached") do
    Convex.OperationError.exception(
      reason: :not_attached, context: ctx,
      debug: Ctx.arguments(ctx), message: message)
      |> wrap_in_error
  end



  @spec already_authenticated!(Ctx.t, Types.auth) :: no_return

  def already_authenticated!(ctx, auth),
  do: do_raise(already_authenticated(ctx, auth))


  @spec already_authenticated(Ctx.t, Types.auth) :: {:error, struct}

  def already_authenticated(ctx, auth) do
    Convex.OperationError.exception(
      reason: :already_authenticated, context: ctx,
      message: "context already authenticated:\nCurrent Authentication: #{Auth.describe(ctx.auth)}\nNew Authentication: #{Auth.describe(auth)}")
      |> wrap_in_error
  end



  @spec already_attached!(Ctx.t, Types.sess) :: no_return

  def already_attached!(ctx, sess), do: do_raise(already_attached(ctx, sess))


  @spec already_attached(Ctx.t, Types.sess) :: {:error, struct}

  def already_attached(ctx, sess) do
    Convex.OperationError.exception(
      reason: :already_attached, context: ctx,
      message: "context already attached:\nAuthentication: #{Auth.describe(ctx.auth)}\nCurrent Session: #{Sess.describe(ctx.sess)}\nNew Session: #{Sess.describe(sess)}")
      |> wrap_in_error
  end


  @spec forbidden!(Ctx.t) :: no_return
  @spec forbidden!(Ctx.t, String.t) :: no_return

  def forbidden!(ctx, msg \\ "access forbidden"), do: do_raise(forbidden(ctx, msg))


  @spec forbidden(Ctx.t) :: {:error, struct}
  @spec forbidden(Ctx.t, String.t) :: {:error, struct}

  def forbidden(ctx, message \\ "access forbidden") do
    Convex.OperationError.exception(
      reason: :forbidden, context: ctx,
      debug: Ctx.arguments(ctx), message: message)
      |> wrap_in_error
  end


  @spec internal!(Ctx.t) :: no_return
  @spec internal!(Ctx.t, String.t) :: no_return

  def internal!(ctx, msg \\ "internal error"), do: do_raise(internal(ctx, msg))


  @spec internal(Ctx.t) :: {:error, struct}
  @spec internal(Ctx.t, String.t) :: {:error, struct}

  def internal(ctx, message \\ "internal error") do
    Convex.OperationError.exception(
      reason: :internal_error, context: ctx,
      debug: Ctx.arguments(ctx), message: message)
      |> wrap_in_error
  end


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


  def log_trace({_, [{m, f, _, _} | _] = trace}) when is_atom(m) and is_atom(f) do
    Logger.debug("stacktrace:\n#{Exception.format_stacktrace(trace)}")
  end

  def log_trace(_), do: :ok


  def stacktrace({_, [{m, f, _, _} | _] = trace}) when is_atom(m) and is_atom(f), do: trace

  def stacktrace(_), do: nil


  def error({error, [{m, f, _, _} | _]}) when is_atom(m) and is_atom(f), do: error

  def error(error), do: error


  def class({error, [{m, f, _, _} | _]}) when is_atom(m) and is_atom(f), do: class(error)

  def class(%{__struct__: struct}), do: struct

  def class(_), do: :unknown


  def reason({error, [{m, f, _, _} | _]}) when is_atom(m) and is_atom(f), do: reason(error)

  def reason(%Error{reason: reason}), do: reason

  def reason(%OperationError{reason: reason, debug: nil}), do: reason

  def reason(%OperationError{reason: reason, debug: debug}), do: {reason, debug}

  def reason(%{__exception__: true, __struct__: _, reason: reason}), do: reason

  def reason(%{__exception__: true, __struct__: struct}), do: struct

  def reason(reason), do: reason


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
