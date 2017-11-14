defmodule Convex.Guards do

  @moduledoc """
  This module provides functions to call another function in the context of
  performing an operation is a safe way.

  They are mostly used internally by `Convex.Context` and should be used only
  if you exactly know what you are doing.
  """

  #===========================================================================
  # Includes
  #===========================================================================

  alias Convex.Context, as: Ctx
  alias Convex.ErrorHandler


  #===========================================================================
  # API Functions
  #===========================================================================

  @spec protect(context :: Ctx.t, fun :: (() -> any))
    :: {:ok, any} | {:error, context :: Ctx.t} | no_return
  @doc """
  Execute a function in the context of performing an operation in a safe way,
  by wrapping the execution with error handling try-rescue.

  If any exception is raised during the execution of the function, the current
  operation will be properly terminated.
  The exception may be re-raised in function of the configured
  `Convex.ErrorHandler` but the current operation will be handled properly.
  """

  def protect(ctx, fun) do
    try do
      {:ok, fun.()}
    rescue
      e in Convex.OperationError ->
        new_ctx = (e.context || ctx)
          |> Ctx.error(e.message)
          |> Ctx._internal_failure(e.reason)
        {:error, new_ctx}
      e in Convex.Error ->
        ctx
          |> Ctx.error(e.message)
          |> Ctx._internal_failure(e.reason, e.exception)
        reraise e, System.stacktrace()
      e ->
        case ErrorHandler.handle(ctx, e) do
          {:error, handler_ctx, reason, message, debug} ->
            new_ctx = handler_ctx
              |> Ctx.error(message)
              |> Ctx._internal_failure(reason, debug)
            {:error, new_ctx}
          {:raise, handler_ctx, reason, message, debug} ->
            handler_ctx
              |> Ctx.error(message)
              |> Ctx._internal_failure(reason, debug)
            reraise e, System.stacktrace()
          {:failed, handler_ctx, reason} ->
            {:ok, Ctx.failed(handler_ctx, reason)}
          {:done, handler_ctx, result} ->
            {:ok, Ctx.done(handler_ctx, result)}
        end
    end
  end


  @spec ensure_discharged(context :: Ctx.t)
    :: {:ok, context :: Ctx.t} | {:error, atom}
  @doc """
  Validates the given context has properly been discharged, meaning that
  current operation has been handled or delegated.

  It is used as a safe guard to detect any programming error.
  """

  def ensure_discharged(ctx) do
    if ctx.forked > 0 do
      Ctx._internal_failure(ctx, :internal_error, :forked_context)
      {:error, :forked_context}
    else
      case ctx.state do
        :running ->
          Ctx._internal_failure(ctx, :internal_error, :context_not_discharged)
          {:error, :context_not_discharged}
        :delegating ->
          Ctx._internal_failure(ctx, :internal_error, :context_not_delegated)
          {:error, :context_not_delegated}
        :forking ->
          Ctx._internal_failure(ctx, :internal_error, :context_not_joined)
          {:error, :context_not_joined}
        _ -> {:ok, ctx}
      end
    end
  end


  @spec ensure_discharged!(context :: Ctx.t) :: context :: Ctx.t | no_return
  @doc """
  Same as `ensure_discharged/1` but raise an error instead of returning
  an `:error` tuple.
  """

  def ensure_discharged!(ctx) do
    if ctx.forked > 0 do
      Ctx._internal_failure(ctx, :internal_error, :forked_context)
      raise Convex.Error, reason: :forked_context,
        message: "forked context should never be returned, they should be joined back to the forking context with function 'join'."
    else
      case ctx.state do
        :running ->
          Ctx._internal_failure(ctx, :internal_error, :context_not_discharged)
          raise Convex.Error, reason: :context_not_discharged,
            message: "context not properly discharged; one of the functions 'done', 'failed', 'stream', 'fork'/'join' or 'delegate' must be called."
        :delegating ->
          Ctx._internal_failure(ctx, :internal_error, :context_not_delegated)
          raise Convex.Error, reason: :context_not_delegated,
            message: "context delegation not completed; one of the functions 'delegate_done' or 'delegate_failed' must be called."
        :forking ->
          Ctx._internal_failure(ctx, :internal_error, :context_not_joined)
          raise Convex.Error, reason: :context_not_joined,
            message: "forked context not joined back; the functions 'join' must be called."
        _ -> ctx
      end
    end
  end

end
