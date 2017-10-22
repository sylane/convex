defmodule Convex.ContextTest do

  #===========================================================================
  # Sub-Modules
  #===========================================================================

  defmodule TestDirector do
    alias Convex.Context, as: Ctx
    import Convex.Pipeline
    def perform(ctx, [:test, :map], %{values: values}) do
      Ctx.map ctx, values, fn frk, v -> Ctx.done(frk, v * 10) end
    end
    def perform(ctx, _op, _args), do: Ctx.failed(ctx, :unknown_operation)
  end


  #===========================================================================
  # Includes
  #===========================================================================

  use ExUnit.Case, async: true

  import Convex.Pipeline

  alias Convex.Context.Sync
  alias Convex.ContextTest.TestDirector


  #===========================================================================
  # Tests Setup
  #===========================================================================

  setup do
    {:ok, [ctx: Sync.new(director: TestDirector)]}
  end


  #===========================================================================
  # Test Cases
  #===========================================================================

  test "map", %{ctx: ctx} do
    assert [] = test_map(ctx, [])
    assert [10] = test_map(ctx, [1])
    assert [10, 20, 30] = Enum.sort(test_map(ctx, [1, 2, 3]))
  end


  #===========================================================================
  # Internal Functions
  #===========================================================================

  def test_map(ctx, values) do
    perform! with: ctx do
      test.map values: ^values
    end
  end

end
