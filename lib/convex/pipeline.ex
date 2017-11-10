defmodule Convex.Pipeline do

  @moduledoc """
  Module to create and perform pipeline of operations with a simple syntax.


  ## Perform Syntax

  There is two syntaxes to perfrom a pipeline, `perform` and `perform!`.
  The difference is that when using a blocking context,
  `perform` will return `{:ok, result}` or `{:error, reason}`,
  and `perform!` will return the result directly or raise an exception.


  ### Simple Example

  After importing this module you can use the following syntax to perform a
  pipeline of operation:

    ```Elixir
    perform ctx do
      foo.bar num: 1, txt: "toto"
      spam.bacon arg: true
    end

    ```

  The behaviour of this pipeline will be different in function of the context
  that is used (more exacly in function of the context's callback module).
  If the context is `Convex.Context.Sync`, this will block until all the
  operations are performed. It will use the configured director to route
  the context to the service handling the operation `foo.bar` and perform
  it with the arguments `%{num: 1, txt: "toto"}`. Then it will use the director
  again to route the context to the service handling operation `spam.bacon`
  and perform it with arguments `%{arg: true}`. Then the result of the last
  operation will be returned.

  The operations can optionaly be specified with the arguments enclosed
  in parentesis:

    ```Elixir
    perform ctx do
      foo.bar(num: 1, txt: "toto")
      spam.bacon(arg: true)
    end

    ```


  ### Operation Arguments

  An argument to the operations in a pipeline can be any variable defined outside
  the pipeline scope if it's "pinned" with `^` (Like with `Ecto` queries):

    ```Elixir
    foo = 123
    bar = "toto"
    buz = false
    perform ctx do
      foo.bar num: ^foo, txt: ^bar
      spam.bacon arg: ^buz
    end

    ```

  If you already have all the operation arguments in a map variable you can
  simply pass it by prefixing it with `^`:

    ```Elixir
    args = %{a: 1, b:2}
    perform ctx, do: some.operation ^args

    ```

  This is equivalent to:

    ```Elixir
    a = 1; b = 2
    perform ctx, do: some.operation(a: ^a, b: ^b)

    ```

  In the case you have a map variable with arguments but still need to provide
  some more you can extend it with:

    ```Elixir
    args = %{a: 1, b:2}
    perform ctx, do: some.operation %{^args | c: 3}

    ```

  This is equivalent to:

    ```Elixir
    a = 1; b = 2; c = 3
    perform ctx, do: some.operation(a: ^a, b: ^b, c: ^c)

    ```

  A special argument is `ctx`. It is used to pass the context itself
  or one of its *public* field as argument. If the `auth`, `sess` or `policy`
  fields implement the `Access` protocol, sub-fields can be
  deferenced with the dotted notation:

    ```Elixir
    perform ctx do
      first.operation auth_value: ctx.auth.value, sess: ctx.sess, policy: ctx.policy
      second.operation debug: ctx
    end

    ```


  ### Stored Values

  Any result of an intermediate operation can be stored to later use.
  To do that, assigne the operation to a name and use it as argument
  to the following operation (without prefixing it with `^`):

    ```Elixir
    perform ctx do
      val = first.operation arg: "foo"
      second.operation arg: val
    end

    ```

  Stored values can be dereferenced using the dotted notation in the following
  operations if they implement the `Access` protocol:

    ```Elixir
    perform ctx do
      val = first.operation arg: "foo"
      second.operation arg: val.sub.value
    end

    ```

  In addition, the intermediary results can be *unpacked* from maps and tuples
  to multiple stored values:

    ```Elixir
    perform ctx do
      %{a: var1, b: var2} = first.operation arg: "foo"
      {var3, var4} = second.operation arg: var1
      third.operation a: var2, b: var3, c: var4
    end

    ```

  ### Pipeline Result

  By default the pipeline result value is the result of the last operation.

  It is possible to return a stored value as the pipeline result, in this case
  the result of the last operation will be ignored:

    ```Elixir
    perform ctx do
      value = first.operation arg: "foo"
      second.operation arg: value
      value
    end

    ```

  In addition, the pipeline result can be maps or a tuples *repacked*
  from multiple stored values:

    ```Elixir
    perform ctx do
      %{a: var1, b: var2} = first.operation arg: "foo"
      {var3, var4, var5} = second.operation arg: "bar"
      %{foo: var1, bar: {var2, var3, %{spam: var4, bacon: var5}}}
    end

    ```

  Special return values are `ctx` and its *public* fields. If the `auth`,
  `sess` or `policy` values implement the `Access` protocol, sub-fields can be
  deferenced with the dotted notation:

    ```Elixir
    perform ctx do
      some.operation ^args
      {ctx, ctx.auth.value, ctx.sess, ctx.policy}
    end

    ```


  ### Perform Arguments

  If the context is not authenticated, you can pass the authentication data
  to use to perform the pipeline as an argument:

    ```Elixir
    perform with: ctx, as: auth_data do
      first.operation
      second.operation
    end

    ```

  In this case the context will be first authenticated with given authentication
  data, and the context policy will be the default one if configured
  (See `Convex.Config`). If you want to specify an explicit policy you can do:

    ```Elixir
    perform with: ctx, as: auth_data, policy: policy_data do
      first.operation
      second.operation
    end

    ```


  ## Prepare Syntax

  Another tool provided by this module is the `prepare` syntax.
  This is used to create an operation pipeline without performing it.
  It can later be performed by calling `Convex.Context.perform/2`,
  `Convex.Context.perform/3`, `Convex.Context.perform!/2`
  or `Convex.Context.perform!/3`:

    ```Elixir
    pipe = prepare do
      res1 = first.operation a: ^a, b: true
      res2 = second.operation ^args
      {res1, res2}
    end
    Convex.Context.perfrom(ctx, pipe)

    ```


  ## Fork Syntax

  And finally, this module provide a syntax to simplify simple
  forks in the operation handlers. If an operation handler needs to generate
  multiple results from multiple functions it can do:

    ```Elixir
      ctx = fork ctx do
        Foo.foo(spam, bacon)
        bar
        buz(eggs)
      end
    ```

  This is equivalent to:

    ```Elixir
      {ctx1, frk1} = Convex.Context.fork(ctx)
      frk1 = Foo.foo(frk1, spam, bacon)
      {ctx2, frk2} = Convex.Context.fork(ctx1)
      frk2 = bar(frk2)
      {ctx3, frk3} = Convex.Context.fork(ctx2)
      frk3 = buz(frk3, eggs)
      ctx = Convex.Context.join(ctx3, [frk1, frk2, frk3])
    ```
  """

  #===========================================================================
  # Types
  #===========================================================================

  @type t :: term


  #===========================================================================
  # Macros
  #===========================================================================


  @spec perform(Ctx.t | Keyword.t, Keyword.t)
    :: {:ok, result :: any} | {:error, reason :: term}
  @doc """
  Macro to perform a pipeline of operation.

  See the module documentation for more information.
  """

  defmacro perform(opts, blocks) when is_list(opts) do
    def_fun_ast = quote do: Convex.Context.perform
    fun_ast = Keyword.get(opts, :handler, def_fun_ast)
    ctx = Keyword.fetch!(opts, :with)
    {_, ctx_opts} = Keyword.split(opts, [:as, :with, :handler])
    {pipe, post} = parse_pipeline(blocks)
    case Keyword.fetch(opts, :as) do
      :error -> gen_perform(pipe, post, ctx, nil, nil, fun_ast, ctx_opts)
      {:ok, auth_ast} ->
        policy_ast = case Keyword.get(opts, :policy) do
          nil -> Macro.escape(Convex.Config.default_policy())
          policy ->  policy
        end
        gen_perform(pipe, post, ctx, auth_ast, policy_ast, fun_ast, ctx_opts)
    end
  end

  defmacro perform(ctx, blocks) do
    fun_ast = quote do: Convex.Context.perform
    {pipe, post} = parse_pipeline(blocks)
    gen_perform(pipe, post, ctx, nil, nil, fun_ast, [])
  end


  @spec perform!(Ctx.t | Keyword.t, Keyword.t) :: result :: any
  @doc """
  Macro to perform a pipeline of operation.

  It will raise an exception if any operation fail.

  See the module documentation for more information.
  """

  defmacro perform!(opts, blocks) when is_list(opts) do
    def_fun_ast = quote do: Convex.Context.perform!
    fun_ast = Keyword.get(opts, :handler, def_fun_ast)
    ctx = Keyword.fetch!(opts, :with)
    {_, ctx_opts} = Keyword.split(opts, [:as, :with, :handler])
    {pipe, post} = parse_pipeline(blocks)
    case Keyword.fetch(opts, :as) do
      :error -> gen_perform(pipe, post, ctx, nil, nil, fun_ast, ctx_opts)
      {:ok, auth_ast} ->
        policy_ast = case Keyword.get(opts, :policy) do
          nil -> Macro.escape(Convex.Config.default_policy())
          policy ->  policy
        end
        gen_perform(pipe, post, ctx, auth_ast, policy_ast, fun_ast, ctx_opts)
    end
  end

  defmacro perform!(ctx, blocks) do
    fun_ast = quote do: Convex.Context.perform!
    {pipe, post} = parse_pipeline(blocks)
    gen_perform(pipe, post, ctx, nil, nil, fun_ast, [])
  end


  @spec prepare(Keyword.t) :: pipeline :: Pipeline.t
  @doc """
  Create a pipeline without initiating it.

  See the module documentation for more information.
  """

  defmacro prepare(blocks) do
    {pipe, nil} = parse_pipeline(blocks)
    quote do: unquote(pipe)
  end


  @spec fork(Ctx.t, Keyword.t) :: context :: Ctx.t
  @doc """
  Macro to fork an operation in a simple way.

  See the module documentation for more information.
  """

  defmacro fork(ctx_ast, blocks) do
    {last_ctx_ast, forked_ctx_list, calls_blocks} =
      parse_fork_calls(1, blocks, ctx_ast, [], [])
    join_call = quote do
      Convex.Context.join(unquote(last_ctx_ast), unquote(forked_ctx_list))
    end
    calls_ast = :lists.append(Enum.reverse([[join_call] | calls_blocks]))
    {:__block__, [], calls_ast}
  end


  #===========================================================================
  # Internal Structures
  #===========================================================================

  defmodule ASTParserState do
    @moduledoc false
    defstruct [
      store_keys: [],
      key: nil,
      val: nil,
      args: nil,
      name:  nil,
      type: nil,
      pack: nil,
      store_key: nil,
      store_val: nil,
      mutations: nil,
      pipe: nil
    ]
  end

  alias Convex.Pipeline.ASTParserState, as: This


  #===========================================================================
  # Internal Functions
  #===========================================================================

  defp gen_perform(pipe, nil, ctx, nil, _, fun_ast, ctx_opts) do
    {fun_name_ast, fun_ctx, _} = fun_ast
    {fun_name_ast, fun_ctx, [ctx, pipe, ctx_opts]}
  end

  defp gen_perform(pipe, nil, ctx, auth_ast, policy_ast, fun_ast, ctx_opts) do
    ctx_ast = quote do
      Convex.Context.authenticate(unquote(ctx), unquote(auth_ast), unquote(policy_ast))
    end
    {fun_name_ast, fun_ctx, _} = fun_ast
    {fun_name_ast, fun_ctx, [ctx_ast, pipe, ctx_opts]}
  end

  defp gen_perform(pipe, post, ctx, nil, _, fun_ast, ctx_opts) do
    {fun_name_ast, fun_ctx, _} = fun_ast
    {:case, [], [{fun_name_ast, fun_ctx, [ctx, pipe, ctx_opts]}, [do: post]]}
  end

  defp gen_perform(pipe, post, ctx, auth_ast, policy_ast, fun_ast, ctx_opts) do
    ctx_ast = quote do
      Convex.Context.authenticate(unquote(ctx), unquote(auth_ast), unquote(policy_ast))
    end
    {fun_name_ast, fun_ctx, _} = fun_ast
    {:case, [], [{fun_name_ast, fun_ctx, [ctx_ast, pipe, ctx_opts]}, [do: post]]}
  end


  defp parse_pipeline(ast), do: parse_pipeline(%This{}, ast, nil)


  defp parse_pipeline(this, [{a, _} | _] = kw, nil) when is_atom(a) do
    parse_pipeline(this, Keyword.get(kw, :do), Keyword.get(kw, :else))
  end

  defp parse_pipeline(this, {:__block__, _, specs_ast}, post) do
    parse_pipeline(this, specs_ast, post)
  end

  defp parse_pipeline(this, [{_, _, _} | _] = ast, post) do
    %This{this | pipe: []}
      |> reduce_ast(ast, &parse_spec/2)
      |> make_pipeline(post)
  end

  defp parse_pipeline(this, {_, _, _} = spec_ast, post) do
    %This{this | pipe: []}
      |> parse_spec(spec_ast)
      |> make_pipeline(post)
  end

  defp parse_pipeline(this, [], post) do
    %This{this | pipe: []}
      |> make_pipeline(post)
  end

  defp parse_pipeline(this, nil, post) do
    %This{this | pipe: []}
      |> make_pipeline(post)
  end


  defp make_pipeline(this, post), do: {Enum.reverse(this.pipe), post}


  defp parse_spec(this, {:=, _, [store_ast, spec_ast]}) do
    %This{this | store_val: nil, store_key: nil, mutations: []}
      |> parse_store(store_ast)
      |> parse_operation_or_result(spec_ast)
      |> make_spec()
  end

  defp parse_spec(this, {_, _, _} = spec_ast) do
    %This{this | store_val: nil, store_key: nil, mutations: []}
      |> parse_operation_or_result(spec_ast)
      |> make_spec()
  end

  defp parse_spec(this, {_, _} = ast) do
    %This{this | store_val: nil, mutations: []}
      |> parse_operation_or_result(ast)
      |> make_spec()
  end

  defp parse_spec(this, ast) when is_list(ast) do
    %This{this | store_val: nil, store_key: nil, mutations: []}
      |> parse_operation_or_result(ast)
      |> make_spec()
  end


  defp make_spec(%This{type: :pack, store_val: val, pack: fun} = this) do
    %This{this | pipe: [{:{}, [], [:pack, fun, val]} | this.pipe]}
      |> merge_store_keys()
  end

  defp make_spec(%This{type: :perform, store_val: val} = this) do
    spec = {:{}, [], [this.name, this.args, Enum.reverse(this.mutations), val]}
    %This{this | pipe: [spec | this.pipe]}
      |> merge_store_keys()
  end


  defp merge_store_keys(%This{store_key: nil} = this), do: this

  defp merge_store_keys(%This{store_key: key, store_keys: keys} = this)
   when is_atom(key) do
    %This{this | store_keys: [key | keys]}
  end

  defp merge_store_keys(%This{store_key: new_keys, store_keys: keys} = this) do
    %This{this | store_keys: merge_keys(new_keys, keys)}
  end


  defp parse_store(this, {_, _} = ast) do
    {fun, new_keys} = unpack_function(ast)
    %This{this | store_key: new_keys, store_val: fun}
  end

  defp parse_store(this, {:{}, _, _} = ast) do
    {fun, new_keys} = unpack_function(ast)
    %This{this | store_key: new_keys, store_val: fun}
  end

  defp parse_store(this, {:%{}, _, _} = ast) do
    {fun, new_keys} = unpack_function(ast)
    %This{this | store_key: new_keys, store_val: fun}
  end

  defp parse_store(this, ast) when is_list(ast) do
    {fun, new_keys} = unpack_function(ast)
    %This{this | store_key: new_keys, store_val: fun}
  end

  defp parse_store(this, {name, _, a}) when is_atom(a) and is_atom(name) do
    %This{this | store_key: name, store_val: name}
  end

  defp parse_store(_this, ast), do: error_bad_store_var(ast)


  defp parse_operation_or_result(this, {_, _} = ast) do
    %This{this | type: :pack, pack: pack_function(this.store_keys, ast)}
  end

  defp parse_operation_or_result(this, {:{}, _, _} = ast) do
    %This{this | type: :pack, pack: pack_function(this.store_keys, ast)}
  end

  defp parse_operation_or_result(this, {:%{}, _, _} = ast) do
    %This{this | type: :pack, pack: pack_function(this.store_keys, ast)}
  end

  defp parse_operation_or_result(this, ast) when is_list(ast) do
    %This{this | type: :pack, pack: pack_function(this.store_keys, ast)}
  end

  defp parse_operation_or_result(this, ast) do
    case parse_dotted(ast) do
      [:ctx | _] ->
        %This{this | type: :pack, pack: pack_function(this.store_keys, ast)}
      [store_key | _]->
        if store_key in this.store_keys do
          %This{this | type: :pack, pack: pack_function(this.store_keys, ast)}
        else
          parse_operation(this, ast)
        end
      _ -> parse_operation(this, ast)
    end
  end


  defp parse_operation(this, {name_ast, _, args}) when args in [nil, []] do
    %This{this | name: [], type: :perform, args: nil}
      |> parse_name(name_ast)
      |> parse_arguments([])
  end

  defp parse_operation(this, {name_ast, _, [args_ast]}) do
    %This{this | name: [], type: :perform, args: nil}
      |> parse_name(name_ast)
      |> parse_arguments(args_ast)
  end

  defp parse_operation(_this, ast), do: error_bad_operation(ast)


  defp parse_arguments(this, {:^, _, [{_, _, _} = ast]}) do
    %This{this | args: runtime_ensure_map(ast)}
  end

  defp parse_arguments(this, {:%{}, _, [{:|, _, [{:^, _, [{_, _, _} = ast]}, args]}]}) do
    this2 = parse_arguments(%This{this | args: []}, args)
    mutation = {:{}, [], [:merge, runtime_ensure_map(ast)]}
    %This{this2 | mutations: [mutation | this2.mutations]}
  end

  defp parse_arguments(_this, {_, _, _} = ast) do
    error_bad_arguments(ast)
  end

  defp parse_arguments(this, ast) when is_list(ast) do
    %This{this | args: []}
      |> reduce_ast(ast, &parse_argument/2)
      |> make_arguments()
  end


  defp make_arguments(%This{args: args} = this) do
    %This{this | args: {:%{}, [], args}}
  end


  defp parse_argument(this, {k, v}) do
    %This{this | key: nil, val: nil}
      |> parse_key(k)
      |> parse_value(v)
      |> make_argument()
  end


  defp make_argument(%This{key: nil} = this), do: this

  defp make_argument(%This{args: args, key: key, val: val} = this) do
    %This{this | args: [{key, val} | args]}
  end


  defp parse_name(this, ast) when is_atom(ast) do
    %This{this | name: [ast]}
  end

  defp parse_name(this, ast) do
    case parse_dotted(ast) do
      :error -> error_bad_operation(ast)
      name -> %This{this | name: name}
    end
  end


  defp parse_key(this, k) when is_atom(k), do: %This{this | key: k}

  defp parse_key(_this, ast), do: error_bad_argument_key(ast)


  defp parse_value(this, ast), do: parse_value(this, ast, ast)


  defp parse_value(this, full, ast) do
    case parse_dotted(ast) do
      :error -> parse_basic_value(this, full, ast)
      [:ctx | [field | _] = path] when field in [:auth, :sess, :policy] ->
        mutation = {:{}, [], [:ctx, this.key, path]}
        %This{this | key: nil, mutations: [mutation | this.mutations]}
      [:ctx | _] -> error_bad_ref(ast)
      [store_key | path] ->
        if store_key in this.store_keys do
          mutation = {:{}, [], [:store, this.key, store_key, path]}
          %This{this | key: nil, mutations: [mutation | this.mutations]}
        else
          error_unknown_store_ref(ast)
        end
    end
  end


  defp parse_basic_value(this, _full, ast)
    when is_atom(ast) or is_number(ast) or is_binary(ast),
    do: %This{this | val: ast}

  defp parse_basic_value(this, full, l) when is_list(l) do
    {this, rev_items} = Enum.reduce l, {this, []}, fn v, {this, acc} ->
      new_this = parse_basic_value(this, full, v)
      {new_this, [new_this.val | acc]}
    end
    %This{this | val: Enum.reverse(rev_items)}
  end

  defp parse_basic_value(this, _full, {:@, _, _} = ast),
    do: %This{this | val: ast}

  defp parse_basic_value(this, full, {a, b}) do
    this = parse_basic_value(this, full, a)
    value_a = this.val
    this = parse_basic_value(this, full, b)
    value_b = this.val
    value = {value_a, value_b}
    %This{this | val: value}
  end

  defp parse_basic_value(this, full, {:{}, tup_ctx, l}) do
    {this, rev_items} = Enum.reduce l, {this, []}, fn v, {this, acc} ->
      new_this = parse_basic_value(this, full, v)
      {new_this, [new_this.val | acc]}
    end
    value = {:{}, tup_ctx, Enum.reverse(rev_items)}
    %This{this | val: value}
  end

  defp parse_basic_value(this, full, {:%{}, map_ctx, kw}) do
    {this, rev_items} = Enum.reduce kw, {this, []}, fn v, {this, acc} ->
      new_this = parse_basic_value(this, full, v)
      {new_this, [new_this.val | acc]}
    end
    value = {:%{}, map_ctx, Enum.reverse(rev_items)}
    %This{this | val: value}
  end

  defp parse_basic_value(this, _full, {:^, _, [v]}), do: %This{this | val: v}

  defp parse_basic_value(_this, full, {_, _, _}), do: error_sub_mutation(full)

  defp parse_basic_value(_this, full, _), do: error_bad_argument_value(full)


  defp parse_dotted(ast), do: parse_dotted(ast, [])


  defp parse_dotted({:., _, [sub_ast, name]}, acc) when is_atom(name) do
    parse_dotted(sub_ast, [name | acc])
  end

  defp parse_dotted({bad_name, _, ns}, _acc)
    when bad_name in [:{}, :%{}] and ns in [[], nil] do
      :error
  end

  defp parse_dotted({name, _, ns}, acc)
    when is_atom(name) and ns in [[], nil] do
      [name | acc]
  end

  defp parse_dotted({name, _, ns}, acc)
    when ns in [[], nil] do
      parse_dotted(name, acc)
  end

  defp parse_dotted(_, _), do: :error


  defp pack_function(keys, ast) do
    ctx_var = Macro.var(:ctx, __MODULE__)
    pack_ast = pack_value(ctx_var, keys, ast)
    quote do: fn unquote(ctx_var) -> unquote(pack_ast) end
  end


  defp pack_value(ctx_var, keys, ast), do: pack_value(ctx_var, keys, ast, ast)


  defp pack_value(ctx_var, keys, full, ast) do
    case parse_dotted(ast) do
      :error -> pack_basic_value(ctx_var, keys, full, ast)
      [:ctx] ->
        quote do: Convex.Context.compact(unquote(ctx_var))
      [:ctx | [field | _] = path] when field in [:auth, :sess, :policy] ->
        quote do:  get_in(unquote(ctx_var), unquote(path))
      [:ctx | _] ->
        error_bad_ref(ast)
      [store_key | _] = path ->
        case Enum.member?(keys, store_key) do
          false -> error_unknown_store_ref(ast)
          true -> quote do: get_in(unquote(ctx_var).store, unquote(path))
        end
    end
  end


  defp pack_basic_value(_ctx_var, _keys, _full, ast)
    when is_binary(ast) or is_number(ast) or is_atom(ast),
    do: ast

  defp pack_basic_value(_ctx_var, _keys, _full, {:^, _, [ast]}), do: ast

  defp pack_basic_value(ctx_var, keys, full, {a, b}) do
    {pack_value(ctx_var, keys, full, a), pack_value(ctx_var, keys, full, b)}
  end

  defp pack_basic_value(ctx_var, keys, full, values) when is_list(values) do
    for val <- values, do: pack_value(ctx_var, keys, full, val)
  end

  defp pack_basic_value(ctx_var, keys, full, {:{}, ast_ctx, values}) when is_list(values) do
    {:{}, ast_ctx, (for val <- values, do: pack_value(ctx_var, keys, full, val))}
  end

  defp pack_basic_value(ctx_var, keys, full, {:%{}, ast_ctx, values}) when is_list(values) do
    items = for {k, v} <- values do
      case k do
        key when is_atom(key) or is_binary(key) or is_number(key) ->
          {k, pack_value(ctx_var, keys, full, v)}
        _ -> error_bad_expression(full)
      end
    end
    {:%{}, ast_ctx, items}
  end

  defp pack_basic_value(_ctx_var, _keys, full, _ast) do
    error_bad_expression(full)
  end


  defp unpack_function(ast) do
    store_var = Macro.var(:store, __MODULE__)
    {unpack_ast, vars} = unpack_value(%{}, ast)
    store_ast = Enum.reduce vars, store_var, fn {key, ast}, store_ast ->
      quote do: Map.put(unquote(store_ast), unquote(key), unquote(ast))
    end
    ast = quote do: fn ctx, result ->
      unquote(unpack_ast) = result
      unquote(store_var) = ctx.store
      {%Convex.Context{ctx | store: unquote(store_ast)}, result}
    end
    {ast, Map.keys(vars)}
  end

  defp unpack_value(vars, {:_, _, _} = ast), do: {ast, vars}

  defp unpack_value(vars, {name, _, namespace} = ast)
   when is_atom(name) and is_atom(namespace) do
    if Map.has_key?(vars, name), do: error_unpack_duplicated_ref(ast)
    size = Map.size(vars)
    var = Macro.var(:"var_#{size}", __MODULE__)
    {var, Map.put(vars, name, var)}
  end

  defp unpack_value(vars, value)
   when is_atom(value) or is_binary(value) or is_number(value) do
    {value, vars}
  end

  defp unpack_value(vars, {a, b}) do
    {a_ast, vars2} = unpack_value(vars, a)
    {b_ast, vars3} = unpack_value(vars2, b)
    {{a_ast, b_ast}, vars3}
  end

  defp unpack_value(vars, {tag, x, values}) when tag in [:{}, :%{}] do
    {rev_ast, vars3} = Enum.reduce values, {[], vars}, fn val, {acc, vars} ->
      {ast, vars2} = unpack_value(vars, val)
      {[ast | acc], vars2}
    end
    {{tag, x, Enum.reverse(rev_ast)}, vars3}
  end

  defp unpack_value(vars, values) when is_list(values) do
    {rev_ast, vars3} = Enum.reduce values, {[], vars}, fn val, {acc, vars} ->
      {ast, vars2} = unpack_value(vars, val)
      {[ast | acc], vars2}
    end
    {Enum.reverse(rev_ast), vars3}
  end


  defp merge_keys(l1, l2) do
    i1 = for v <- l1, do: {v, nil}
    i2 = for v <- l2, do: {v, nil}
    Map.keys(Map.merge(Enum.into(i1, %{}), Enum.into(i2, %{})))
  end

  defp runtime_ensure_map(ast) do
    quote do
      case unquote(ast) do
        [{_, _} | _] = args -> Enum.into(args, %{})
        args when is_map(args) -> args
      end
    end
  end


  defp reduce_ast(this, ast, fun) do
    Enum.reduce(ast, this, fn a, t -> fun.(t, a) end)
  end


  defp parse_fork_calls(c, [{:do, block} | _], ctx_ast, forks_ast, acc) do
    parse_fork_calls(c, block, ctx_ast, forks_ast, acc)
  end

  defp parse_fork_calls(c, {:__block__, _, calls}, ctx_ast, forks_ast, acc) do
    parse_fork_calls(c, calls, ctx_ast, forks_ast, acc)
  end

  defp parse_fork_calls(c, {_, _, _} = call, ctx_ast, forks_ast, acc) do
    #TODO: could be optimized to not fork if there is a single item
    parse_fork_calls(c, [call], ctx_ast, forks_ast, acc)
  end

  defp parse_fork_calls(_c, [], ctx_ast, forks_ast, acc) do
    {ctx_ast, forks_ast, acc}
  end

  defp parse_fork_calls(c, [call | calls], ctx_ast, forks_ast, acc) do
    {cast, fast, ast} = generate_fork_call(c, call, ctx_ast)
    parse_fork_calls(c + 1, calls, cast, [fast | forks_ast], [ast | acc])
  end


  defp generate_fork_call(c, {{:., _, _} = name, env, args}, ctx_ast) do
    generate_fork_call(c, name, env, args, ctx_ast)
  end

  defp generate_fork_call(c, {name, env, space}, ctx_ast)
   when is_atom(name) and is_atom(space) do
    generate_fork_call(c, name, env, [], ctx_ast)
  end

  defp generate_fork_call(c, {name, env, args}, ctx_ast)
   when is_atom(name) and is_list(args) do
    generate_fork_call(c, name, env, args, ctx_ast)
  end

  defp generate_fork_call(_c, call_ast, _ctx_ast) do
    raise ArgumentError,
      message: "invalid fork call: #{Macro.to_string(call_ast)}"
  end


  defp generate_fork_call(c, name, env, args, ctx_ast) do
    cvar = Macro.var(:"pipe_fork_ctx#{c}", __MODULE__)
    fvar1 = Macro.var(:"pipe_fork_frk#{c}a", __MODULE__)
    fvar2 = Macro.var(:"pipe_fork_frk#{c}b", __MODULE__)
    call = {name, env, [fvar1 | args]}
    {:__block__, _, ast} = quote do
      {unquote(cvar), unquote(fvar1)} = Convex.Context.fork(unquote(ctx_ast))
      unquote(fvar2) = unquote(call)
    end
    {cvar, fvar2, ast}
  end


  defp error_bad_arguments(ast) do
    raise ArgumentError,
      message: "invalid operation argument: #{Macro.to_string(ast)} (use ^ to reference outer scope)"
  end


  defp error_bad_argument_value(ast) do
    raise ArgumentError,
      message: "invalid operation argument value: #{Macro.to_string(ast)} (use ^ to reference outer scope)"
  end


  defp error_bad_argument_key(ast) do
    raise ArgumentError,
      message: "invalid operation argument key: #{Macro.to_string(ast)}"
  end


  defp error_bad_operation(ast) do
    raise ArgumentError,
      message: "invalid opeation: #{Macro.to_string(ast)}"
  end


  defp error_bad_store_var(ast) do
    raise ArgumentError,
      message: "invalid operation storage variable: #{Macro.to_string(ast)}"
  end


  defp error_bad_ref(ast) do
    raise ArgumentError,
      message: "invalid operation reference: #{Macro.to_string(ast)}"
  end


  defp error_unknown_store_ref(ast) do
    raise ArgumentError,
      message: "reference to unknown stored value: #{Macro.to_string(ast)}"
  end


  defp error_unpack_duplicated_ref(ast) do
    raise ArgumentError,
      message: "multiple reference to the same stored value in unpacking statment: #{Macro.to_string(ast)}"
  end


  defp error_sub_mutation(ast) do
    raise ArgumentError,
      message: "store or context references cannot be deep in another structure: #{Macro.to_string(ast)}"
  end


  defp error_bad_expression(ast) do
    raise ArgumentError,
      message: "invalid expression: #{Macro.to_string(ast)}"
  end

end
