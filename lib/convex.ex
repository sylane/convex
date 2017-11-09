defmodule Convex do

  @moduledoc """
  Convex is a library to simplify comunication between application services.

  It provides a functional-flavored mix of
  [visitor pattern](https://en.wikipedia.org/wiki/Visitor_pattern)
  and [command pattern](https://en.wikipedia.org/wiki/Command_pattern).

  TODO

  ## Concepts

  The concept is to split responsabilities in three parts:

   - The services providing the simplest operations possible.
   - The director that knows which service provide which operations.
   - The initiator (could be a service itself) that decides what operations
     to perform with what arguments, and the way it wants to get the
     results back.


  ### Example

  Take an hypothetical application composed of a user service, and an API service
  where the user service itself uses and indexing service and a rendering service.

  To provide an API that returns a list of formated user given a key, a possible
  implementation could be:

    1. The API calls the user service entry point with the key and the user info.
    2. The user service calls the authentication service with the user info.
    3. The user service calls the index service with the key.
    4. The user service calls the rendering service for each result of the index.
    5. The API gets the result and send it back to the client.

  The simplest implementation would require `3 + N` synchronous calls that would
  block the user service process and the API process while waiting for a result.

  With `Convex`, the initiator is the one planning the execution:

    1. The API create a context that suites the way it wants to get the result.
       It could be synchronous, asynchronous, sending messages to a websocket
       process...
    2. The API creates a pipeline and perform it with the context:
      ```Elixir
      perform with: ctx do
        authenticate.user info: ^user_info
        user_id = index.lookup key: ^key
        user.render user_id: user_id
      end
      ```
    3. The context is routed to the authentication service to perform the
       `authenticate.user` operation that will authenticate the context.
    4. The context is routed to the indexing service perform the `index.lookup`
       operation that will produce the user ids for the key.
    5. The context is forked and routed to the rendering service
       for each produced user id to perform the `user.render` operations
       (possibly in parallel).
    6. The API gets all the results in function of the context it created.

  The advantages of this solution are:

    - The services only implement simple operations with less *hard-coded*
      inter-service calls.
    - There is no synchronous calls involved, so none of the services are
      ever blocked waiting for another service.
    - The initiator is the one deciding how the results are sent back.
    - There is less inter-process messages because the execution use
      the visitor pattern to jump from service to service without going
      back to the initiator between each operations.
    - The services doesn't have to know each-others, and doesn't have to
      know how the initiator wants to get the results back.

  Adding a new service to the application would only require updating the
  routing for operations (the director), and adding a new API with a
  different behaviour (HTTP vs Websocket) wouldn't require any changes to
  any services.


  ### Testing

  Another advantage of interfacing all the inter-service comunications
  is that it is very simple to test a service.

  To isolate a service, you only need to create your own testing director
  and use it in the test. This way you can selectively route operations
  to a real service or mock them out.


  ### Definitions

  #### Operation

  An operation is a name and some arguments that some service could perform.
  An operation name is a list of atoms, even though the director declaration
  supports the dotted string representation to reduce noise.

  Using the `o` sigil, the dotted string representation can be used.

  See `Convex.Sigils`.


  #### Director

  A director is a module that knows how to route an operation to a service
  by its name and arguments.

  Each application needs to create there own director.

  Directors can be defined as a tree of modules, so each service could provide
  there own director and the application only provide a root director that
  delegates to each service director in function of the operation name.

  See `Convex.Director`.


  #### Pipeline

  A pipeline is a list of operations with there arguments and the way there
  result should be stored.
  The operations in a pipeline are executed sequencially.
  Each operations can use the results of previouse one as arguments.
  Operations can fork the pipeline so the remaining operations are
  performed for every produced values.

  See `Convex.Pipeline`.


  #### Context

  A context is what encapsulate all the data describing a pipeline of operations.
  It is what is passed around between services in order to complete them.

  It is composed of a standard interface the service know about, and an internal
  part defined by the initiator that defines how what to do during the execution.
  For example there is 3 provided callback modules for the context that could
  be used as-is or customized:

    - `Convex.Context.Async` provides a non-blocking fire-and-forget behavior,
      the oeprations are executed and the results are ignored.
    - `Convex.Context.Sync` provides a blocking call that returns only when
      all the operations are done or any of them failed.
    - `Convex.Context.Process` provides a non-blocking call that will send
      the execution status and result to a given process.
      `Convex.Handler.Process` can be used to process these messages.

  An HTTP API service may use the `Convex.Context.Sync` backend
  and a websocket API service may use the `Convex.Context.Process` backend
  to perform the same operations without any services requiring special
  knowledge about the API requesting an operation to be performed.

  See `Convex.Context`.


  #### Proxy

  Sometime a service needs to setup a comunication channel with another service
  in order to get notifications. For example a statefull websocket API may want
  to notify a client asynchrnously of something.

  In order to do that, a service operation handler has the option to request
  a proxy from the operation context. It can then keep this proxy and use
  it at any moment to send messages to the initiator of the pipeline.

  Like the context, this proxy has a standard interface the service knows
  about, and an internal callback module that defines its behaviour. The
  callback module is usually defined byt the context own callback module.
  This way the service do not have to know how the peer service expects to
  receive the notification messages, multiple APIs could have different ways
  of notifying the client.

  See `Convex.Proxy`.


  ## Configuration

  The minimal configuration for an application using Convex should specify
  the `director` option:

  ```
  config :convex,
    director: MyApp.MyDirector,
  ```

  # Configuration Keys

  ## `director`

    The director module `Convex` should use to route operations.
    Thre module **MUST** implement the `Convex.Director` beaviour.

  ## `error_handler`

    A module handling errors during operation execution.
    It **MUST** implement the `Convex.ErrorHandler` behaviour.

  ## `config_handler`

    A module handling `Convex` configuration.
    It **MUST** implement the `Convex.Config` behaviour.

  # e.g.

  ```
  config :convex,
    director: MyApp.MyDirector,
    error_handler: MyApp.MyConvexErrorHandler,
    config_handler: MyApp.MyConvexConfigHandler

  ```
  """

end
