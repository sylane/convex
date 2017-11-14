defmodule Convex do

  @moduledoc """
  Convex is a library to simplify communication between application services.

  It provides a functional-flavored mix of
  [visitor pattern](https://en.wikipedia.org/wiki/Visitor_pattern)
  and [command pattern](https://en.wikipedia.org/wiki/Command_pattern).

  ##### TODO High-level description of when to use this library. What types of systems \
  does it simplify? Diagram?

  ## Concepts

  The concept is to split responsibilities in three parts:

   - The services providing the simplest operations possible.
   - The director that knows which service provide which operations.
   - The initiator (could be a service itself) that decides what operations
     to perform with what arguments, and the way it wants to get the
     results back.


  ## Example

  Take an hypothetical application composed of a user service, and an API service
  where the user service itself uses an indexing service and a rendering service.

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

    1. The API creates a context that defines how it will receive the result.
       It could be synchronous, asynchronous, sending messages to a websocket
       process...
    2. The API creates a pipeline and begins executing it with the context:
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
       operation that will produce multiple user ids for the key.
    5. The context is forked and routed to the rendering service
       for each produced user id to perform the `user.render` operation
       (possibly in parallel).
    6. The API received all the results in the manner defined by the context
       it created.

  The advantages of this solution are:

    - The services only implement simple operations with less *hard-coded*
      inter-service calls.
    - There are no synchronous calls involved, so none of the services are
      ever blocked waiting for another service.
    - The initiator decides how the results are received.
    - There are fewer inter-process messages because the execution uses
      the visitor pattern to transition execution from service to service
      without returning to the initiator between each operation step.
    - The services can be loosely coupled, and don't require knowledge of
      how the initiator wants to receive the results.

  Adding a new service to the application would only require updating the
  routing for operations (the director), and adding a new API with a
  different behaviour (HTTP vs websocket) wouldn't require changes to any
  services.


  ## Testing

  Another advantage of interfacing all the inter-service communications
  is that it is very simple to test a service.

  To isolate a service, you only need to create your own testing director
  and use it in the test. This way you can selectively route operations
  to a real service or mock them out.


  ## Definitions

  #### Operation

  An operation is a name and some arguments that some service could perform.
  An operation name is a list of atoms, although the director declaration
  supports the dotted string representation to reduce noise.

  Using the `o` sigil, the dotted string representation can be used.

  See `Convex.Sigils`.


  #### Director

  A director is a module that knows how to route an operation to a service
  by its name and arguments.

  Each application needs to create its own director.

  Directors can be defined as a tree of modules, so each service could provide
  their own director and the application need only provide a root director that
  delegates to each service director in function of the operation name.

  See `Convex.Director`.


  #### Pipeline

  A pipeline is a list of operations (with arguments) and the data
  dependencies between the operations.
  The operations in a pipeline are executed sequentially.
  Each operation can use the results of previous one as arguments.
  Operations can fork the pipeline so the remaining operations are
  performed for every produced value at that step.

  See `Convex.Pipeline`.


  #### Context

  A context is what encapsulates all the data describing a pipeline of operations.
  It is what is passed around between services in order to complete the pipeline
  of operations.

  It is composed of a standard interface that any service can interact with, and
  an internal implementation defined by the initiator that defines how the
  operations should be processed during the execution.
  For example there are 3 built-in callback modules for the context that can
  be used as-is or customized:

    - `Convex.Context.Async` provides a non-blocking fire-and-forget behavior,
      the operations are executed and the results are ignored.
    - `Convex.Context.Sync` provides a blocking call that returns when all the
      operations are done or returns immediately on any failure.
    - `Convex.Context.Process` provides a non-blocking call that will send
      the execution status and result to a given process on completion.
      `Convex.Handler.Process` can be used to process these messages.

  This design allows an HTTP API service using the `Convex.Context.Sync`
  backend or a websocket API service using the `Convex.Context.Process` backend
  to perform the same operations without the dependent services requiring special
  knowledge about the API requesting an operation to be performed.

  See `Convex.Context`.


  #### Proxy

  A proxy is a communication channel with another service that can be
  established in order to be notified of events emitted from this service. For
  example a stateful websocket API may want to notify a connected client about
  some asynchronous event affecting it from the user service.

  In order to do that, a service operation handler has the option to request
  a proxy from the operation context. It can then hold onto this proxy and use
  it at some later time to relay messages back to the initiator of the pipeline.

  Like the context, this proxy has a standard interface that services can
  interact with, and an internal callback module that defines its behaviour. The
  callback module is usually defined by the context's own callback module.
  This way the services does not need to know how the initiator expects to
  receive the notification messages.

  See `Convex.Proxy`.


  ## Configuration

  The minimal configuration for an application using Convex should specify
  the `director` option:

  ```
  config :convex,
    director: MyApp.MyDirector
  ```

  ### Configuration Keys

  #### `director`

    The director module `Convex` should use to route operations.
    The module **must** implement the `Convex.Director` behaviour.

  #### `error_handler`

    A module handling errors during operation execution.
    The module **must** implement the `Convex.ErrorHandler` behaviour.

  #### `config_handler`

    A module handling `Convex` configuration.
    The module **must** implement the `Convex.Config` behaviour.

  #### e.g.

  ```
  config :convex,
    director: MyApp.MyDirector,
    error_handler: MyApp.MyConvexErrorHandler,
    config_handler: MyApp.MyConvexConfigHandler

  ```
  """

end
