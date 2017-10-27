defmodule Convex do

  @moduledoc """
  Convex is a library to add a layer of comunication between multiple
  services without any of these services to have to know each others.

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