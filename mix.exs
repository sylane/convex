defmodule Convex.Mixfile do
  use Mix.Project

  def project do
    [ app: :convex,
      version: "0.1.0",
      elixir: "~> 1.4",
      elixirc_paths: elixirc_paths(Mix.env),
      build_embedded: Mix.env == :prod,
      start_permanent: Mix.env == :prod,
      consolidate_protocols: Mix.env != :test,
      deps: deps(),
      test_coverage: [tool: ExCoveralls],
      preferred_cli_env: [
        "coveralls": :test,
        "coveralls.html": :test,
        "coveralls.detail": :test,
        "coveralls.post": :test
      ],
      dialyzer: [
        plt_add_deps: :transitive,
        flags: [:error_handling, :race_conditions, :no_opaque],
      ],
    ]
  end


  defp elixirc_paths(:test), do: ["lib", "test/convex/support"]

  defp elixirc_paths(_),     do: ["lib"]


  def application do
    [extra_applications: [:logger]]
  end

  defp deps do
    [ {:excoveralls, "~> 0.6", only: :test},
      {:dialyxir, "~> 0.5", only: [:dev], runtime: false},
    ]
  end

end
