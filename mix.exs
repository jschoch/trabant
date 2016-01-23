defmodule Trabant.Mixfile do
  use Mix.Project

  def project do
    [app: :trabant,
     version: "0.0.1",
     elixir: "~> 1.0",
     build_embedded: Mix.env == :prod || Mix.env == :gamma,
     start_permanent: Mix.env == :prod,
     deps: deps]
  end

  # Configuration for the OTP application
  #
  # Type `mix help compile.app` for more information
  def application do
    [applications: [:logger,:httpoison]]
  end

  # Dependencies can be Hex packages:
  #
  #   {:mydep, "~> 0.3.0"}
  #
  # Or git/path repositories:
  #
  #   {:mydep, git: "https://github.com/elixir-lang/mydep.git", tag: "0.1.0"}
  #
  # Type `mix help deps` for more examples and options
  defp deps do
    [ 
    {:benchmark,github: "mzruya/elixir-benchmark", only: :test},
    #{:mdigraph,git: "https://github.com/jschoch/erlang-mdigraph"},
    {:uuid, "~> 1.0"},
    {:httpoison, "~> 0.7"},
    {:ex_aws,github: "CargoSense/ex_aws"},
    {:poison, "~> 1.4.0"}
    ]
  end
end
