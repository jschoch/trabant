defmodule Trabant.Mixfile do
  use Mix.Project

  def project do
    [app: :trabant,
     version: "0.0.1",
     elixir: "~> 1.0",
     build_embedded: Mix.env == :prod,
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
    #{:zdb, github: "jschoch/zdb",branch: "v0.1.0"},
    {:benchmark,github: "mzruya/elixir-benchmark"},
    {:mdigraph,git: "https://github.com/jschoch/erlang-mdigraph"},
    #{:ex_aws,"~> 0.0.5"},
    {:uuid, "~> 1.0"},
    {:httpoison, "~> 0.6.2"},
    {:ex_aws,github: "CargoSense/ex_aws"},
    {:poison, "~> 1.4.0"}
    ]
  end
end
