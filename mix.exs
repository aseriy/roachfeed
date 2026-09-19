defmodule RoachFeed.MixProject do
  use Mix.Project

  @source_url "https://github.com/karlseguin/roachfeed"
  @version "0.0.7"

  def project do
    [
      app: :roachfeed,
      name: "RoachFeed",
      deps: deps(),
      elixir: "~> 1.10",
      version: @version,
      elixirc_paths: paths(Mix.env()),
      description: "CockroachDB ChangeFeed Consumer",
      package: package(),
      docs: docs()
    ]
  end

  defp paths(:test), do: paths(:prod) ++ ["test/support"]
  defp paths(_), do: ["lib"]

  def application do
    [
      extra_applications: [:logger, :ssl]
    ]
  end

  defp deps do
    [
      {:jason, "~>1.2.2", only: :test},
      {:postgrex, "~> 0.21.0", only: :test},
      {:ex_doc, "~> 0.22.6", only: :dev, runtime: false}
    ]
  end

  defp package do
    [
      maintainers: ["Karl Seguin"],
      licenses: ["ISC"],
      links: %{"GitHub" => @source_url}
    ]
  end

  defp docs do
    [
      main: "readme",
      source_url: @source_url,
      extras: [
        "README.md",
        "LICENSE"
      ]
    ]
  end
end
