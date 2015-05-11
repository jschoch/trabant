defmodule Trabant.G do
  defstruct g: nil,md: %{},sub: %{nodes: [], edges: []},limit: 1,trace: false,stream: []
end
defmodule Trabant.R do
  defstruct count: 0, data: [],graph: nil
end
defmodule Trabant.E do
  defstruct pointer: nil, a: nil, b: nil, label: nil
end
defmodule Trabant.V do
  defstruct id: nil
end
