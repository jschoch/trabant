defmodule Trabant do
  @type graph :: %{g: {atom,any,any,any,boolean},md: %{},sub: %{nodes: list, edges: list},stream: list}
  @type key :: String.t | atom
end
defmodule Trabant.G do
  defstruct g: nil,md: %{},sub: %{nodes: [], edges: []},limit: 0,trace: false,stream: []
end
defmodule Trabant.R do
  defstruct count: 0, data: [],graph: nil
end
defmodule Trabant.E do
  defstruct pointer: nil, a: nil, b: nil, label: nil
end

defmodule Trabant.B do
  use Behaviour
  defcallback res(Trabant.graph) :: list
  defcallback new(String.t) :: Trabant.graph
  defcallback add_edge(Trabant.graph,any,any,any) :: :ok
  defcallback v(Trabant.graph,any) :: any
  defcallback e(Trabant.graph,any) :: %{pointer: list,a: any,b: any,label: any}
  defcallback outE(Trabant.graph,any) :: Trabant.graph
  defcallback outE(Trabant.graph,any,String.t|atom) :: Trabant.graph
  defcallback outE(Trabant.graph,any,Map) :: Trabant.graph
  defcallback inV(Trabant.graph,Trabant.key) :: Trabant.graph
  defcallback create_v(Trabant.graph,any,any) :: :ok
  defcallback out(Trabant.graph,any,Trabant.key) :: Trabant.graph
end
