defmodule Trabant do
  @type graph :: %{g: {Atom,any,any,any,boolean},md: %{},sub: %{nodes: list, edges: list},stream: list}
  @type key :: String.t | atom
end
defmodule Trabant.G do
  defstruct g: nil,md: %{},sub: %{nodes: [], edges: []},limit: 1,trace: false,stream: []
end
defmodule Trabant.R do
  defstruct count: 0, data: [],graph: nil
end
defmodule Trabant.E do
  defstruct pointer: nil, a: nil, b: nil, label: nil
end

defmodule Trabant.B do
  use Behaviour
  defcallback res(Trabant.graph) :: List
  defcallback new(String.t) :: Trabant.graph
  defcallback add_edge(Trabant.graph,any,any,any) :: :ok
  @doc "get a single vertex"
  defcallback v(Trabant.graph,any) :: any
  @doc "get all vertices"
  defcallback all_v(Trabant.graph) :: Trabant.graph
  @doc "get a single edge"
  defcallback e(Trabant.graph,any) :: %{pointer: List,a: any,b: any,label: any}
  @doc "get all out edges for list of vertices from graph.stream"
  defcallback outE(Trabant.graph) :: Trabant.graph
  @doc "get out edges with matching key"
  defcallback outE(Trabant.graph,String.t|atom) :: Trabant.graph
  @doc "get out edges with matching key/value pairs from map arg"
  defcallback outE(Trabant.graph,Map) :: Trabant.graph
  @doc "get in neighbours"
  defcallback inn(Traban.graph) :: Trabant.graph
  @doc "get in with map match"
  defcallback inn(Trabant.graph,map) :: Trabant.graph
  @doc "get in neighbours with where"
  defcallback inn(Trabant.graph,map) :: Trabant.graph
  @doc "get b vertices from edge" 
  defcallback inV(Trabant.graph,Trabant.key) :: Trabant.graph
  @doc "get b vertices from match map"
  defcallback inV(Trabant.graph,map) :: Trabant.graph
  defcallback create_v(Trabant.graph,any,any) :: :ok
  @doc "get all neighbors by out going edges"
  defcallback out(Trabant.graph) :: Trabant.graph
  @doc "get out neighbors with matching key/value pairs from map arg, expects list of vertices from graph.stream"
  defcallback out(Trabant.graph,Trabant.key) :: Trabant.graph
  @doc "sort a list"
  defcallback sort(Trabant.graph) :: Trabant.sort
  @doc "convenience for take(1)"
  defcallback first(Trabant.graph) :: any
  @doc "convenience for res.data"
  defcallback data(Trabant.graph) :: List
  @doc "convenience for limit" 
  defcallback limit(Trabant.graph) :: Trabant.graph
  @doc "convenience for limit, overrides graph.limit"
  defcallback limit(Trabant.graph,integer) :: Trabant.graph
end
