defmodule Digraph do
  @behaviour Trabant.B
  require Logger
  def res(%Trabant.G{} = graph) do
    Enum.reduce(graph.stream,%Trabant.R{graph: graph},fn(i,acc) -> 
      acc = Map.put(acc,:count,acc.count + 1)
      Map.put(acc,:data,[i|acc.data])
    end)
  end
  def new(s) do
    g = :digraph.new
    %Trabant.G{g: g}
  end
  def v(graph,opts) do
    {vertex,label} = :digraph.vertex(graph.g,opts)
    Map.put(graph,:v,vertex)
  end
  def add_edge(graph,a,b,label) do
    r = :digraph.add_edge(graph.g,a,b,label)
    :ok
  end
  def create_v(graph,term,label \\[]) do
    r = :digraph.add_vertex(graph.g,term)
  end
  def outE(graph,label) when is_atom(label) do
    IO.puts "unf"
    stream = Stream.map(graph.stream,fn(vertex) ->
      Logger.debug "vertex: #{inspect vertex}"
      outE(graph,vertex)
    end)
    Map.put(graph,:stream,stream)
  end
  def mmatch(target,test) do
    #%{foo: :foo,bar: :bar} |> Map.to_list |> Enum.reduce(true,fn(p,acc) -> acc = acc && p in %{foo: :foo} end)
    #Enum.all?(map, &Enum.member?(%{foo: :foo, bar: :bar), &1))
    test |> Map.to_list |> Enum.reduce(true,fn(p,acc) -> acc = acc && p in target end)
  end
  def outE(%Trabant.G{} = graph,v, map) when is_map(map) do
    edges = :digraph.out_edges(graph.g,v)
    stream = Stream.map(edges,fn(edge) ->
      e = :digraph.edge(graph.g,edge)
      {pointer,a,b,label} = e
      case mmatch(label,map) do
        true -> %Trabant.E{pointer: pointer, a: a, b: b, label: label}
        false -> nil
      end
    end)
    stream = Stream.filter(stream,&(&1 != nil))
    Map.put(graph,:stream,stream)
  end
  @doc "get out edges from single vertex"
  def outE(graph,v) do
    edges = :digraph.out_edges(graph.g,v)
    stream = Stream.map(edges,&(:digraph.edge(graph.g,&1)))
    Map.put(graph,:stream,stream)
  end
  @doc "get out edges with key"
  @spec outE(Trabant.graph,any,Trabant.key) :: Trabant.graph
  def outE(graph,v,key) do
    edges = :digraph.out_edges(graph.g,v)
    stream = Stream.filter(edges,fn(edge) ->
      {e,a,b,label} = :digraph.edge(graph.g,edge)
      Map.has_key?(label,key)
    end)
    Map.put(graph,:stream,stream)
  end
  def inV(graph,key) do
    stream = Stream.map(graph.stream,fn(edge) ->
      {e,a,b,label} = :digraph.edge(graph.g,edge)
      Logger.debug "b: #{inspect b}"
      if (Map.has_key?(b,key)) do
        b
      else
        nil
      end
    end)
    stream = Stream.filter(stream,&(&1 != nil))
    Map.put(graph,:stream,stream)
  end
end
