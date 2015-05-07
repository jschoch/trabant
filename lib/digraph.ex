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
  def v_id(graph,id) do
    graph |> v(%{id_index: id}) |> out
  end
  def v(graph,term) do
    {vertex,label} = :digraph.vertex(graph.g,term)
    Map.put(graph,:stream,[vertex])
  end
  def add_edge(graph,{a,b,label}) do
    add_edge(graph,a,b,label)
  end
  def add_edge(graph,a,b,label) do
    r = :digraph.add_edge(graph.g,a,b,label)
    :ok
  end
  def create_v(graph,term,label \\[]) do
    case Map.has_key?(term,:id) do
      true -> 
        index = %{id_index: term.id}
        :digraph.add_vertex(graph.g,index)
        :digraph.add_vertex(graph.g,term)
        add_edge(graph,index,term,:index)
      false ->
        r = :digraph.add_vertex(graph.g,term)
    end
  end
  def mmatch(target,test) do
    #%{foo: :foo,bar: :bar} |> Map.to_list |> Enum.reduce(true,fn(p,acc) -> acc = acc && p in %{foo: :foo} end)
    #Enum.all?(map, &Enum.member?(%{foo: :foo, bar: :bar), &1))
    test |> Map.to_list |> Enum.reduce(true,fn(p,acc) -> acc = acc && p in target end)
  end
  def e(graph_pointer,pointer) do
    {pointer,a,b,label} = :digraph.edge(graph_pointer,pointer)
    %Trabant.E{pointer: pointer,a: a, b: b, label: label}
  end
  def out(graph) do
    stream = Stream.map(graph.stream,fn(vertex) ->
      :digraph.out_neighbours(graph.g,vertex)
    end)
    Map.put(graph,:stream,stream)
  end
  def outE(%Trabant.G{} = graph) do
    stream = Stream.flat_map(graph.stream,fn(vertex) ->
      Logger.debug "vertex: #{inspect vertex}"
      :digraph.out_edges(graph.g,vertex)
    end)
    Map.put(graph,:stream,stream)
  end
  @doc "get edges with matching key"
  def outE(graph,label) when is_atom(label) do
    IO.puts "unf"
    stream = Stream.flat_map(graph.stream,fn(vertex) ->
      Logger.debug "vertex: #{inspect vertex}"
      #outE(graph,vertex)
      Enum.filter(:digraph.out_edges(graph.g,vertex),fn(pointer) ->
        #TODO: consider that the edge label is not a map
        edge = e(graph.g,pointer)
        Logger.debug "outE/2 \n\tlabel #{inspect label} \n\tpointer #{inspect pointer} \n\tedge: #{inspect edge}"
        Map.has_key?(edge.label,label)
      end)
    end)
    Map.put(graph,:stream,stream)
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
  #@doc "get out edges from single vertex"
  #def outE(graph,v) do
    #edges = :digraph.out_edges(graph.g,v)
    #stream = Stream.map(edges,fn(edge) ->
      #{pointer, a,b,label} = :digraph.edge(graph.g,edge)
      #%Trabant.E{pointer: pointer, a: a,b: b,label: label}
    #end)
    #Map.put(graph,:stream,stream)
  #end
  #@doc "get out edges with key"
  #@spec outE(Trabant.graph,any,Trabant.key) :: Trabant.graph
  #def outE(graph,key) do
    #edges = :digraph.out_edges(graph.g,v)
    #stream = Stream.filter(edges,fn(edge) ->
      #{e,a,b,label} = :digraph.edge(graph.g,edge)
      #Map.has_key?(label,key)
    #end)
    #Map.put(graph,:stream,stream)
  #end
  def inV(graph,key) do
    stream = Stream.map(graph.stream,fn(edge) ->
      IO.inspect edge
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
