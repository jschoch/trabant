defmodule Mdigraph do
  @behaviour Trabant.B
  @t_name "graph-#{Mix.env}" |> String.to_char_list
  require Logger
  def new() do
    new("")
  end
  def new(s) do
    #g = :mdigraph.new
    case Mix.env do
      :prod -> IO.puts "skipping destroy in :prod env"
      _ -> IO.puts "destroying schema and db for env #{Mix.env}"
        :mnesia.stop
        :mnesia.delete_schema([node])
        :mnesia.create_schema([node])
        :mnesia.start
    end
    g = :mdigraph.new(@t_name,[:cyclic])
    %Trabant.G{g: g}
  end
  def get_graph() do
    {:mdigraph, :"vertices-#{@t_name}", :"edges-#{@t_name}", :"neighbours-#{@t_name}", true}
  end
  def delete_graph() do
    g = get_graph
    Logger.warn "deleting graph #{inspect g}"
    :mdigraph.delete(g)
    :ok
  end
  def v_id(graph,id) do
    graph |> v(%{id_index: id}) |> out
  end
  def v(graph,term) do
    #Logger.debug "v(graph.g,term)\n\tgraph.g #{inspect graph.g}\n\tterm: #{inspect term}"
    case :mdigraph.vertex(graph.g,term) do
      false -> v = [] 
      {vertex,label} -> v = [vertex] #= :mdigraph.vertex(graph.g,term)
    end
    Map.put(graph,:stream,v)
  end
  def all_v(graph) do
    all = :mdigraph.vertices(graph.g) |> Enum.filter(&(!Map.has_key?(&1,:id_index)))
    Map.put(graph,:stream,all)
  end
  def add_edge(graph,{a,b,label}) do
    add_edge(graph,a,b,label)
  end
  def add_edge(graph,a,b,label) do
    r = :mdigraph.add_edge(graph.g,a,b,label)
    :ok
  end
  def create_v(graph,term,label \\[]) do
    case Map.has_key?(term,:index_id) do
      true -> raise "#{__MODULE__} can't use :index_id key in a vertex"
      false -> nil
    end
    case Map.has_key?(term,:id) do
      true -> 
        index = %{id_index: term.id}
        :mdigraph.add_vertex(graph.g,index)
        :mdigraph.add_vertex(graph.g,term)
        add_edge(graph,index,term,:index)
      false ->
        r = :mdigraph.add_vertex(graph.g,term)
    end
  end
  def e(graph_pointer,pointer) do
    {pointer,a,b,label} = :mdigraph.edge(graph_pointer,pointer)
    %Trabant.E{pointer: pointer,a: a, b: b, label: label}
  end
  @doc "get out neighbours"
  def out(graph) do
    stream = Stream.flat_map(graph.stream,fn(vertex) ->
      :mdigraph.out_neighbours(graph.g,vertex)
    end)
    Map.put(graph,:stream,stream)
  end
  def out(graph,key) when is_atom(key) do
    raise "not done yet"
  end
  @doc "get out edges, expects a list of vertexes from graph.stream"
  def outE(%Trabant.G{} = graph) do
    stream = Stream.flat_map(graph.stream,fn(vertex) ->
      #Logger.debug "vertex: #{inspect vertex}"
      :mdigraph.out_edges(graph.g,vertex)
    end)
    Map.put(graph,:stream,stream)
  end
  @doc "get edges with matching key"
  def outE(graph,label) when is_atom(label) do
    stream = Stream.flat_map(graph.stream,fn(vertex) ->
      #Logger.debug "vertex: #{inspect vertex}"
      Enum.filter(:mdigraph.out_edges(graph.g,vertex),fn(pointer) ->
        #TODO: consider that the edge label is not a map
        edge = e(graph.g,pointer)
        #Logger.debug "outE/2 \n\tlabel #{inspect label} \n\tpointer #{inspect pointer} \n\tedge: #{inspect edge}"
        Map.has_key?(edge.label,label)
      end)
    end)
    Map.put(graph,:stream,stream)
  end
  @doc "get edges with matching k/v pairs in map arg"
  def outE(%Trabant.G{} = graph, map) when is_map(map) do
    #Logger.debug "snu"
    stream = Stream.flat_map(graph.stream,fn(vertex) ->
      edges = :mdigraph.out_edges(graph.g,vertex)
      #Logger.debug "outE/2map edges #{inspect edges}"
      stream = Stream.filter(edges,fn(edge_pointer) ->
        edge = e(graph.g,edge_pointer) 
        case mmatch(edge.label,map) do
          true -> 
            #Logger.debug "match: #{inspect edge_pointer}"
            # TODO: consider option to return %Trabant.E vs edge pointer
            #%Trabant.E{pointer: pointer, a: a, b: b, label: label}
            edge_pointer
          false -> nil
        end
      end)
      #Stream.filter(stream,&(&1 != nil))
    end)
    Map.put(graph,:stream,stream)
  end
  @doc "get in neighbours" 
  def inn(graph) do
    stream = Stream.flat_map(graph.stream,fn(vertex) ->
      :mdigraph.in_neighbours(graph.g,vertex) |> List.delete(%{id_index: vertex.id})
    end)
    Map.put(graph,:stream,stream)
  end
  def inn(graph,match) do
    stream = Stream.flat_map(graph.stream,fn(vertex) ->
      verts = :mdigraph.in_neighbours(graph.g,vertex) |> List.delete(%{id_index: vertex.id})
      Enum.filter(verts,&(mmatch(&1,match)))
    end)
    Map.put(graph,:stream,stream)
  end
  @doc "get all inbound vertices from edge, expects a list of edges in the stream"
  def inV(graph) do
    stream = Stream.map(graph.stream,fn(edge_pointer) ->
      edge = e(graph.g,edge_pointer)
      edge.b
    end)
    Map.put(graph,:stream,stream)
  end
  @doc "get vertices with matching key, expects list of edges from graph.stream"
  def inV(graph,key) when is_atom(key) do
    stream = Stream.map(graph.stream,fn(edge) ->
      {e,a,b,label} = :mdigraph.edge(graph.g,edge)
      #Logger.debug "b: #{inspect b}"
      if (Map.has_key?(b,key)) do
        b
      else
        nil
      end
    end)
    stream = Stream.filter(stream,&(&1 != nil))
    Map.put(graph,:stream,stream)
  end
  defdelegate data(graph), to: Trabant
  defdelegate first(graph), to: Trabant
  defdelegate limit(graph), to: Trabant
  defdelegate limit(graph,limit), to: Trabant
  defdelegate res(graph), to: Trabant
  defdelegate mmatch(target,test), to: Trabant
  defdelegate create_child(graph,opts), to: Trabant

  @doc "sort by id by default"
  def sort(graph) do
    #stream = Stream.flat_map(graph.stream,&(Enum.sort(I#))
    Logger.warn "sort will enumerate the whole stream"
    sorted = Enum.to_list(graph.stream) |> Enum.sort(&(&1.id < &2.id))
    Map.put(graph,:stream,sorted) 
  end
end
