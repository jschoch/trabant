defmodule Mdigraph do
  @behaviour Trabant.B
  @t_name "Graph-#{Mix.env}" |> String.to_char_list
  @g     {:mdigraph, :"vertices-#{@t_name}", :"edges-#{@t_name}", :"neighbours-#{@t_name}",true}
  require Logger
  def init do
    raise "need to check for an existing schema and warn or get user input"
    :mnesia.stop
    #:mnesia.delete_schema([node])
    #:mnesia.create_schema([node])
    :mnesia.start
  end
  def new() do
    new("")
  end
  def new(s) do
    #g = :mdigraph.new
    case Mix.env do
      :prod -> IO.puts "skipping destroy in :prod env"
      _ -> IO.puts "destroying schema and db for env #{Mix.env}"
    end
    g = :mdigraph.new(@t_name,[:cyclic])
    %Trabant.G{g: g}
  end
  def delete_graph() do
    g = get_graph
    Logger.warn "deleting graph #{inspect g}"
    :mdigraph.delete(g)
    :ok
  end
  def get_id_node(graph,%{id: id} = vertex) when is_map(vertex) do
    get_id_node(graph,id)
  end
  def get_id_node(graph,%{id_index: id}) do
    raise "cant' get id_node of self id: #{inspect id}"
  end
  def get_id_node(graph,id) when is_binary(id) or is_number(id) do
    case :mdigraph.vertex(graph.g,%{id_index: id}) do
      {id_node,label} -> id_node
      horror -> raise "no id node: #{inspect id} found, something very bad happened\n\thorror: #{inspect horror}"
    end
  end
  def v_id(id) do
    v_id(graph(),id)
  end
  def v_id(graph,id) do
    #[id_node] = graph |> v(%{id_index: id}) |> data
    #data_node = :mdigraph.in_neighbours(graph.g,id_node)
    case graph |> v(%{id_index: id}) |> data do
      [id_node] -> data_node = :mdigraph.in_neighbours(graph.g,id_node)
      [] -> data_node = []
      doh -> raise "horror #{inspect doh}"
    end
    Map.put(graph,:stream,data_node)
  end
  def v(term) do
    v(graph(),term)
  end
  def v(graph,term) do
    case :mdigraph.vertex(graph.g,term) do
      false -> v = [] 
      {vertex,label} -> 
        v = [vertex] 
    end
    Map.put(graph,:stream,v)
  end
  def all_v(graph) do
    all = :mdigraph.vertices(graph.g) 
      #filter out "terminal nodes"
      |> Stream.filter(&(!is_number(&1)))
      |> Stream.filter(&(!is_binary(&1)))
      # filter out "id_nodes"
      |> Stream.filter(&(!Map.has_key?(&1,:id_index)))
    Map.put(graph,:stream,all)
  end
  def add_edge(graph,{a,b,label}) do
    add_edge(graph,a,b,label)
  end
  def add_edge(graph,a,b,label) do
    r = :mdigraph.add_edge(graph.g,a.id,b.id,label)
    :ok
  end
  @doc """
  TODO: do we really need the label?
  create_v(graph,term)
  creates data -> id_node -> terminal

  id_node is always %{id_index: id}
  terminal_node is always term.id

  terminal maintains all node connections
  id maintains link to data
  requires additional nodes so we can mutate the data and not have to re-create all the associated edges for :mdigraph, and :mdigraph
  """
  def create_v(graph,term,label \\[]) do
    case Map.has_key?(term,:id_index) do
      true -> raise "#{__MODULE__} can't use :index_id key in a vertex"
      false -> nil
    end
    case Map.has_key?(term,:id) do
      true -> 
        index = %{id_index: term.id}
        :mdigraph.add_vertex(graph.g,index)
        :mdigraph.add_vertex(graph.g,term)
        :mdigraph.add_vertex(graph.g,term.id)
        r1 = :mdigraph.add_edge(graph.g,term,index,:index)
        r2 = :mdigraph.add_edge(graph.g,index,term.id,:terminal)
        Logger.debug("edge add results \n\t#{inspect r1}\n\t#{inspect r2}")
      false ->
        #r = :mdigraph.add_vertex(graph.g,term)
        raise "create_v requires key/value for :id"
    end
  end
  def update_v(graph,%{id: id} = vertex) do
    id_node = get_id_node(graph,id)
    edge_pointer = :mdigraph.in_edges(graph.g,id_node)|> List.first 
    {_,old_node,_,_} = :mdigraph.edge(graph.g,edge_pointer)
    :mdigraph.add_vertex(graph.g,vertex)
    :mdigraph.add_edge(graph.g,vertex,id_node,:index)
    :mdigraph.del_vertex(graph.g,old_node)
    vertex
    #:ok
  end
  def e(graph_pointer,pointer) do
    {pointer,a,b,label} = :mdigraph.edge(graph_pointer,pointer)
    %Trabant.E{pointer: pointer,a: a, b: b, label: label}
  end
  @doc "get out neighbours"
  def out(graph) do
    stream = Stream.flat_map(graph.stream,fn(vertex) ->
      out_terminal_nodes = :mdigraph.out_neighbours(graph.g,vertex.id)
      Enum.flat_map(out_terminal_nodes,fn(out_node) ->
        :mdigraph.in_neighbours(graph.g,%{id_index: out_node})
      end)
    end)
    Map.put(graph,:stream,stream)
  end
  def out(graph,key) when is_atom(key) do
    raise "not done yet"
  end
  @doc "get out edges pointers, expects a list of vertexes from graph.stream"
  def outE(%Trabant.G{} = graph) do
    stream = Stream.flat_map(graph.stream,fn(vertex) ->
      #Logger.debug "vertex: #{inspect vertex}"
      terminal_pointers = :mdigraph.out_edges(graph.g,vertex.id)
    end)
    Map.put(graph,:stream,stream)
  end
  @doc "get edges with matching key"
  def outE(graph,label) when is_atom(label) do
    stream = Stream.flat_map(graph.stream,fn(vertex) ->
      #Logger.debug "vertex: #{inspect vertex}"
      #id_node = get_id_node(graph,vertex)
      Enum.filter(:mdigraph.out_edges(graph.g,vertex.id),fn(pointer) ->
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
      edges = :mdigraph.out_edges(graph.g,vertex.id)
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
      in_terminals = :mdigraph.in_neighbours(graph.g,vertex.id) 
      Enum.map(in_terminals,fn(id) ->
        #Logger.debug "inn: id was: #{inspect id}"
        case :mdigraph.in_neighbours(graph.g,%{id_index: id}) do
          [data] -> 
            #Logger.debug "Data from inn: #{inspect data}"
            data
          [] -> nil
        end
      end)
    end)
    stream = Stream.filter(stream,&(&1 != nil))
    Map.put(graph,:stream,stream)
  end
  def inn(graph,match) do
    graph = inn(graph)
    stream = Stream.filter(graph.stream,&(mmatch(&1,match)))
    Map.put(graph,:stream,stream)
  end
  @doc "get all inbound vertices from edge, expects a list of edges in the stream"
  def inV(graph) do
    stream = Stream.flat_map(graph.stream,fn(edge_pointer) ->
      edge = e(graph.g,edge_pointer)
      #Logger.debug "inV edge: #{inspect edge}"
      v_id(graph,edge.b) |> data
    end)
    Map.put(graph,:stream,stream)
  end
  @doc "get edges with matching key, expects list of edges from graph.stream"
  def inV(graph,key) when is_atom(key) do
    stream = Stream.map(graph.stream,fn(edge) ->
      {e,terminal_a,terminal_b,label} = :mdigraph.edge(graph.g,edge)
      [b] = v_id(graph,terminal_b) |> data
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
  def get_graph() do
    {:mdigraph, :"vertices-#{@t_name}", :"edges-#{@t_name}", :"neighbours-#{@t_name}", true}
  end
  def graph do
    #%Trabant.G{g: get_graph}
    %Trabant.G{g: @g}
  end
  def graph(graph) do
    Map.put(graph,:g,get_graph)
  end
  def del_v(graph,id) when is_number(id) or is_binary(id) do
    [vertex] = v_id(graph,id) |> data
    del_v(graph,vertex)
  end
  def del_v(graph,map) when is_map(map) do
    a = :mdigraph.del_vertex(graph.g,map)
    b = :mdigraph.del_vertex(graph.g,map.id)
    c = :mdigraph.del_vertex(graph.g,%{id_index: map.id})
    {a,b,c}
  end
  def del_e(graph,edge_pointer) do
    :mdigraph.del_edge(graph.g,edge_pointer)
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
