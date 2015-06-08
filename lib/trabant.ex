defmodule Trabant do
  @type graph :: %{g: {Atom,any,any,any,boolean},md: %{},sub: %{nodes: list, edges: list},stream: list}
  @type key :: String.t | atom
  @silly 1
  require Logger
  use Application
  def start(_type,_args) do
    backend = backend()
    Logger.info "starting backend: #{backend}"
    case backend do
      Mdigraph ->
        :mnesia.start
        Trabant.Super.start_link(Mdigraph)
      _ ->
        Trabant.Super.start_link(backend)
    end
  end
  def stop do
    case backend do
      Mdigraph ->
        :mnesia.stop
      _ -> nil
    end
  end
  def backend() do
    Application.get_env(__MODULE__,:backend)
  end
  def backend(new) do
    Application.stop(:trabant)
    Application.put_env(__MODULE__,:backend,new)
    #Trabant.Super.start_link(new)
    start(nil,nil)
  end
  def silly do
    @silly
  end
  #
  # calls to backends
  #
  def new do
    Trabant.backend.new
  end
  def new(name) do
    Trabant.backend.new(name)
  end
  def get_graph() do
    Trabant.backend.get_graph()
  end
  def graph() do
    Trabant.backend.graph()
  end
  def graph(graph) do
    Trabant.backend.graph(graph)
  end
  def add_edge(graph,{a,b,label,map}) do
    Trabant.backend.add_edge(graph,a,b,label,map)
  end
  def add_edge(graph,a,b,label,term \\%{}) do
    Trabant.backend.add_edge(graph,a,b,label,term)
  end
  def v(map) when is_map(map) do
    Trabant.backend.v(map)
  end
  def v_id(id) when is_number(id) or is_binary(id) do
    Trabant.backend.v_id(id)
  end
  def v(graph,map) do
    Trabant.backend.v(graph,map)
  end
  def v_id(graph,id) do
    Trabant.backend.v_id(graph,id)
  end
  def all_v(graph) do
    Trabant.backend.all_v(graph)
  end
  def e(graph,pointer) do
    Trabant.backend.e(graph,pointer)
  end
  def outE(graph) do
    Trabant.backend.outE(graph)
  end
  def outE(graph,atom) do
    Trabant.backend.outE(graph,atom)
  end
  def outE(graph,map) do
    Trabant.backend.outE(graph,map)
  end
  def inn(graph) do
    Trabant.backend.inn(graph)
  end
  def inn(graph,vertex)do
    Trabant.backend.inn(graph,vertex)
  end
  def inn(graph,vertex,where)do
    Trabant.backend.inn(graph,vertex,where)
  end
  def inV(graph) do
    Trabant.backend.inV(graph)
  end
  def inV(graph,atom) do
    Trabant.backend.inV(graph,atom)
  end
  def inV(graph,map) do
    Trabant.backend.inV(graph,map)
  end
  def out(graph) do
    Trabant.backend.out(graph)
  end
  def out(graph,vertex)do
    Trabant.backend.out(graph,vertex)
  end
  def create_v(graph,vertex, label \\[]) do
    Trabant.backend.create_v(graph,vertex, label)
  end
  def update_v(graph,vertex) do
    Trabant.backend.update_v(graph,vertex)
  end
  def delete_graph() do
    Trabant.backend.delete_graph()
  end
  def del_v(graph,id) when is_number(id) or is_binary(id) do
    Trabant.backend.del_v(graph,id)
  end
  def del_v(graph,map) when is_map(map) do
    Trabant.backend.del_v(graph,map)
  end
  def del_e(graph,pointer) do
    Trabant.backend.del_e(graph,pointer)
  end

  #
  # delegates
  #

  @doc "looks for all key/val pairs in test in target, returns true if they all match"
  def mmatch(target,test) when is_map(target) do
    list = Map.to_list(target)
    mmatch(list,test)
  end
  def mmatch(target,test) do
    #%{foo: :foo,bar: :bar} |> Map.to_list |> Enum.reduce(true,fn(p,acc) -> acc = acc && p in %{foo: :foo} end)
    #Enum.all?(map, &Enum.member?(%{foo: :foo, bar: :bar), &1))
    Map.to_list(test)
      |> Enum.reduce(true,fn(p,acc) -> 
        Logger.debug inspect [acc,p,target]
        acc = acc && p in target 
      end)

  end
  @doc "process stream result into enum, collect md and counts etc"
  def res(%Trabant.G{} = graph) do
    Enum.reduce(graph.stream,%Trabant.R{graph: graph},fn(i,acc) ->
      acc = Map.put(acc,:count,acc.count + 1)
      case match?(%Trabant.G{},i) do
        true ->
          raise "found another stream, recurse!"
        false ->
          Map.put(acc,:data,[i|acc.data])
      end
    end)
  end
  def res(term) do
    raise "expected a %Trabant.G{} got: \n\t#{inspect term,pretty: true}"
  end
  @doc "get res().data"
  def data(graph) do
    #Logger.debug inspect res(graph)
    r = res(graph)
    r.data
  end
  @doc "TODO: do we really need this?"
  def first(%Trabant.G{stream: []} = graph) do
    graph
  end
  def first(graph) do
    stream = Stream.take(graph.stream,1)
    Map.put(graph,:stream,stream)
  end
  @doc "sort by id by default"
  def sort(graph) do
    #stream = Stream.flat_map(graph.stream,&(Enum.sort(I#))
    Logger.warn "sort will enumerate the whole stream"
    sorted = Enum.to_list(graph.stream) |> Enum.sort(&(&1.id < &2.id))
    Map.put(graph,:stream,sorted)
  end
  @doc "limits the total results, uses limit attribute of the graph"
  def limit(graph) do
    stream = Stream.take(graph.stream,graph.limit)
    Map.put(graph,:stream,stream)
  end
  @doc "limits this part of the chain by the limit arg"
  def limit(graph,limit) do
    stream = Stream.take(graph.stream,limit)
    Map.put(graph,:stream,stream)
  end
  @doc "creates a vertex and links via edges %{id: <parent id>,child: <map should be enforced>,label: <edge label>"
  def create_child(graph, opts) when is_map(opts) do
    #[source] = v_id(graph,opts.id) |> data
    create_v(graph,opts.child)
    add_edge(graph,opts.id,opts.child.id,opts.label)
  end


  #  ID related functions

  # lookup table for id types, first binary byte of id
  @id_types  %{
    default: "a",
    node: "a",
    out_edge: "b",
    in_edge: "c",
    edge_label: "d",
    node_label: "e",
    out_neighbor: "f",
    in_neighbor: "g"
  }

  # reverse index on @id_types

  @id_type Enum.into(@id_types,%{},fn({k,v}) -> {v,k} end) 

  # deprecated i think
  @post_types %{
    default: "a"

  }
  def create_binary_id(type \\:default) when is_atom(type) do
    id = UUID.uuid4 |> UUID.info!
    Logger.debug "id: #{inspect id[:binary]}"
    @id_types[type] <> id[:binary] 
  end
  def create_string_id(type \\:default) when is_atom(type) do
    id = UUID.uuid4(:hex)
    @id_types[type] <> id
  end
  def id_type?(<< s :: binary-size(33) >> ) when is_binary(s) do
    << id_type :: binary-size(1), _ :: binary >> = s
    Map.fetch!(@id_type,id_type)
  end
  def id_type?(x) do
    raise "TODO: type for #{inspect x} not defined yet"
  end
  def parse_id(<< s :: binary-size(17) >> ) when is_binary(s) do
    << id_type :: binary-size(1), id :: binary-size(16) >> = s
    %{id_type: id_type, bid: id,sid: UUID.binary_to_string!(id,:hex)}
  end
  def parse_id(<< s :: binary-size(33) >> ) when is_binary(s) do
    << id_type :: binary-size(1), id :: binary-size(32) >> = s
    %{id_type: id_type, bid: UUID.info!(id)[:binary],sid: id}

  end
  def parse_id(x) do
    raise "bad id " <> inspect x
  end
  def cast_id(<< id_type :: binary-size(1), id :: binary-size(32) >> ,t) when is_atom(t) do
    @id_types[t] <> id
  end
  def cast_id(s,a) do
    raise "cast_id: bad id: #{inspect [s,a]}"
  end
end
