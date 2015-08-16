defmodule Ddb.V do
  @derive [ExAws.Dynamo.Encodable]
  #defstruct [:id,:r, :created_at]
  defstruct id: nil, r: "0", created_at: Timex.Time.now(:secs),v_type: nil,t: nil
end
defmodule Ddb.E do
  require Logger
  @derive [ExAws.Dynamo.Encodable]
  defstruct id: nil, r: nil, label: nil,created_at: Timex.Time.now(:secs),target_id: nil,e_type: nil,t: nil

  # convert label values to existing atoms
  #defimpl ExAws.Dynamo.Decodable do
    #def decode(%{label: label} = map) do
      #Logger.error("importing edge: abandon labels as atoms please, use strings")
      #%{map | label: String.to_atom(label)}
    #end
  #end
end
defmodule Ddb.N do
  @derive [ExAws.Dynamo.Encodable]
  #defstruct [:id,:r, :created_at]
  defstruct id: nil, r: nil, created_at: Timex.Time.now(:secs),nbr_type: nil,label: nil,t: nil
end
defmodule Ddb do
  @behaviour Trabant.B
  #t_name() "Graph-#{Mix.env}"
  alias ExAws.Dynamo
  require Logger
  def t_name() do
    "Graph-#{Mix.env}"
  end
  def graph do
    %Trabant.G{g: %{table_name: t_name(), hash_key_name: "id",range_key_name: "r"}}
  end
  def new() do
    tbl = Dynamo.create_table(t_name(),[id: :hash,r: :range],[id: :string,r: :string], 25, 25)
    #tbl = Dynamo.create_table(t_name(),[id: :hash],[id: :string], 1, 1)
    Logger.debug inspect tbl
    %Trabant.G{g: %{table_name: t_name(), hash_key_name: "id",range_key_name: "r"}}
  end
  def new(string) do
    Logger.warn "use t_name(), this will ignore arg string: #{string} for new"
    new
  end
  def delete_graph() do
    Logger.warn "deleting table #{t_name()}"
    Dynamo.delete_table(t_name())
  end
  def all(graph,raw \\true) do
    case raw do
      false ->
        {:ok, stuff} = Dynamo.scan(t_name())
        stuff
      true -> 
        Dynamo.stream_scan(t_name()) 
          |> Enum.map(&Dynamo.Decoder.decode(&1))
      :better ->
        map = Dynamo.stream_scan(t_name())
          |> Enum.map(&Dynamo.Decoder.decode(&1))
          |> Enum.map(fn(i) ->
            [i["id"],i["r"],i["t"],i["label"]]
          end) 
    end
  end
  def test_id(id) when is_binary(id) do
    Logger.debug "testing id: #{id}"
    case byte_size(id) == 33 do
      false -> raise "need 33 byte binary for id, consider using create_string_id\n\tid: #{id}"
      true -> nil
    end
  end
  def test_id(id) do
    raise "can't create id, only supporting 33 byte strings right now" 
  end
  def dump do
    f = "backup-full-#{:os.system_time}"
    dump(f)
  end
  def dump(name) do
    full = all graph, true
    txt = "error"
    dir = "backups/"
    f = dir <> "backup-#{name}-#{:os.system_time}"
    if (!File.dir?( dir)), do: File.mkdir_p! dir
    case Poison.encode(full) do
      {:ok,s} -> txt = s
      doh -> raise "encoding error: "<> inspect doh
    end
    File.write(f,txt)
    {:ok, f}
  end
  def restore do
    raise "TODO: how do we do this ?"
  end
  def restore(name) do
    Logger.info "restoring #{name}"
    {:ok, str} = File.read(name)
    IO.puts inspect str
    case Poison.decode(str) do
      {:ok,map} -> 
        Logger.info "size: " <>inspect Enum.count map
        "map: " <> inspect( Enum.take(map,5), pretty: true)
        Enum.each(map,fn(i) ->
          IO.puts inspect i
          restore_item(i)
        end)
      {err,reason} -> raise "error: #{err} \n\n#{inspect reason}"
    end
  end
  def restore_item(item) do
    case item do
      %{"t" =>  t} when t in ~w(in_edge out_edge) ->
        item = Map.merge(%Ddb.E{},item)
        {:ok, res} = ExAws.Dynamo.put_item(Ddb.t_name(),item)
      %{"t" => "node"} ->
        item = Map.merge(%Ddb.V{},item)
        {:ok, res} = ExAws.Dynamo.put_item(Ddb.t_name(),item)
      %{"t" =>  t} when t in ~w(in_nbr out_nbr) ->
        item = Map.merge(%Ddb.N{},item)
        {:ok, res} = ExAws.Dynamo.put_item(Ddb.t_name(),item)
      doh -> raise "unknown type error #{inspect item}"
    end
  end
  @doc "creates a vertex, atom label is cast to string" 
  def create_v(map,label) when is_map(map) and is_atom(label) do
    create_v(map,Atom.to_string(label))
  end
  @doc "creates a vertex, id is optional, graph is derived from graph()"
  def create_v(map,label) when is_map(map) and is_binary(label) do
    graph = graph
    case Map.has_key?(map,:id) do
      true -> nil
      false -> 
        id = create_string_id(:node)
        map = Map.put(map,:id,id)
    end
    create_v(graph,map,label)
  end
  def create_v(graph,term,label \\:default_label)
  def create_v(graph,%{id: id}, label) when is_number(id) do
    raise "can't use integer id's until we workout how to get the table creation types aligned and correct"
  end
  def create_v(graph,%{id: id, r: r} = term,label) when is_binary(id) do
    test_id(id)
    vertex = %Ddb.V{} |> Map.merge(term)
    vertex = Map.merge(vertex,%{t: "node",v_type: "node"})
    {:ok, res} = Dynamo.put_item(t_name(),vertex, return_values: :all_old)
    if !res === %{}, do: Logger.error "create_v: overwrite, old value: " <> inspect res
    # TODO: this should be optional one day, for now casting strings and atoms is driving me nutz
    {:ok, item} = Dynamo.get_item(t_name(),%{id: vertex.id, r: vertex.r})
    r = decode_vertex(item["Item"])
    if r == nil, do: raise "create_v: disaster! "<> inspect {id,r,term,label,item}
    r
  end
  @doc "another hack for range key"
  def create_v(graph,%{id: id} = term,label) when is_binary(id) do
    test_id(id)
    r = "0"
    vertex = %Ddb.V{r: r} |> Map.merge(term)
    create_v(graph,vertex)
  end

  def add_out_edge(graph,aid,bid,label,term) when is_binary(aid) and is_binary(bid) do
    ie_id = cast_id(bid,:in_edge)
    #ie_r = aid <> Atom.to_string(label)
    ie_r = aid <> label
    oe_map = %{
      id: cast_id(aid,:out_edge),
      map: term,
      label: label,
      target_id: ie_id,
      e_type: :out,
      t: "out_edge",
      #r: bid <> Atom.to_string(label)
      r: bid <> label
    }
    out_edge = Map.merge(%Ddb.E{},oe_map)
    {:ok,%{}} = Dynamo.put_item(t_name(),out_edge)
    out_edge
  end
  def add_in_edge(graph,aid,bid,label) when is_binary(aid) and is_binary(bid) do
    ie_id = cast_id(bid,:in_edge)
    #ie_r = aid <> Atom.to_string(label)
    ie_r = aid <> label
    ie_map = %{
      id: ie_id,
      r: ie_r,
      label: label,
      t: "in_edge",
      e_type: :in
      # TODO: ensure we don't want the term in both edges, only :out_edge
      #map: term
    }
    in_edge = Map.merge(%Ddb.E{},ie_map)
    Logger.debug inspect in_edge
    {:ok,%{}} = Dynamo.put_item(t_name(),in_edge)
    in_edge
  end
  def add_out_nbr(aid,bid,label)  when is_binary(aid) and is_binary(bid) do
    #Logger.error inspect [aid,bid,label]
    out_map = %{
      id: cast_id(aid,:out_neighbor),
      label: "out_nbr",
      nbr_type: :out,
      t: "out_nbr",
      r: bid}
    out_nbr = struct(Ddb.N,out_map)
    {:ok,%{}} = Dynamo.put_item(t_name(),out_nbr)
  end
  def add_in_nbr(aid,bid,label) when (is_binary(aid) and is_binary(bid)) do
    in_map = %{
      id: cast_id(bid,:in_neighbor),
      label: "in_nbr",
      nbr_type: :in,
      t: "in_nbr",
      r: aid}
    in_nbr = struct(Ddb.N,in_map)
    {:ok,%{}} = Dynamo.put_item(t_name(),in_nbr)
  end
  def add_edge(graph,%{} = a, %{} = b,label, %{} = term) when is_atom(label)  do
    add_edge(graph,a.id,b.id,label,term)
  end
  def add_edge(graph,aid,bid,label, %{} = term) when is_atom(label) do
    add_edge(graph,aid,bid,Atom.to_string(label),term) 
  end
  def add_edge(graph,aid,bid,label, %{} = term) when is_binary(label) do
    Logger.debug("add_edge aid: #{aid} bid: #{bid} label: #{label} term: #{inspect term}")
    children = []
    pid = self()
    
    #setup out_edge
    children = [spawn(fn-> add_out_edge(graph,aid,bid,label,term);send(pid,self); end) | children]

    # setup in_edge for b
    children = [spawn(fn-> add_in_edge(graph,aid,bid,label);send(pid,self); end) | children]
    # setup neightbors

    #out
    children = [spawn(fn-> add_out_nbr(aid,bid,label);send(pid,self); end) | children]
        #in
    children = [spawn(fn-> add_in_nbr(aid,bid,label);send(pid,self); end) |children]
    wait_on(children)
    #out_edge
  end
  def add_edge(graph,a,b,label,term) do
    raise "add_edge/5 must use a atom as a label, and map as a term #{inspect [a,b,label,term]}"
  end
  def decode_vertex({:ok, %{}}) do
    raise "empty"
  end
  def decode_vertex(raw) do
    map = Dynamo.Decoder.decode(raw) 
      |> keys_to_atoms
    Map.merge(%Ddb.V{},map)
  end
  def id_from_neighbor(s) do
    Logger.debug inspect s
    cast_id(s,:node)
  end
  def out(graph) do
    stream = Stream.flat_map(graph.stream,fn(vertex) ->
      Logger.debug "getting out neighbors for vertex id: #{vertex.id}"
      graph = out(graph,vertex) 
      #Logger.debug "out(graph): \n\n\t#{inspect Enum.to_list(graph.stream)}"
      graph.stream
    end)
    Map.put(graph,:stream,stream)
  end
  def out(graph,vertex) do
    eav = [id: cast_id(vertex.id,:out_neighbor)]
    kce = "id = :id "
    r = Dynamo.stream_query(t_name(),
      expression_attribute_values: eav,
      key_condition_expression: kce)
    stream = Stream.flat_map(r,fn(raw) -> 
      item = Dynamo.Decoder.decode(raw,as: Ddb.N)
      r = id_from_neighbor(item.r)
      Logger.debug "found neighbor #{r}"
      g = v_id(graph,r)
      g.stream
    end)
    Map.put(graph,:stream,stream)
  end
  @doc "get all neighbors with in edges from a stream of vertexes"
  def inn(graph) do
    stream = Stream.flat_map(graph.stream,fn(vertex) ->
      inn(graph,vertex).stream
    end)
    Map.put(graph,:stream,stream)
  end
  @doc "get all neighbors with in edges from a single vertex"
  def inn(graph,%Ddb.V{} = vertex) do
    id = cast_id(vertex.id,:in_neighbor)
    eav = [id: "#{id}"]
    kce = "id = :id "
    r = Dynamo.stream_query(t_name(),
      expression_attribute_values: eav,
      key_condition_expression: kce)
    stream = Stream.flat_map(r,fn(raw) ->
      item = Dynamo.Decoder.decode(raw,as: Ddb.N)
      g = v_id(graph,item.r)
      g.stream
    end)
    Map.put(graph,:stream,stream)
  end
  @doc "get neighbors with matching attributes with in edges for mmap"
  def inn(graph,mmap) when is_map(mmap) do
    g = inn(graph)
    stream = Stream.filter(g.stream,fn(vertex) ->
      Logger.debug inspect vertex
      mmatch(vertex,mmap)
      #mmatch(mmap,vertex)
    end)
    Map.put(graph,:stream,stream)
  end
  def update_v(graph) do
    stream = Stream.flat_map(graph.stream, fn(vertex) ->
      update_v(graph,vertex).stream
    end)
    Map.put(graph,:stream,stream)
  end
  def update_v(graph,%Ddb.V{id: id} = vertex) do
    Logger.warn "update should update, but we have it putting a new item instead"
    create_v(graph,vertex)
    #Dynamo.update_item(t_name(),id, 
  end
  @doc "deletes a list of vertexes from a stream" 
  def del_v(graph) do
    Stream.each(graph.stream,fn(vertex) ->
      del_v(graph,vertex)
    end)
    #TODO: do we need to put in some metadata here?
    Map.put(graph,:stream,[])
  end
  @doc """
    deletes a vertex


    this has to delete all in and out edges, as well as all in and out neighbors.  
    """
  def del_v(graph,%Ddb.V{id: id,r: r} = v) do
    Logger.debug "del_v called on id: " <> id
    out_edges = outE(graph, v)  
    #TODO: deleting labeld in_edges seems expensive, can we omit labels for edges if neede?
    Enum.each(out_edges.stream,  fn(edge_pointer) ->
      Logger.debug "del_v: ep: #{inspect edge_pointer}"
      edge = e(edge_pointer)
      Logger.debug "del_v: starting children"
      # remove :in_edge
      children = []
      pid = self()
      #remove in edges
      children = [spawn( fn-> del_ie(edge);send(pid,self); end) | children]

      #remove :out_neighbor
      children = [spawn( fn-> del_on(edge);send(pid,self) end) | children]

      #remove :in_neighbor
      children = [spawn( fn-> del_in(edge);send(pid,self) end) | children]

      # delete out_edge fro source vertex
      children = [spawn( fn->  del_e(graph,edge_pointer);send(pid,self) end) | children]
      wait_on(children)
    end)
    # delete vertex
    Logger.debug "del_v processing in_edges for id: "<>id 
    in_edges = inE(graph,v)
    Enum.each(in_edges.stream,fn(edge_pointer) ->
      #
      # in edges require that we delete additional nbrs and edges on the source vertes
      #
      edge = parse_pointer(edge_pointer)
      Logger.debug( "del_v in_edges: #{inspect edge}\n pointer: #{inspect edge_pointer}" )
      del_e(graph,edge_pointer)
      out_nbr_id = cast_id edge.bid, :out_neighbor
      out_nbr_r = cast_id edge.aid,:node
      ptr = {out_nbr_id,out_nbr_r}
      Logger.debug "del_v deleting nbr: "<> inspect ptr
      del_e(graph,ptr)
      target_out_e_id = cast_id edge.bid, :out_edge
      #target_out_e_r = edge.aid <> Atom.to_string(edge.label)
      target_out_e_r = edge.aid <> edge.label
      ptr = {target_out_e_id,target_out_e_r}
      Logger.debug "del_v deleting source out_edge: "<> inspect ptr
      del_e(graph,ptr)
      
    end )
    nbrs = nbrE(graph,v)
    Enum.each(nbrs.stream,fn(edge_pointer) -> 
      edge = parse_pointer(edge_pointer) 
      del_e(graph,edge_pointer)
      Logger.debug "nbr edges: #{inspect edge}"
    end)
    Dynamo.delete_item(t_name(),[id: id,r: r])
  end
  @doc "wait for a message from children pids from spawn"
  def wait_on([]) do
    Logger.debug "children are done, nice!"
    nil
  end
  def wait_on(children) do
    receive do
      pid when is_pid(pid) -> 
        Logger.debug "child: #{inspect pid} done!"
        wait_on(List.delete(children,pid))
    end
  end
  @doc "delete in edge"
  def del_ie(edge) do
    ie_id = cast_id(edge.target_id,:in_edge)
    #ie_r = cast_id(edge.id,:node) <> Atom.to_string(edge.label)
    ie_r = cast_id(edge.id,:node) <> edge.label
    ie_ptr = {ie_id,ie_r} 
    del_e(graph,ie_ptr)
  end
  @doc "delete out neighbor and edges"
  def del_on(edge) do
    out_n_id = cast_id(edge.id,:out_neighbor)
    out_n_r = cast_id(edge.target_id,:node)
    del_e(graph,{out_n_id,out_n_r})
    Logger.debug "del_on deleting " <> inspect [id: out_n_id,r: out_n_r]
    in_n_id = cast_id edge.target_id,:in_neighbor
    in_r = cast_id edge.id, :node
    ptr = {in_n_id, in_r}
    Logger.debug "deleting in nbr from remote node " <> inspect ptr
    del_e(graph,ptr)
  end
  @doc "delete in neighbor and edges"
  def del_in(edge) do
    in_n_id = cast_id(edge.target_id,:in_neighbor)
    in_n_r = cast_id(edge.id,:node)
    del_e(graph,{in_n_id,in_n_r})
    out_n_id = cast_id edge.id, :out_neighbor
    out_r = cast_id edge.target_id, :node
    ptr = {out_n_id,out_r}
    Logger.debug "deleting out nbr from remote node " <> inspect ptr
    del_e(graph,ptr)
  end
  @doc "delete labels maybe not needed" 
  def del_l(graph,%Ddb.V{id: id, r: r} = v) do
    raise "TODO: figure out if we need this"
    label_id = cast_id(id,:edge_label)
    #Enum.each(stream,fn(raw) ->
      #Dynamo.delete_item(t_name(),[id: item.id, r: item.r])
    #end)
  end
  def v(graph,map) when is_map(map) do
    case Map.has_key?(map,:r) do
      true -> v_id(graph,{map.id,map.r})
      false -> v_id(graph,map.id)
    end
  end
  @doc "this should be @hack tagged"
  def keys_to_atoms(map) do
    Enum.reduce(Map.keys(map),%{}, fn(key,acc) ->
      Map.put(acc,String.to_existing_atom(key),map[key])
    end)
  end
  def v_id(key) do
    v_id(graph(),key)
  end
  def v_id(graph,{nil,_}) do
    raise "v_id/2 can't go fetch a vertex with nil as the id!"
  end
  def v_id(graph,{id,r}) do
    Logger.debug "getting item\n\tid: #{inspect id}\n\tr: #{inspect r}"
    #map = Dynamo.get_item!(t_name(),%{id: id,r: r})
      #|> Dynamo.Decoder.decode() |> keys_to_atoms
    #Logger.debug("raw item: #{inspect map,pretty: true}")
    ## preserve additional attributes by not using as:
    #map = Map.merge(%Ddb.V{},map)
      #|> Dynamo.Decoder.decode(as: Ddb.V)
    case Dynamo.get_item(t_name(),%{id: id, r: r}) do
      {:ok,map} when map == %{} -> 
        Logger.warn "empty result for #{inspect [id,r]}"
        stream = []
      {:ok,raw} when is_map(raw) ->
      #raw -> 
        Logger.debug inspect raw
        stream = [decode_vertex(raw["Item"])]
      doh -> raise "the horror #{inspect doh}"
    end
    #map = Dynamo.get_item!(t_name(),%{id: id, r: r}) 
      #|> decode_vertex
    Map.put(graph,:stream,stream)
  end
  @doc "hack to keep range keys but not require them"
  def v_id(graph,id) when is_binary(id) do
    r = "0"
    v_id(graph,{id,r})
  end
  def v_id(graph,id) when is_number(id) do
    raise "id can't be a number right now, need to implement way to configure schema and table for that type and check it correctly"
  end
  def inE(graph) do
    stream = Stream.flat_map(graph.stream,fn(vertex) ->
      inE(graph,vertex).stream
    end)
    Map.put(graph,:stream,stream)
  end
  def inE(graph,vertex) when is_map(vertex) do
    eav = [id: cast_id(vertex.id,:in_edge)]
    kce = "id = :id "
    stream = Dynamo.stream_query(t_name(),
      expression_attribute_values: eav,
      key_condition_expression: kce)

    stream = Stream.map(stream,fn(raw) ->
      # no maps in in_edges to reduce duplication
      s = Dynamo.Decoder.decode(raw,as: Ddb.E)
    end)
    stream = Stream.map(stream, &({&1.id,&1.r}))
    Map.put(graph,:stream,stream)
  end
  
  @doc "gets all neighbor edges" 
  def nbrE(graph,%Ddb.V{} = vertex) do
    Logger.debug "getting out edges for vertex: #{inspect vertex}"
    eav = [id: cast_id(vertex.id,:in_neighbor)]
    kce = "id = :id"
    stream = Dynamo.stream_query(t_name(),
      expression_attribute_values: eav,
      key_condition_expression: kce)
    in_stream = Stream.map(stream,fn(raw) ->
      Logger.debug "in: "<> inspect raw
      s = Dynamo.Decoder.decode(raw,as: Ddb.N)
    end)
    in_stream = Stream.map(in_stream, &({&1.id,&1.r}))
    Logger.debug "getting out nbrs for vertex: #{inspect vertex}"
    eav = [id: cast_id(vertex.id,:out_neighbor)]
    kce = "id = :id"
    stream = Dynamo.stream_query(t_name(),
      expression_attribute_values: eav,
      key_condition_expression: kce)
    out_stream = Stream.map(stream,fn(raw) ->
      Logger.debug "out: "<> inspect raw
      s = Dynamo.Decoder.decode(raw,as: Ddb.N)
    end)
    out_stream = Stream.map(out_stream, &({&1.id,&1.r}))
    final_stream = Stream.concat(in_stream,out_stream)
    Map.put(graph,:stream,final_stream)
  end
  @doc "get all out edges from stream of vertexes"
  def outE(graph) do
    stream = Stream.flat_map(graph.stream,fn(vertex) ->
      outE(graph,vertex).stream
    end)
    Map.put(graph,:stream,stream)
  end
  @doc "gets all out edges for a single vertex, uses %Trabant.V{} :id"
  def outE(graph,%Ddb.V{} = vertex) when is_map(vertex) do
    Logger.debug "getting edges for vertex: #{inspect vertex}"
    eav = [id: cast_id(vertex.id,:out_edge)]
    kce = "id = :id"
    stream = Dynamo.stream_query(t_name(),
      expression_attribute_values: eav,
      key_condition_expression: kce)
    #Logger.debug "raw Dynamo stream\n\n\n" <> inspect Enum.to_list stream
    #stream = Stream.map(stream, &Dynamo.Decoder.decode(&1,as: Ddb.E))
    stream = Stream.map(stream,fn(raw) ->
      r = Dynamo.Decoder.decode(raw) 
      s = Dynamo.Decoder.decode(raw,as: Ddb.E)
      Map.merge(s,r["map"])
    end)
    #Logger.debug "as Ddb.E stream\n\n\n" <> inspect Enum.to_list(stream), pretty: true
    stream = Stream.map(stream, &({&1.id,&1.r}))
    #Logger.debug "to go into graph stream\n\n\n" <> inspect Enum.to_list(stream), pretty: true

    Map.put(graph,:stream,stream)
  end

  def outE(graph,label) when is_atom(label) do
    outE(graph,Atom.to_string(label))
  end

  @doc "get label, labels should be used for indexing mostly"
  def outE(graph,label_key) when is_binary(label_key)do
    stream = Stream.flat_map(graph.stream,fn(vertex) ->
      # TODO: should consider index for this
      Stream.filter(outE(graph,vertex).stream, fn(edge_pointer) ->
        e = parse_pointer(edge_pointer)
        Logger.debug "outE(#{label_key}) ep: #{inspect e}"
        e[:label] == label_key
      end)
    end)
    Map.put(graph,:stream,stream)
  end
  @doc "match map for outE"
  def outE(graph,match_map) when is_map(match_map) do

    # get vertexes
    stream = Stream.flat_map(graph.stream,fn(vertex) ->
      Logger.debug "match map for outE vertex: #{inspect vertex}"
      # get edge pointers
      edges = outE(graph,vertex)
      checked_edges = check_edges(edges,match_map)
      # remove nil results
      #Logger.debug "checked edges #{inspect Enum.to_list(checked_edges)}"
      #Logger.debug "done checking edges"
      Stream.filter(checked_edges,&(&1 != nil))
      #Enum.filter(edges,&(&1 != nil))
    end)
    #Logger.debug "start"
    #Logger.debug "output stream from outE mmap: #{inspect Enum.to_list(stream)}"
    #Logger.debug "done"
    Map.put(graph,:stream,stream)
  end
  @doc "compares a list of edge pointers to a map to see if the attributes and values exist in the edge"
  defp check_edges(edges,match_map) do
    Stream.map(edges.stream,fn(edge_pointer) ->
      Logger.debug "edge pointer: #{inspect edge_pointer}"
        edge = e(edge_pointer)
        #test if edge matches
        case mmatch(edge,match_map) do
          true ->
            Logger.debug "match: #{}\n\t#{inspect edge_pointer}"
            # TODO: consider option to return %Trabant.E vs edge pointer
            #%Trabant.E{pointer: pointer, a: a, b: b, label: label}
            edge_pointer
         false ->
           #Logger.debug "no match #{inspect edge_pointer}\n\te:  #{inspect edge, pretty: true}"
           nil
       end
    end)
  end
  #@out_reg ~r/^(?<out_id>.*)_(?<label>.*)/
  #@id_reg ~r/^out_edge-(?<id>.*)$/
  def parse_pointer({nil,_}) do
    raise "nil no workie in parse_pointer/1"
  end
  def parse_pointer({aid,bid_and_label}) do
    Logger.debug "parse_pointer \n\t#{inspect aid} \n\t#{inspect bid_and_label}"
    << bid :: binary-size(33),label :: binary >> = bid_and_label
    #%{aid: cast_id(aid,:node),bid: cast_id(bid,:node), label: String.to_existing_atom(label)}
    %{aid: cast_id(aid,:node),bid: cast_id(bid,:node), label: label}
  end
  def parse_pointer(foo) do
    raise "bad pointer #{inspect foo}"
  end
  @doc "fetches unique vertexes from a list of edge pointers"
  def inV(graph) do
    stream = Stream.flat_map(graph.stream,fn(edge_pointer) ->
      Logger.debug "EP: #{inspect edge_pointer}"
      edge = parse_pointer(edge_pointer)
      Logger.debug "inV fetching node id: #{edge.bid}"
      v_id(graph,edge.bid) |> data
    end)
    #TODO: possible infinite loop here
    stream = Stream.uniq(stream)
    Map.put(graph,:stream,stream)
  end
  def inV(graph,key) when is_atom(key) do
    inV(graph, Atom.to_string(key))
  end
  @doc "get verteces with matching attribute keys"
  def inV(graph,key) when is_binary(key) do
    #TODO: should be able to optimize with label indexes
    stream = Stream.filter(inV(graph).stream,fn(vertex) ->
      Map.has_key?(vertex,key)
    end)
    Map.put(graph,:stream,stream)
  end
  def inV(graph,mmap) when is_map(mmap) do
    stream = Stream.filter(inV(graph).stream,fn(vertex) -> 
      mmatch(vertex,mmap)
    end)
    Map.put(graph,:stream,stream)
  end
  def e(graph,{id,r}) do
    e({id,r})
  end
  def e({id,r}) do
    raw = Dynamo.get_item!(t_name(),%{id: id, r: r}) #|> Dynamo.Decoder.decode(as: Ddb.E)
    r = Dynamo.Decoder.decode(raw)
    s = Dynamo.Decoder.decode(raw,as: Ddb.E)
    map = Map.merge(s,r["map"])
    #<< target_id :: binary-size(33), label :: binary >> = map.r
    #target = cast_id(target_id,:)
    #map = Map.put(map,:target, target)
    Logger.debug "e() raw: #{inspect map}"
    map
  end
  def q(graph,map) do
    eav = Map.to_list(map)
    kce = map |> Enum.map(fn({k,v})-> "#{k} = :#{k}" end) |> Enum.join(",")
    Logger.debug "eav: #{inspect eav}\nkce: #{inspect kce}"
    r = Dynamo.stream_query(t_name(),
      expression_attribute_values: eav,
      key_condition_expression: kce)
    raise "TODO: not done yet"
  end
  def all_v(graph) do
    Logger.debug "all_v runs a full table scan!"
    eav = [r: "0"]
    r = Dynamo.stream_scan(t_name(),
      filter_expression: "r = :r",
      expression_attribute_values: eav)
    stream = Stream.map(r,fn(raw) ->
      decode_vertex(raw)
    end)
    Map.put(graph,:stream,stream)
  end
  def all_nbrs(graph) do
    Logger.warn "scanning all neighbors"
    eav = [r: "a"]
    kce = "begins_with (r,:r)"
    r = Dynamo.stream_scan(t_name(),
      filter_expression: "begins_with(r, :r)",
      expression_attribute_values: eav)
    stream = Stream.map(r,fn(raw) ->
      decode_vertex(raw)
    end)
    Map.put(graph,:stream,stream)
  end
  def del_e(graph) do
    stream = Stream.flat_map(graph.stream,fn(edge_pointer) ->
      e = e(edge_pointer)
      Logger.debug "del_e: e:\n\t#{inspect e}"
      del_e(graph,e)
    end)
    Map.put(graph,:stream,stream)
  end
  def del_e(graph,%Ddb.E{} = e) do
    Logger.debug "del_e: deleting map: "<> inspect e 
    Dynamo.delete_item(t_name(),%{id: e.id,r: e.r})
  end
  def del_e(graph,{id,r}) do
    Logger.debug "del_e: deleting tpl: "<> inspect {id,r}
    {:ok,m} = Dynamo.delete_item(t_name(),%{id: id, r: r})
    #Logger.error "del_e" <> inspect r
    :ok
  end
  #def all(graph) do
  #  Dynamo.steam_scan(t_name()) |> Enum.map( &(Dynamo.Decoder.decode(&1)))
  #end
  #  Example of stream_query
  #
  # iex(23)> t |> Dynamo.stream_query(limit: 1, expression_attribute_values: [id: "1"],key_condition_expression: "id = :id")
  #  {:ok,
  #  %{"Count" => 1, "Items" => #Function<25.29647706/2 in Stream.resource/3>,
  #  "ScannedCount" => 1}}
  #  iex(24)> t
  #  "Users-dev"
  #
  defdelegate cast_id(s,a), to: Trabant
  defdelegate parse_id(s), to: Trabant
  defdelegate id_type?(x), to: Trabant
  defdelegate create_binary_id(s), to: Trabant
  defdelegate create_string_id(s), to: Trabant
  defdelegate data(graph), to: Trabant
  defdelegate first(graph), to: Trabant
  defdelegate limit(graph), to: Trabant
  defdelegate limit(graph,limit), to: Trabant
  defdelegate res(graph), to: Trabant
  defdelegate mmatch(target,test), to: Trabant
  defdelegate create_child(graph,opts), to: Trabant
end
