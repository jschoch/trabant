defmodule Ddb.V do
  @derive [ExAws.Dynamo.Encodable]
  #defstruct [:id,:r, :created_at]
  defstruct id: nil, r: "0", created_at: Timex.Time.now(:secs)
end
defmodule Ddb.E do
  @derive [ExAws.Dynamo.Encodable]
  #defstruct [:id,:r, :created_at]
  defstruct id: nil, r: nil, created_at: Timex.Time.now(:secs)
end
defmodule Ddb.N do
  @derive [ExAws.Dynamo.Encodable]
  #defstruct [:id,:r, :created_at]
  defstruct id: nil, r: nil, created_at: Timex.Time.now(:secs)
end
defmodule Ddb do
  @behaviour Trabant.B
  @t_name "Graph-#{Mix.env}"
  alias ExAws.Dynamo
  require Logger
  def new() do
    tbl = Dynamo.create_table(@t_name,[id: :hash,r: :range],[id: :string,r: :string], 1, 1)
    #tbl = Dynamo.create_table(@t_name,[id: :hash],[id: :string], 1, 1)
    Logger.debug inspect tbl
    %Trabant.G{g: %{table_name: @t_name, hash_key_name: "id",range_key_name: "r"}}
  end
  def new(string) do
    Logger.warn "use @t_name, this will ignore arg string: #{string} for new"
    new
  end
  def delete_graph() do
    Logger.warn "deleting table #{@t_name}"
    Dynamo.delete_table(@t_name)
  end
  def all(raw \\false) do
    case raw do
      false ->
        {:ok, stuff} = Dynamo.scan(@t_name)
        #Dynamo.Decoder.decode(stuff["Items"])
      true -> Dynamo.stream_scan(@t_name) |> Enum.map(&Dynamo.Decoder.decode(&1))
    end
  end
  @test_id_reg ~r/_/
  def test_id(id) do
    case Regex.match?(@test_id_reg,id) do
      true -> raise "bad id: we suck and cant' deal with _ in id: #{inspect id}"
      false -> nil
    end
  end
  def create_v(graph,term,label \\[])
  #def create_v(graph,term,label \\[]) do
    #create_v(graph,term,label)
  #end
  def create_v(graph,%{id: id}, label) when is_number(id) do
    raise "can't use integer id's until we workout how to get the table creation types aligned and correct"
  end
  def create_v(graph,%{id: id, r: r} = term,label) when is_binary(id) do
    test_id(id)
    vertex = %Ddb.V{created_at: Timex.Time.now(:secs)} |> Map.merge(term)
    res = Dynamo.put_item(@t_name,vertex)
    Logger.info inspect res
    vertex
  end
  @doc "another hack for range key"
  def create_v(graph,%{id: id} = term,label) when is_binary(id) do
    test_id(id)
    r = "0"
    vertex = %Ddb.V{created_at: Timex.Time.now(:secs),r: r} |> Map.merge(term)
    create_v(graph,vertex)
  end
  def add_edge(graph,a,b,label) do
    #TODO: perfect case for using Tasks and concurrency
    # setup out edges for a
    out_edge = Map.merge(%Ddb.E{},%{created_at: Timex.Time.now(:secs),id: "out_edge-#{a.id}",r: "#{b.id}_#{Poison.encode!(label)}"})
    #edge = struct(Ddb.E,map)
    Logger.debug inspect out_edge
    {:ok,%{}} = Dynamo.put_item(@t_name,out_edge)

    # setup in_edges for b
    in_edge = Map.merge(%Ddb.E{},%{id: "in_edge_#{b.id}",r: "#{a.id}_#{Poison.encode!(label)}"})
    #in_edge = struct(Ddb.E,map)
    Logger.debug inspect in_edge
    {:ok,%{}} = Dynamo.put_item(@t_name,in_edge)

    # setup neightbors
    map = %{id: "#{a.id}_onbr",r: b.id}
    a_nbr = struct(Ddb.N,map)
    {:ok,%{}} = Dynamo.put_item(@t_name,a_nbr)

    map = %{id: "#{b.id}_inbr",r: a.id}
    a_nbr = struct(Ddb.N,map)
    {:ok,%{}} = Dynamo.put_item(@t_name,a_nbr)
    out_edge
  end
  def decode_vertex({:ok, %{}}) do
    raise "empty"
  end
  def decode_vertex(raw) do
    map = Dynamo.Decoder.decode(raw) 
      |> keys_to_atoms
    Map.merge(%Ddb.V{},map)
  end
  @nid_reg ~r/^(?<id>.+)_[i|o]nbr$/
  def id_from_neighbor(s) do
    r = Regex.named_captures(@nid_reg,s)
    r["id"]
  end
  def out(graph) do
    stream = Stream.flat_map(graph.stream,fn(vertex) ->
      graph = out(graph,vertex) 
      Logger.debug "out(graph): \n\n\t#{inspect Enum.to_list(graph.stream)}"
      graph.stream
    end)
    Map.put(graph,:stream,stream)
  end
  def out(graph,vertex) do
    eav = [id: "#{vertex.id}_onbr"]
    kce = "id = :id "
    r = Dynamo.stream_query(@t_name,
      expression_attribute_values: eav,
      key_condition_expression: kce)
    stream = Stream.flat_map(r,fn(raw) -> 
      item = Dynamo.Decoder.decode(raw,as: Ddb.N)
      #[id|tail] = String.split(item.id,"_")
      id = id_from_neighbor(item.id)
      g = v_id(graph,id)
      g.stream
      #decode_vertex(raw) 
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
    eav = [id: "#{vertex.id}_inbr"]
    kce = "id = :id "
    r = Dynamo.stream_query(@t_name,
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
    #Dynamo.update_item(@t_name,id, 
  end
  @doc "deletes a list of vertexes from a stream" 
  def del_v(graph) do
    Stream.each(graph.stream,fn(vertex) ->
      del_v(graph,vertex)
    end)
    #TODO: do we need to put in some metadata here?
    Map.put(graph,:stream,[])
  end
  @doc "deletes a vertex"
  def del_v(graph,%Ddb.V{id: id,r: r}) do
    Dynamo.delete_item(@t_name,[id: id,r: r])
    raise "need to delete edges and neighbors"
  end
  def v(graph,map) when is_map(map) do
    v_id(graph,{map.id,map.r})
  end
  @doc "this should be @hack tagged"
  def keys_to_atoms(map) do
    Enum.reduce(Map.keys(map),%{}, fn(key,acc) ->
      Map.put(acc,String.to_existing_atom(key),map[key])
    end)
  end
  def v_id(graph,{nil,_}) do
    raise "v_id/2 can't go fetch a vertex with nil as the id!"
  end
  def v_id(graph,{id,r}) do
    Logger.debug "getting item\n\tid: #{inspect id}\n\tr: #{inspect r}"
    #map = Dynamo.get_item!(@t_name,%{id: id,r: r})
      #|> Dynamo.Decoder.decode() |> keys_to_atoms
    #Logger.debug("raw item: #{inspect map,pretty: true}")
    ## preserve additional attributes by not using as:
    #map = Map.merge(%Ddb.V{},map)
      #|> Dynamo.Decoder.decode(as: Ddb.V)
    case Dynamo.get_item(@t_name,%{id: id, r: r}) do
      {:ok,map} when map == %{} -> 
        Logger.warn "empty result for #{inspect [id,r]}"
        stream = []
      {:ok,raw} when is_map(raw) ->
      #raw -> 
        Logger.debug inspect raw
        stream = [decode_vertex(raw["Item"])]
      doh -> raise "the horror #{inspect doh}"
    end
    #map = Dynamo.get_item!(@t_name,%{id: id, r: r}) 
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
  def inE(graph,vertex) when is_map(vertex) do
    eav = [id: "in_edge",r: "#{vertex.id}_"]
    kce = "id = :id AND begins_with (r,:r)"
    r = Dynamo.stream_query(@t_name,
      expression_attribute_values: eav,
      key_condition_expression: kce)
  end
  @doc "get all out edges from stream of vertexes"
  def outE(graph) do
    stream = Stream.flat_map(graph.stream,fn(vertex) ->
      #eav = [id: "out_edge-#{vertex.id}"]
      #kce = "id = :id"
      #Dynamo.stream_query(@t_name,
        #expression_attribute_values: eav,
        #key_condition_expression: kce)  #|> Enum.to_list
      outE(graph,vertex).stream
    end)
    Map.put(graph,:stream,stream)
  end
  @doc "gets all out edges for a single vertex, uses %Trabant.V{} :id"
  def outE(graph,%Ddb.V{} = vertex) when is_map(vertex) do
    Logger.debug "getting edges for vertex: #{inspect vertex}"
    eav = [id: "out_edge-#{vertex.id}"]
    kce = "id = :id"
    stream = Dynamo.stream_query(@t_name,
      expression_attribute_values: eav,
      key_condition_expression: kce)
    #Logger.debug "raw Dynamo stream\n\n\n" <> inspect Enum.to_list stream
    #stream = Stream.map(stream, &Dynamo.Decoder.decode(&1,as: Ddb.E))
    stream = Stream.map(stream,fn(raw) ->
      map = Dynamo.Decoder.decode(raw) 
      s = Dynamo.Decoder.decode(raw,as: Ddb.E)
      Map.merge(s,raw)
    end)
    Logger.debug "as Ddb.E stream\n\n\n" <> inspect Enum.to_list(stream), pretty: true
    stream = Stream.map(stream, &({&1.id,&1.r}))
    #Logger.debug "to go into graph stream\n\n\n" <> inspect Enum.to_list(stream), pretty: true

    Map.put(graph,:stream,stream)
  end

  @doc "get label, labels should be used for indexing mostly"
  def outE(graph,label_key) when is_atom(label_key)do
    Logger.debug "outE(:lbl)\n\t'G'" <> inspect graph
    stream = Stream.flat_map(graph.stream,fn(vertex) ->
      # TODO: seems like a hack but not sure how to get the matching labels
      # need a manditory :label attribute or something
      #Logger.debug "outE: vertex" <> inspect vertex
      Stream.filter(outE(graph,vertex).stream, fn(edge_pointer) ->
        e = parse_pointer(edge_pointer)
        #Logger.debug "got edge label :#{inspect e}"
        Map.has_key?(e["label"], label_key )
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
        edge = parse_pointer(edge_pointer)
        #test if edge matches
        case mmatch(edge["label"],match_map) do
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
  @out_reg ~r/^(?<out_id>.*)_(?<label>.*)/
  @id_reg ~r/^out_edge-(?<id>.*)$/
  def parse_pointer({nil,_}) do
    raise "nil no workie in parse_pointer/1"
  end
  def parse_pointer({a,b}) do
    map = Regex.named_captures(@id_reg,a) |> Map.merge(Regex.named_captures(@out_reg,b))
    #Logger.debug "parse_pointer \n\t#{inspect a} \n\t#{inspect b}\n\t#{inspect map}"
    Map.put(map,"label",Poison.decode!(map["label"],keys: :atoms))
  end
  @doc "fetches unique vertexes from a list of edge pointers"
  def inV(graph) do
    stream = Stream.flat_map(graph.stream,fn(edge_pointer) ->
      Logger.debug "EP: #{inspect edge_pointer}"
      out_edge = parse_pointer(edge_pointer)
      Logger.debug "inV fetching node id: #{out_edge["out_id"]}"
      v_id(graph,out_edge["out_id"]) |> data
    end)
    #TODO: possible infinite loop here
    stream = Stream.uniq(stream)
    Map.put(graph,:stream,stream)
  end
  def inV(graph,key) when is_atom(key) do
    stream = Stream.filter(inV(graph).stream,fn(vertex) ->
      #Logger.debug "FUCK YOU \n\t#{inspect vertex}"
      Map.has_key?(vertex,key)
    end)
    Map.put(graph,:stream,stream)
  end
  def inV(graph,mmap) when is_map(mmap) do
    raise "TODO: implement me"
  end
  def e(graph,{id,r}) do
    e({id,r})
  end
  def e({id,r}) do
    i = Dynamo.get_item!(@t_name,%{id: id, r: r}) |> Dynamo.Decoder.decode
    #TODO need a regex here
    [_,a_id] = String.split(id,"-")
    [b_id|t] = String.split(r,"_")

    #%Trabant.E{pointer: pointer, a: a, b: b, label: label}
  end
  def q(graph,map) do
    eav = Map.to_list(map)
    kce = map |> Enum.map(fn({k,v})-> "#{k} = :#{k}" end) |> Enum.join(",")
    Logger.debug "eav: #{inspect eav}\nkce: #{inspect kce}"
    r = Dynamo.stream_query(@t_name,
      expression_attribute_values: eav,
      key_condition_expression: kce)
    raise "TODO: not done yet"
  end
  def all_v(graph) do
    Logger.debug "all_v runs a full table scan!"
    eav = [r: "0"]
    r = Dynamo.stream_scan(@t_name,
      filter_expression: "r = :r",
      expression_attribute_values: eav)
    stream = Stream.map(r,fn(raw) ->
      decode_vertex(raw)
    end)
    Map.put(graph,:stream,stream)
  end
  #  Example of stream_query
  #
  # iex(23)> t |> Dynamo.stream_query(limit: 1, expression_attribute_values: [id: "1"],key_condition_expression: "id = :id")
  #  {:ok,
  #  %{"Count" => 1, "Items" => #Function<25.29647706/2 in Stream.resource/3>,
  #  "ScannedCount" => 1}}
  #  iex(24)> t
  #  "Users-dev"
  #
  defdelegate data(graph), to: Trabant
  defdelegate first(graph), to: Trabant
  defdelegate limit(graph), to: Trabant
  defdelegate limit(graph,limit), to: Trabant
  defdelegate res(graph), to: Trabant
  defdelegate mmatch(target,test), to: Trabant
  defdelegate create_child(graph,opts), to: Trabant
end
