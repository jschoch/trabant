defmodule Ddb.V do
  @derive [ExAws.Dynamo.Encodable]
  defstruct [:id,:r, :created_at]
end
defmodule Ddb.E do
  @derive [ExAws.Dynamo.Encodable]
  defstruct [:id,:r, :created_at]
end
defmodule Ddb.N do
  @derive [ExAws.Dynamo.Encodable]
  defstruct [:id,:r, :created_at]
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
    res = Dynamo.put_item(@t_name,vertex)
    Logger.info inspect res
    vertex
  end
  def add_edge(graph,a,b,label) do
    #TODO: perfect case for using Tasks and concurrency
    # setup out edges for a
    map = Map.merge(label,%{created_at: Timex.Time.now(:secs),id: "out_edge-#{a.id}",r: "#{b.id}_#{Poison.encode!(label)}"}) 
    edge = struct(Ddb.E,map)
    Logger.debug inspect edge
    {:ok,%{}} = Dynamo.put_item(@t_name,edge)
  
    # setup in_edges for b
    map = Map.merge(map,%{id: "in_edge_#{b.id}",r: "#{a.id}_#{Poison.encode!(label)}"})
    in_edge = struct(Ddb.E,map)
    Logger.debug inspect in_edge
    {:ok,%{}} = Dynamo.put_item(@t_name,in_edge)
  
    # setup neightbors
    map = %{id: "#{a.id}_nbr",r: b.id}
    a_nbr = struct(Ddb.N,map)
    {:ok,%{}} = Dynamo.put_item(@t_name,a_nbr)

    map = %{id: "#{b.id}_nbr",r: a.id}
    a_nbr = struct(Ddb.N,map)
    {:ok,%{}} = Dynamo.put_item(@t_name,a_nbr)
    edge
  end
  def out(graph,vertex) do
    eav = [id: "#{vertex.id}_nbr"]
    kce = "id = :id "
    r = Dynamo.stream_query(@t_name,
      expression_attribute_values: eav,
      key_condition_expression: kce) 
  end
  def inn(graph,vertex) do
    eav = [id: "#{vertex.id}_nbr"]
    kce = "id = :id "
    r = Dynamo.stream_query(@t_name,
      expression_attribute_values: eav,
      key_condition_expression: kce)
  end
  def v(graph,map) when is_map(map) do
    v_id(graph,{map.id,map.r})
  end
  def v_id(graph,{id,r}) do
    map = Dynamo.get_item!(@t_name,%{id: id,r: r})
      |> Dynamo.Decoder.decode() |> keys_to_atoms
    Map.put(graph,:stream,[map])
  end
  @doc "this should be @hack tagged"
  def keys_to_atoms(map) do
    Enum.reduce(Map.keys(map),%{}, fn(key,acc) ->
      Map.put(acc,String.to_existing_atom(key),map[key]) 
    end)
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
  def outE(graph) do
    stream = Stream.flat_map(graph.stream,fn(vertex) ->
      #r = outE(graph,vertex)
      #Logger.debug("FUCK #{inspect(r.stream |> Enum.to_list)}")
      #r
      eav = [id: "out_edge-#{vertex.id}"]
      kce = "id = :id"
      Logger.debug "outE: \n\tvertex: #{inspect vertex}\n\teav: #{inspect eav}\n\tkce: #{inspect kce}"
      Dynamo.stream_query(@t_name,
      #Dynamo.query(@t_name,
        expression_attribute_values: eav,
        key_condition_expression: kce)  #|> Enum.to_list
    end)
    Map.put(graph,:stream,stream)
  end
  def outE(graph,%Ddb.V{} = vertex) when is_map(vertex) do
    eav = [id: "out_edge-#{vertex.id}"]
    kce = "id = :id"
    stream = Dynamo.stream_query(@t_name,
      expression_attribute_values: eav,
      key_condition_expression: kce)
    stream = Stream.map(stream, &Dynamo.Decoder.decode(&1,as: Ddb.E))
    stream = Stream.map(stream, &({&1.id,&1.r}))
    Map.put(graph,:stream,stream)
  end
  @doc "TODO: this is kind of a hack, not really sure where it fits, shoudl consider removing it"
  def outE(graph,label_key) when is_atom(label_key)do
    stream = Stream.flat_map(graph.stream,fn(vertex) ->
      # TODO: seems like a hack but not sure how to get the matching labels
      # need a manditory :label attribute or something
      Stream.filter(outE(graph,vertex).stream, fn(edge_pointer) ->
        e = parse_pointer(edge_pointer)
        Logger.debug "got edge label :#{inspect e}"
        Map.has_key?(e["label"], label_key )
      end)
    end)
    Map.put(graph,:stream,stream)
  end
  @doc "match map for outE"
  def outE(graph,match_map) when is_map(match_map) do
    stream = Stream.flat_map(graph.stream,fn(vertex) ->
      edges = Stream.map(outE(graph,vertex),fn(edge_pointer) ->
        edge = parse_pointer(edge_pointer)
        case mmatch(edge["label"],match_map) do
          true ->
            #Logger.debug "match: #{inspect edge_pointer}"
            # TODO: consider option to return %Trabant.E vs edge pointer
            #%Trabant.E{pointer: pointer, a: a, b: b, label: label}
            edge_pointer
          false -> nil
        end
      end)
      Stream.filter(edges,&(&1 != nil))
    end)
    Map.put(graph,:stream,stream)
  end
  @out_reg ~r/^(?<out_id>.*)_(?<label>.*)/
  @id_reg ~r/^out_edge-(?<id>.*)$/
  def parse_pointer({a,b}) do
    map = Regex.named_captures(@id_reg,a) |> Map.merge(Regex.named_captures(@out_reg,b))
    Logger.debug "parse_pointer \n\t#{inspect a} \n\t#{inspect b}\n\t#{inspect map}"
    Map.put(map,"label",Poison.decode!(map["label"],keys: :atoms))
  end
  def inV(graph) do
    stream = Stream.flat_map(graph.stream,fn(edge_pointer) ->
      Logger.debug "EP: #{inspect edge_pointer}"
      out_edge = parse_pointer(edge_pointer)
      Logger.debug "inV fetching node id: #{out_edge["out_id"]}"
      v_id(graph,out_edge["out_id"]) |> data
      #[vertex
    end)
    Map.put(graph,:stream,stream)
  end
  def inV(graph,key) when is_atom(key) do
    stream = Stream.filter(inV(graph).stream,fn(vertex) ->
      Logger.debug "FUCK YOU \n\t#{inspect vertex}"
      Map.has_key?(vertex,key)
    end)
    Map.put(graph,:stream,stream)
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
