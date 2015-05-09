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
  def create_v(graph,term,label \\[]) do 
    vertex = %Ddb.V{created_at: Timex.Time.now(:secs)} |> Map.merge(term)
    case vertex do
      %Ddb.V{id: id} = vertex when is_binary(id) ->  Dynamo.put_item(@t_name,vertex)
    end
    vertex
  end
  def add_edge(graph,a,b,label) do
    #TODO: perfect case for using Tasks and concurrency
    # setup out edges for a
    map = Map.merge(label,%{created_at: Timex.Time.now(:secs),id: "out_edge-#{a.id}",r: "#{b.id}_#{Poison.encode!(label)}"}) 
    edge = struct(Ddb.V,map)
    Logger.debug inspect edge
    {:ok,%{}} = Dynamo.put_item(@t_name,edge)
  
    # setup in_edges for b
    map = Map.merge(map,%{id: "in_edge_#{b.id}",r: "#{a.id}_#{Poison.encode!(label)}"})
    in_edge = struct(Ddb.V,map)
    Logger.debug inspect in_edge
    {:ok,%{}} = Dynamo.put_item(@t_name,in_edge)
  
    # setup neightbors
    map = %{id: "#{a.id}_nbr",r: b.id}
    a_nbr = struct(Ddb.V,map)
    {:ok,%{}} = Dynamo.put_item(@t_name,a_nbr)

    map = %{id: "#{b.id}_nbr",r: a.id}
    a_nbr = struct(Ddb.V,map)
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
    map = Dynamo.get_item!(@t_name,%{id: id,r: r})|> Dynamo.Decoder.decode(as: Ddb.V)
    Map.put(graph,:stream,[map])
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
      stream = Dynamo.stream_query(@t_name,
        expression_attribute_values: eav,
        key_condition_expression: kce) 
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
        #raise "not done implementing def e(edge)"
        e = parse_edge_label(edge_pointer)
        Logger.debug "got edge label :#{inspect e}"
        Map.has_key?(e.label, label_key )
      end)
    end)
    Map.put(graph,:stream,stream)
  end
  @doc "match map for outE"
  def outE(graph,match_map) when is_map(match_map) do
    stream = Stream.flat_map(graph.stream,fn(vertex) ->
      stream = Stream.map(outE(graph,vertex),fn(edge_pointer) ->
        edge = parse_edge_label(edge_pointer)
        case mmatch(edge.label,match_map) do
          true ->
            #Logger.debug "match: #{inspect edge_pointer}"
            # TODO: consider option to return %Trabant.E vs edge pointer
            #%Trabant.E{pointer: pointer, a: a, b: b, label: label}
            edge_pointer
          false -> nil
        end
      end)
      Stream.filter(stream,&(&1 != nil))
    end)
    Map.put(graph,:stream,stream)
  end
  def parse_edge_label({id,r}) do
    [id,label] = String.split(r,"_")
    label = Poison.decode!(label,keys: :atoms)
    %{label: label,b_id: id} 
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
