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
  def all(raw \\false) do
    case raw do
      false -> 
        {:ok, stuff} = Dynamo.scan(@t_name) 
        #Dynamo.Decoder.decode(stuff["Items"])
      true -> Dynamo.scan(@t_name)
    end
  end
  def create_v(graph,term,label \\[]) do 
    vertex = %Ddb.V{created_at: Timex.Time.now(:secs)} |> Map.merge(term)
    case vertex do
      %{id: id} = vertex when is_binary(id) ->  Dynamo.put_item(@t_name,vertex)
    end
    vertex
  end
  def add_edge(graph,a,b,label) do
    #TODO: perfect case for using Tasks and concurrency
    # setup out edges for a
    map = Map.merge(label,%{created_at: Timex.Time.now(:secs),id: "out_edge",r: "#{a.id}_#{b.id}"}) 
    edge = struct(Ddb.V,map)
    Logger.debug inspect edge
    {:ok,%{}} = Dynamo.put_item(@t_name,edge)
  
    # setup in_edges for b
    map = Map.merge(map,%{id: "in_edge",r: "#{b.id}_#{a.id}"})
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
  def v_id(graph,{id,rk}) do
    map = Dynamo.get_item!(@t_name,%{id: id,r: rk})|> Dynamo.Decoder.decode
    Map.put(graph,:stream,[map])
  end
  def inE(graph,vertex) when is_map(vertex) do
    eav = [id: "in_edge",r: "#{vertex.id}_"]
    kce = "id = :id AND begins_with (r,:r)"
    r = Dynamo.stream_query(@t_name,
      expression_attribute_values: eav,
      key_condition_expression: kce)
  end

  def outE(graph,vertex) when is_map(vertex) do
    eav = [id: "out_edge",r: "#{vertex.id}_"]
    kce = "id = :id AND begins_with (r,:r)"
    r = Dynamo.stream_query(@t_name,
      expression_attribute_values: eav,
      key_condition_expression: kce)
  end
  def outE(graph,id) do
    raise "not done yet"
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
  defdelegate res(graph), to: Digraph
  defdelegate data(graph), to: Digraph
end
