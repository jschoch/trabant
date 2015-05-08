defmodule Ddb.V do
  @derive [ExAws.Dynamo.Encodable]
  defstruct [:id, :created_at, :data]
end
defmodule Ddb.E do
  @derive [ExAws.Dynamo.Encodable]
  defstruct [:id, :created_at, :data]
end
defmodule Ddb.N do
  @derive [ExAws.Dynamo.Encodable]
  defstruct [:id, :created_at, :data]
end
defmodule Ddb do
  @behaviour Trabant.B
  @t_name "Users-#{Mix.env}"
  alias ExAws.Dynamo
  require Logger
  def new(s) do
    tbl = Dynamo.create_table(@t_name, "id", %{id: :string}, 1, 1) 
    Logger.debug inspect tbl
    %Trabant.G{g: %{table_name: @t_name, hash_key_name: "id"}}
  end
  def create_v(graph,term,label \\[]) do 
    vertex = %Ddb.V{created_at: Timex.Time.now(:secs)} |> Map.merge(term)
    case vertex do
      %{id: id} = vertex when is_binary(id) ->  Dynamo.put_item(@t_name,vertex)
    end
  end
  def v_id(graph,id) do
    map = Dynamo.get_item!("Users-dev",%{id: id})|> Dynamo.Decoder.decode
    Map.put(graph,:stream,[map])
  end
  defdelegate res(graph), to: Digraph
  defdelegate data(graph), to: Digraph
end
