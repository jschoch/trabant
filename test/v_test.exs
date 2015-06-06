defmodule VTest do
  use ExUnit.Case

  setup_all do
    Trabant.backend(Ddb)
    #Trabant.backend(Digraph)
    #Trabant.backend(Mdigraph)
    #Trabant.delete_graph
    :ok
  end
  setup do
    Trabant.delete_graph
    Trabant.new
    :ok
  end

  @m %{id: "1",name: "Bob",r: "0"}
  @m2  %{id: "2",name: "Biff",r: "0"}
  @m3  %{id: "3",nick: "Brock",r: "0"}
  @edge_label %{type: "foo"}

  import Trabant
  test "get vertex by term" do
    graph = Hel.create_data

    # test get vertex by term

    result = graph |> v(@m) |> res
    assert result != nil
    [vertex] = result.data
    assert vertex != nil
    assert vertex.id == @m.id, "wrong result #{inspect vertex}"

    # data works as well

    [vertex] = graph |> v(@m) |> data
    assert vertex.id == @m.id
    IO.puts "raw vertex result: \n\n#{inspect result, pretty: true}"
  end
end
