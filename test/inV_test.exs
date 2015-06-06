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
  test "inV(graph) works" do
    graph = Hel.create_data
    g = graph
      |> v(@m)
      |> outE
      #|> outE(@m)
      |> inV
    result = data(g)
    assert result != nil
    IO.puts inspect result, pretty: true
  end
    test "inv(:label) works" do
    graph = Hel.create_data
    result = graph |> v(@m) |> outE |>  res

    # test basic traversal
    chain_result = result.graph |> inV(:name) |> data

    #chain_result =  Enum.to_list(got_graph.stream)
    assert chain_result.data == [@m2], "wrong result #{inspect chain_result}"

  end
end
