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

  @m Hel.maps.m
  @m2  Hel.maps.m2
  @m3  Hel.maps.m3
  @edge_label %{type: "foo"}

  import Trabant
  test "inV(graph) works" do
    graph = Hel.create_data
    g = graph
      |> v(@m)
      |> outE
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
    [item] = chain_result

    #chain_result =  Enum.to_list(got_graph.stream)
    assert item.id == @m2.id, "wrong result #{inspect chain_result}"

  end
  test "inV(%{foo: \"foo\"}) works" do
    graph = Hel.create_data
    r = graph |> v(@m) 
      |> outE 
      |> inV(%{name: "Biff"}) 
      |> data
    assert r != [], "empty result! was []"
    [item] = r
    assert item.id == @m2.id 
  end
end
