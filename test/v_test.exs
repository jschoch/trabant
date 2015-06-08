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

  import Trabant

  test "get vertex by term" do
    graph = Hel.create_data
    maps = Hel.maps
    # test get vertex by term

    result = graph |> v(maps.m) |> res
    assert result != nil
    IO.puts inspect result, pretty: true
    [vertex] = result.data
    assert vertex != nil
    assert vertex.id == maps.m.id, "wrong result #{inspect vertex}"

    # data works as well

    [vertex] = graph |> v(maps.m) |> data
    assert vertex.id == maps.m.id
    IO.puts "raw vertex result: \n\n#{inspect result, pretty: true}"
  end
  test "vertex lookups" do
    graph = Hel.createG
    gods = Hel.gods
    alcmene_id = gods.alcmene
    #alcmene = %{age: 45, id: "9",r: "0", name: "Alcmene", type: :human}

    # create some vertexes

    term_with_r = %{id: create_string_id,r: "0",foo: "1234"}
    create_v(graph,term_with_r)
    term = %{id: create_string_id,foo: "1234"}
    create_v(graph,term)

    # fetch vertex from store
    updated_graph = v(graph,term)
    assert match? %Trabant.G{}, updated_graph
    res = res(updated_graph)
    assert res.count == 1, "wrong count for #{inspect res}"
    [got] = res.data
    assert got.id == term.id

    # lookup by id index

    result = graph |> v_id(alcmene_id) |> res
    [got] = result.data
    assert got.id == alcmene_id, "wrong result #{inspect got, pretty: true}"
  end
end
