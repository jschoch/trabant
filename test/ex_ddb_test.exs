defmodule DdbTest do
  use ExUnit.Case
  setup_all do
    #Trabant.start
    Trabant.backend(Ddb)
    Trabant.delete_graph
    :ok
  end
  test "some stuff from hacking" do


    graph = Trabant.new
    # note we merge so any attribute will be pushed into ddb
    # should look at the actual table to see if they are unique attributs vs the map as json
    node = %{id: "1",name: :foo,r: "a"}
    Trabant.create_v(graph,node)
    [got] = Ddb.v_id(graph,{"1","a"}) |> Ddb.data
    assert got.id == node.id
  end
end
