defmodule DdbTest do
  use ExUnit.Case
  setup_all do
    #Trabant.start
    Trabant.backend(Ddb)
    Trabant.delete_graph
    :ok
  end
  import Trabant
  test "test basics" do
    graph = new
    # note we merge so any attribute will be pushed into ddb
    # should look at the actual table to see if they are unique attributs vs the map as json
    id = create_string_id
    node = %{id: id,name: :foo}
    create_v(graph,node)
    [got] = v_id(graph,id) |> data
    assert got.id == node.id
  end
  test "add edge" do
    graph = new
    aid = create_string_id
    a = %{id: aid,name: :foo}
    create_v(graph,a)
    bid = create_string_id
    b = %{id: bid,name: :foo}
    create_v(graph,b)
    # add edge by id
    add_edge(graph,a.id,b.id,:friend,%{ismap: true})
    # add edge by map
    add_edge(graph,a,b,:map_friend)
  end
end
