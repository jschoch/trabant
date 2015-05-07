defmodule DigraphTest do
  use ExUnit.Case
  
  setup do
    #if (Mix.env != :prod) do
      #Zdb.delete_table("graph",:no_raise)
      #Zdb.create("graph")
    #end
    :ok
  end
  import Digraph 
  test "basic digraph stuff works" do
    graph = new("graph")
    m = %{name: "Bob"}
    m2 = %{name: "Biff"}
    m3 = %{nick: "Brock"}
    create_v(graph,m)
    create_v(graph,m2)
    create_v(graph,m3)
    edge_label = %{type: :foo}
    e = add_edge(graph,m,m2,edge_label)
    e2 = add_edge(graph,m,m2,%{type: :bar})
    e3 = add_edge(graph,m,m2,%{lbl: :baz})
    e4 = add_edge(graph,m,m3,%{lbl: :unf})
    e5 = add_edge(graph,m3,m,%{lbl: :back_at_you})
    assert e != nil
    assert e == :ok, "wrong result #{inspect e}"
  
    # test get vertex by term
  
    result = graph |> v(m) |> res
    [vertex] = result.data
    assert vertex != nil
    assert vertex == m, "wrong result #{inspect vertex}"

    # test get out edges for single vertex

    outE_result = graph |> v(vertex) |> outE() |> res
    assert match?( %Trabant.G{} , outE_result.graph),"bad match wrong result #{inspect outE_result}"
    assert outE_result.count == 4, "wrong count for #{inspect outE_result}\n\n#{inspect Enum.to_list(outE_result.graph.stream)}"

    # test count

    c = graph |> v(vertex) |> outE |> res
    assert c.count == 4, "wrong result #{inspect c}"
    
    # test outE with key

    chain_result = graph |> v(vertex) |> outE(:lbl) |> res
    assert chain_result.count == 2, "wrong result #{inspect chain_result}"
    
    # test basic traversal 

    chain_result = graph |> v(vertex) |> outE(:lbl) |> inV(:name) |> res
    assert chain_result.data == [%{name: "Biff"}], "wrong result #{inspect chain_result}"

  
    # test if map match filter works on outE

    map_match_result = graph |> v(vertex) |> outE(edge_label) |> res
    assert map_match_result.data != [], "doh! #{inspect map_match_result}"
    [edge_pointer] = map_match_result.data
    edge = e(graph.g,edge_pointer)
    assert edge.label == edge_label, "wrong result #{inspect map_match_result}"

    # test if bad match returns []

    map_match_result = graph |> v(vertex) |>  outE(%{nope: :nada}) |> res
    assert map_match_result.data == [], "wrong result #{inspect map_match_result}"
  
    chain_result = graph |> v(m) |> outE(:lbl) |> inV(:nick) |> res
    assert chain_result.data == [%{nick: "Brock"}], "wrong result #{inspect chain_result}"
    
    # test re-use stream

    result = chain_result.graph |> outE(:lbl) |> res
    [edge_pointer] = result.data
    edge = e(graph.g,edge_pointer)
    assert match?( %Trabant.E{},edge),"bad match \n\tedge: #{inspect edge} \n\tresult: #{inspect result}"
    assert edge.label == %{lbl: :back_at_you},"wrong result #{inspect edge} #{inspect e5}"
  end
  test "vertex lookups" do
    graph = Hel.createDi

    # get vertex via term

    alcmene = %{age: 45, id: 9, name: "Alcmene", type: :human}
    updated_graph = v(graph,alcmene)
    assert match? %Trabant.G{}, updated_graph
    res = res(updated_graph)
    assert res.count == 1, "wrong count for #{inspect res}"
    assert res.data == [alcmene]
    
    # lookup by id index

    result = graph |> v_id(9) |> res
    assert res.data == [alcmene]
  end
  test "where works" do
    # graph |> v(where: {:age,:gt,10})
    assert false, "TODO: need to get where working"
  end
  test "limit works" do
    # graph |> v(:foo) |> outE(limit: 2)
    assert false, "TODO: get limit working" 
  end
  test "lookups by index work" do
    assert false, "TODO:  make sure v_id, and create_v work with our special :index_id"
    assert false, "TODO:  make sure we can't use special :index_id attribute and we throw a sane message on error"
    assert false, "TODO: think about id's and if we should pass around whole maps vs just ids, or how to make this optional?"
  end
end
  
