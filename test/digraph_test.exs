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
    alcmene = %{age: 45, id: 9, name: "Alcmene", type: :human}

    # get vertex via term
    updated_graph = v(graph,alcmene)
    assert match? %Trabant.G{}, updated_graph
    res = res(updated_graph)
    assert res.count == 1, "wrong count for #{inspect res}"
    assert res.data == [alcmene]
    
    # lookup by id index

    result = graph |> v_id(9) |> res
    assert res.data == [alcmene]
  end
  test "inV works" do
    graph = Hel.createDi
    [alcmene] = graph |> v_id(9) |> data
    [jupiter] = graph |> v_id(2) |> data

    #should be the same as out()
    result = graph |> v_id(2) |> outE |> inV |> res
    assert result.count == 3, "wrong count #{inspect result}"

    # test map match
    result = graph |> v_id(2) |> outE(%{relation: :brother}) |> inV |> res
    assert result.count == 1, "wrong count #{inspect result}"
  end
  test "can't use :index_id for vertex" do
    graph = new("foo")
    assert_raise(RuntimeError,fn ->
      create_v(graph,%{index_id: 1})
    end)
  end
  test "data conenience works" do
    graph = Hel.createDi
    [alcmene] = graph |> v_id(9) |> data
    assert is_map(alcmene), "doh! #{inspect alcmene}"
  end
  test "first works" do
    graph = Hel.createDi
    [alcmene] = graph |> v_id(9) |> data
    [jupiter] = graph |> v_id(2) |> data
    [pluto] = graph |> v_id(10) |> data

    [first_v] = graph |> v_id(2) |> out |> first |> data
    assert first_v == pluto, "wrong result #{inspect first_v}"
  end
  test "limit works" do
    graph = new
    Enum.each(1..100,&(create_v(graph,%{id: &1})))
    result = graph |> all_v |> limit(2) |> res
    assert result.count == 2 
  end
  test "sort works" do
    graph = new
    Enum.each(1..100,&(create_v(graph,%{id: &1})))
    result = graph |> all_v |> sort |>  limit(2) |> res
    assert result.count == 2
  end
  test "inn works" do
    graph = Hel.createDi
    [alcmene] = graph |> v_id(9) |> data
    [jupiter] = graph |> v_id(2) |> data
    [pluto] = graph |> v_id(10) |> data

    ins = graph |> v_id(2) |> inn |> data
    ids = Enum.filter_map(ins,&(&1.id in [10,5]),&(&1.id))
    assert ids == [10,5], "doh! #{inspect ids}" 
    [herc] = graph |> v_id(2) |> inn(%{type: :demigod}) |> data
    assert herc.id == 5
  end
  test "where works" do
    # graph |> v(where: {:age,:gt,10})
    assert false, "TODO: need to get where working"
  end
  test "limit works" do
    # graph |> v(:foo) |> outE(limit: 2)
    assert false, "TODO: get limit working" 
  end
  test "TODOs" do
    assert false, "TODO: think about id's and if we should pass around whole maps vs just ids, or how to make this optional?"
  end
end
  
