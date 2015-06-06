defmodule outeTest do
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

  


  test "inv(:label) works" do
    graph = Hel.create_data
    result = graph |> v(@m) |> outE |>  res

    # test basic traversal
    chain_result = result.graph |> inV(:name) |> data

    #chain_result =  Enum.to_list(got_graph.stream)
    assert false, "you need to figure out how to cast to a struct and keep the other attributes you add like :name, and :nick, this test will not work without that"
    assert chain_result.data == [@m2], "wrong result #{inspect chain_result}"

  end
  #test "map match works on outE" do
    #graph = create_data
#
    #map_match_result = graph |> v(@m) |> outE(@edge_label) |> res
    #assert map_match_result.data != [], "doh! #{inspect map_match_result}"
    #[edge_pointer] = map_match_result.data
    #edge = e(graph.g,edge_pointer)
    #assert edge != nil
    #assert edge.label == @edge_label, "wrong result #{inspect map_match_result}"
#
    ## test if bad match returns []
#
    #map_match_result = graph |> v(@m) |>  outE(%{nope: :nada}) |> res
    #assert map_match_result.data == [], "wrong result #{inspect map_match_result}"
  #
    #chain_result = graph |> v(@m) |> outE(:lbl) |> inV(:nick) |> res
    #assert chain_result.data == [@m3], "wrong result #{inspect chain_result}"
    #
    ## test re-use stream
#
    #result = chain_result.graph |> outE(:lbl) |> res
    #[edge_pointer] = result.data
    #edge = e(graph.g,edge_pointer)
    #assert match?( %Trabant.E{},edge),"bad match \n\tedge: #{inspect edge} \n\tresult: #{inspect result}"
    #assert edge.label == %{lbl: :back_at_you},"wrong result #{inspect edge}"
  #end
  test "vertex lookups" do
    graph = Hel.createG
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
    graph = Hel.createG
    [alcmene] = graph |> v_id(9) |> data
    [jupiter] = graph |> v_id(2) |> data

    #should be the same as out()
    result = graph |> v_id(2) |> outE |> inV |> res
    assert result.count == 3, "wrong count #{inspect result}"

    # test map match
    result = graph |> v_id(2) |> outE(%{relation: :brother}) |> inV |> res
    assert result.count == 1, "wrong count #{inspect result}"
  end
  test "create_child works" do
    graph = Trabant.new
    source = %{id: "1"}
    create_v(graph,source)
    v = %{id: "2"}
    label = %{test: :test}
    create_child(graph,%{id: source.id,child: v,label: label})
    [got] = graph |> v_id("2") |> data
    assert v.id == got.id
  end
  test "can't use :index_id for vertex" do
    case Trabant.backend != Ddb do
      true ->
        graph = Trabant.new("foo")
        assert_raise(RuntimeError,fn ->
          create_v(graph,%{index_id: 1})
        end)
      false -> nil
    end
  end
  test "create_v actually creates edges" do
    graph = Trabant.new
    v = %{id: 1,node: :foo}
    create_v(graph,v)
    case Trabant.backend do
      Mdigraph ->
        edges = :mdigraph.edges(graph.g)
        assert Enum.count(edges) == 2
      Ddb -> nil
      nope -> raise "not implemented yet #{inspect nope}"
    end
  end
  test "data conenience works" do
    graph = Hel.createG
    [alcmene] = graph |> v_id(9) |> data
    assert is_map(alcmene), "doh! #{inspect alcmene}"
  end
  test "first works" do
    graph = Hel.createG
    [alcmene] = graph |> v_id(9) |> data
    [jupiter] = graph |> v_id(2) |> data
    [pluto] = graph |> v_id(10) |> data

    [first_v] = graph |> v_id(2) |> out |> first |> data
    assert first_v == pluto, "wrong result #{inspect first_v}"
  end
  test "limit works" do
    graph = Trabant.new
    Enum.each(1..100,&(create_v(graph,%{id: &1})))
    result = graph |> all_v |> limit(2) |> res
    assert result.count == 2
  end
  test "sort works" do
    graph = Trabant.new
    Enum.each(1..100,&(create_v(graph,%{id: &1})))
    result = graph |> all_v |> sort |>  limit(2) |> res
    assert result.count == 2
  end
  test "inn works" do
    graph = Hel.createG
    [alcmene] = graph |> v_id(9) |> data
    [jupiter] = graph |> v_id(2) |> data
    [pluto] = graph |> v_id(10) |> data

    ins = graph |> v_id(2) |> inn |> data
    ids = Enum.filter_map(ins,&(&1.id in [10,5]),&(&1.id))
    assert ids == [10,5], "doh! #{inspect ids}"
    [herc] = graph |> v_id(2) |> inn(%{type: :demigod}) |> data
    assert herc.id == 5
  end
  test " graph() returns %Trabant.G{}" do
    graph = graph()
    assert match?(%Trabant.G{}, graph)
  end
  test "update works" do
    graph = Trabant.new
    alcemene = %{id: 1,type: :human}
    Trabant.create_v(graph,alcemene)
    updated_alcemene = Map.put(alcemene,:foo,"FOOOOOO")
    update_v(graph,updated_alcemene)
    [got_alcemene] = graph |> v_id(1) |> data
    assert alcemene != got_alcemene
    assert got_alcemene == updated_alcemene
  end
  test "delete vertex works" do
    graph = Trabant.new
    v = %{id: 1,type: :human}
    create_v(graph,v)
    del_v(graph,v.id)
    got_v = graph |> v_id(1) |> data
    assert got_v == []
    create_v(graph,v)
    del_v(graph,v)
    got_v = graph |> v_id(1) |> data
    assert got_v == []
  end
  test "delete edge works" do
    graph = Trabant.new
    v = %{id: 1,type: :human}
    child = %{id: 2, type: :monster}
    create_v(graph,v)
    create_child(graph,%{id: v.id,child: child,label: :relation})
    [edge] = v(graph,v) |> outE |> data
    del_e(graph,edge)
    got = v(graph,v) |> outE |> data
    assert got == []
    #TODO: consider adding tests to make sure we don't delete :index edge, or the edge to the terminal node
  end
  test "where works" do
    # graph |> v(where: {:age,:gt,10})
    assert false, "TODO: need to get where working"
  end
  test "limit works" do
    # graph |> v(:foo) |> outE(limit: 2)
    assert false, "TODO: get limit working"
  end
  test "don't delete schema" do
    assert false, "TODO: finish checking for schema and test init cases"
  end
  test "[] handled correctly in chain" do
    assert false,"TODO: if stream == [] what is the right thing to do?  should all tests return [] if the stream []"
  end
end
