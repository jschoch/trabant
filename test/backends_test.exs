defmodule OuteTest do
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
    #Hel.createG 
    :ok
  end
  require Hel
  @m Hel.maps.m
  @m2  Hel.maps.m2
  @m3  Hel.maps.m3
  @edge_label %{type: "foo"}
  @gods Hel.gods
  import Trabant

  test " v_id > outE(mmap) |> inV works" do
    Hel.createG
    graph = graph
    [alcmene] = graph |> v_id(@gods.alcmene) |> data
    [jupiter] = graph |> v_id(@gods.jupiter) |> data

    #should be the same as out()
    result = graph |> v_id(@gods.jupiter) 
      |> outE 
      |> inV 
      |> res
    assert result.count == 3, "wrong count #{inspect result}"

    # test map match
    result = graph |> v_id(@gods.jupiter) 
      |> outE(%{"relation" => "brother"}) 
      |> inV 
      |> res
    assert result.count == 1, "wrong count #{inspect result}"
  end
  test "ensure we don't clobber :label" do
    assert false, "TODO: implement me"
  end
  test "create_child works" do
    graph = graph
    aid = create_string_id
    source = %{id: aid}
    create_v(graph,source)
    bid = create_string_id
    v = %{id: bid}
    label = :test
    create_child(graph,%{id: source.id,child: v,label: label})
    [got] = graph |> v_id(bid) |> data
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
    graph = graph
    aid = create_string_id
    v = %{id: aid,node: :foo}
    create_v(graph,v)
    case Trabant.backend do
      Mdigraph ->
        edges = :mdigraph.edges(graph.g)
        assert Enum.count(edges) == 2
      Ddb -> nil
      nope -> raise "not implemented yet #{inspect nope}"
    end
  end
  test "data convenience works" do
    graph = graph
    id = create_string_id
    create_v(graph,%{id: id})
    r = graph |> v_id(id) |> data
    assert r != []
    [v] = r
    assert is_map(v), "doh! #{inspect v}"
    assert v.id == id
  end
  test "out works" do
    Hel.createG
    graph = graph
    list = graph |> v_id(@gods.pluto) |> out |> data
    assert list != nil
    assert list != []
    assert Enum.count(list) == 2,"wrong size: #{inspect list}"

  end
  test "first works" do
    graph = graph
    lst = Hel.createN(graph,5)
    [a,b,c|d] = lst
    create_child(graph,%{id: a,child: b,label: :pal})
    create_child(graph,%{id: a,child: c,label: :pal})
    r = graph |> v_id(a.id) |> out |> first |> data
    assert r != [], "empty result! got []"
    [first_v] = r
    assert first_v.id == a.id, "wrong result #{inspect first_v}\n\texpected: #{a.id}\n\tgot: #{first_v.id}"
  end
  test "limit works" do
    graph = graph
    Hel.createN(graph,20)
    result = graph |> all_v |> limit(2) |> res
    assert result.count == 2
  end
  test "sort works" do
    graph = graph
    Hel.createN(graph,20)
    result = graph |> all_v |> sort |>  limit(2) |> res
    assert result.count == 2
  end
  test "inn and inn(%{filter: :on}) works" do
    Hel.createG
    graph = graph
    [alcmene] = graph |> v_id(@gods.alcmene) |> data
    [jupiter] = graph |> v_id(@gods.jupiter) |> data
    [pluto] = graph |> v_id(@gods.pluto) |> data

    ins = graph |> v_id(@gods.alcmene) |> inn |> data
    assert ins != [],"crap []"
    ids = Enum.filter_map(ins,&(&1.id in [@gods.pluto,@gods.hurcules]),&(&1.id))
    assert ids == [@gods.hurcules,@gods.pluto], "doh! #{inspect ids}\n\t#{inspect ins,pretty: true}"
    [herc] = graph |> v_id(@gods.alcmene) |> inn(%{type: "demigod"}) |> data
    assert herc.id == @gods.hercules
  end
  test " graph() returns %Trabant.G{}" do
    graph = graph()
    assert match?(%Trabant.G{}, graph)
  end
  test "update works" do
    graph = graph
    id = create_string_id
    alcmene = Map.merge(%Ddb.V{},%{id: id,type: "human"})
    Trabant.create_v(graph,alcmene)
    updated_alcmene = Map.put(alcmene,:foo,"FOOOOOO")
    update_v(graph,updated_alcmene)
    r = graph |> v_id(id) |> data
    assert r != [], "crap []"
    [got_alcmene] = r
    assert alcmene != got_alcmene
    assert got_alcmene == updated_alcmene
  end
  test "delete vertex works" do
    graph = graph
    aid = create_string_id
    v = Map.merge(%Ddb.V{},%{id: aid,type: :human})
    create_v(graph,v)
    del_v(graph,v)
    got_v = graph |> v_id(aid) |> data
    assert got_v == []
    create_v(graph,v)
    del_v(graph,v)
    got_v = graph |> v_id(aid) |> data
    assert got_v == []
  end
  test "delete edge works" do
    graph = graph
    id = create_string_id
    v = %{id: id,type: :human}
    id2 = create_string_id
    child = %{id: id2, type: :monster}
    create_v(graph,v)
    create_child(graph,%{id: v.id,child: child,label: :relation})
    [edge_pointer] = v(graph,v) |> outE |> data
    edge = e(graph,edge_pointer)
    del_e(graph,edge)
    got = v(graph,v) |> outE |> data
    assert got == []
    #TODO: consider adding tests to make sure we don't delete :index edge, or the edge to the terminal node
  end

  test "ensure delete cleans out neighbors" do
    graph = graph
    id = create_string_id
    v = %{id: id,type: :human,txt: "this is v node"}
    id2 = create_string_id
    child = %{id: id2, type: :monster,txt: "this is child node"}
    v = create_v(graph,v)
    create_child(graph,%{id: v.id,child: child,label: :relation,map: %{txt: "this is v to child"}})
    v_graph = v(graph,v)
    [edge_pointer] = v_graph |> outE |> data
    neighbors = v_graph |> out |> data
    assert Enum.count(neighbors) > 0
    del_v(graph,v)
    IO.puts "\n\n" <> inspect all(graph,true), pretty: true
    neighbors = v_graph |> out |> data
    assert Enum.count(neighbors) == 0
    out_edges = v_graph |> outE |> data
    assert Enum.count(out_edges) == 0
  end
  test "where works" do
    # graph |> v(where: {:age,:gt,10})
    assert false, "TODO: need to get where working"
  end
  test "limit works" do
    # graph |> v(:foo) |> outE(limit: 2)
    assert false, "TODO: get limit working"
  end

  test "add a test for e() somewhere" do
    assert false, "TODO: test e()" 
  end
  test "don't delete schema" do
    assert false, "TODO: finish checking for schema and test init cases"
  end
  test "[] handled correctly in chain" do
    assert false,"TODO: if stream == [] what is the right thing to do?  should all tests return [] if the stream []"
  end
end
