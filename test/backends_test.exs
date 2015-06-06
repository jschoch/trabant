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
    :ok
  end

  @m %{id: "1",name: "Bob",r: "0"}
  @m2  %{id: "2",name: "Biff",r: "0"}
  @m3  %{id: "3",nick: "Brock",r: "0"}
  @edge_label %{type: "foo"}

  import Trabant

  test " v_id > outE(mmap) |> inV works" do
    graph = Hel.createG
    [alcmene] = graph |> v_id("9") |> data
    [jupiter] = graph |> v_id("2") |> data

    #should be the same as out()
    result = graph |> v_id("2") |> outE |> inV |> res
    assert result.count == 3, "wrong count #{inspect result}"

    # test map match
    result = graph |> v_id("2") |> outE(%{relation: "brother"}) |> inV |> res
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
    v = %{id: "1",node: :foo}
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
    graph = Hel.createG
    [alcmene] = graph |> v_id("9") |> data
    assert is_map(alcmene), "doh! #{inspect alcmene}"
  end
  test "out works" do
    graph = Hel.createG
    list = graph |> v_id("2") |> out |> data
    assert list != nil

  end
  test "first works" do
    graph = Hel.createG
    [alcmene] = graph |> v_id("9") |> data
    [jupiter] = graph |> v_id("2") |> data
    [pluto] = graph |> v_id("10") |> data

    [first_v] = graph |> v_id("2") |> out |> first |> data
    #TODO: need to ensure that pluto is the expected result
    assert first_v.id == pluto.id, "wrong result #{inspect first_v}\n\texpected: #{pluto.id}\n\tgot: #{first_v.id}"
  end
  test "limit works" do
    graph = Trabant.new
    Enum.each(1..100,&(create_v(graph,%{id: Integer.to_string(&1)})))
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
    [alcmene] = graph |> v_id("9") |> data
    [jupiter] = graph |> v_id("2") |> data
    [pluto] = graph |> v_id("10") |> data

    ins = graph |> v_id(2) |> inn |> data
    ids = Enum.filter_map(ins,&(&1.id in [10,5]),&(&1.id))
    assert ids == [10,5], "doh! #{inspect ids}"
    [herc] = graph |> v_id("2") |> inn(%{type: "demigod"}) |> data
    assert herc.id == 5
  end
  test " graph() returns %Trabant.G{}" do
    graph = graph()
    assert match?(%Trabant.G{}, graph)
  end
  test "update works" do
    graph = Trabant.new
    alcemene = %{id: "1",type: :human}
    Trabant.create_v(graph,alcemene)
    updated_alcemene = Map.put(alcemene,:foo,"FOOOOOO")
    update_v(graph,updated_alcemene)
    [got_alcemene] = graph |> v_id("1") |> data
    assert alcemene != got_alcemene
    assert got_alcemene == updated_alcemene
  end
  test "delete vertex works" do
    graph = Trabant.new
    v = %{id: "1",type: :human}
    create_v(graph,v)
    del_v(graph,v.id)
    got_v = graph |> v_id("1") |> data
    assert got_v == []
    create_v(graph,v)
    del_v(graph,v)
    got_v = graph |> v_id("1") |> data
    assert got_v == []
  end
  test "delete edge works" do
    graph = Trabant.new
    v = %{id: "1",type: :human}
    child = %{id: "2", type: :monster}
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
