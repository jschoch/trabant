defmodule BackendsTest do
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

  require Hel

  @m Hel.maps.m
  @m2  Hel.maps.m2
  @m3  Hel.maps.m3
  @edge_label %{type: "foo"}

  import Trabant

  test "test outE" do
    graph = Hel.create_data
    [vertex] = graph |> v(@m) |> data
    c = graph
      |> v(vertex)
      |> outE

    lst = c.stream |> Enum.to_list
    assert Enum.count(lst) == 4, "outE borked #{inspect lst}\n\t#{inspect c}"
  end
  test "test count" do
    graph = Hel.create_data
    [vertex] = graph |> v(@m) |> data

    c = graph
      |> v(vertex)
      |> outE
      |> res
    assert c.count == 4, "wrong result #{inspect c}"

  end
  test "gets outE as Ddb.E" do
    graph = Hel.create_data
    r = graph
      |> v(@m3)
      |> outE
      |> data
    e = List.first(r)
    assert match?({_,_},e), "wrong type or result #{inspect e}"
    assert e == {cast_id(@m3.id,:out_edge), @m.id<>"back_at_you"}, "wrong: e: #{inspect e}"

  end
  test "test get out edges for single vertex" do
    graph = Hel.create_data
    [vertex] = graph |> v(@m) |> data

    outE_result = graph |> v(vertex) |> outE() |> res
    assert match?( %Trabant.G{} , outE_result.graph),"bad match wrong result #{inspect outE_result}"
    assert outE_result.count == 4, "wrong count for #{inspect outE_result.data}\n\n#{inspect Enum.to_list(outE_result.graph.stream)}"
  end
  test "test outE with %Ddb.V{}" do
    graph = Hel.create_data
    result = graph
      |> v(@m)
      |> outE(%Ddb.V{id: @m.id})
      |> data
    assert result = [@m], "wrong result #{inspect result}"
  end
  test "test outE with mmap" do
    graph = Hel.create_data
    mmap = %{"lbl" =>  "baz"}
    [result] = graph
      |> v(@m)
      |> outE(mmap)
      |> data
    assert result != nil
    assert match?({_,_},result)
    e = Ddb.parse_pointer(result)
    assert e.label == :unf, "wrong result #{inspect e}"
    assert e.aid == @m.id
    assert e.bid == @m2.id
  end
  test "test outE with key" do
    graph = Hel.create_data
    [vertex] = graph |> v(@m) |> data
    result = graph
      |> v(vertex)
      |> outE(:unf)
    #IO.puts inspect "Result " <> inspect result
    #IO.puts inspect "stream" <> inspect Enum.to_list(result.stream)
    chain_result = res(result)
    assert chain_result.count == 2, "wrong result #{inspect chain_result}"

    #IO.puts "outE result #{inspect chain_result.data}"
  end
  
end
