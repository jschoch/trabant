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
  test "can omit an id and the 'graph'" do
    v = create_v(%{foo: :bar},:i_am_a_label)
    r = v_id(v.id) |> data
    assert r != []
    [got] = r
    assert v.id == got.id
  end
  test "create_child can omit id" do
    v = create_v(%{foo: :bar},:i_am_label)
    child = create_child(v.id,%{baz: :biz},:also_label)
    assert Map.has_key?(child,:id), "no key for child #{inspect child}"
    children = v_id(v.id) |> out |> data
    assert children != [] ,"fuck\nall: #{inspect(all(graph(),true),pretty: true)}"
  end
end
