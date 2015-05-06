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
    r = graph |> v(m)
    assert r != nil
    assert r.v == m, "wrong result #{inspect r.v}"
    edge_label = %{type: :foo}
    e = add_edge(graph,m,m2,edge_label)
    e2 = add_edge(graph,m,m2,%{type: :bar})
    e3 = add_edge(graph,m,m2,%{lbl: :baz})
    e4 = add_edge(graph,m,m3,%{lbl: :unf})
    e5 = add_edge(graph,m3,m,%{lbl: :back_at_you})
    assert e != nil
    assert e == :ok, "wrong result #{inspect e}"
    outE_result = outE(graph,r.v)
    assert match?( %Trabant.G{} , outE_result),"wrong result #{inspect outE_result}"
    assert Enum.count(outE_result.stream) == 4, "wrong result #{inspect outE_result}"
    c = outE(graph,r.v)|> res
    assert c.count == 4, "wrong result #{inspect outE_result}"
    chain_result = graph |> outE(r.v,:lbl) |> inV(:name) |> res
    assert chain_result.data == [%{name: "Biff"}], "wrong result #{inspect chain_result}"

  
    # test if map match filter works on outE

    map_match_result = graph |> outE(r.v,edge_label) |> res
    [map] = map_match_result.data
    assert map.label == edge_label, "wrong result #{inspect map_match_result}"
  
    chain_result = graph |> outE(r.v,:lbl) |> inV(:nick) |> res
    assert chain_result.data == [%{nick: "Brock"}], "wrong result #{inspect chain_result}"
    back_to_m = chain_result.graph |> outE(:lbl)
    [sub_stream] = Enum.to_list(back_to_m.stream)
    enum = Enum.to_list(sub_stream.stream)
    assert enum == [m],"wrong result #{inspect enum}"
  end
end
