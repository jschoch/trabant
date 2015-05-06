defmodule Foo do
  @behaviour Trabant.B
  def new(s) do
    :digraph.new()
  end
  @spec add_edge(Trabant.graph,any,any,any) :: :ok
  def add_edge(graph,a,b,label) do
    :ok
  end
end
defmodule TrabantTest do
  use ExUnit.Case
  
  setup do
    #if (Mix.env != :prod) do
      #Zdb.delete_table("graph",:no_raise)
      #Zdb.create("graph")
    #end
    :ok
  end
 
  test "can use behav" do
    #Trabant.create_v(%{foo})
    g_name = "some graph"
    g = Foo.new(g_name)
    assert g != nil
    r = Foo.add_edge(g,:foo,:bar,:baz)
    IO.inspect g
    fail = Foo.new(:foo)
  end
end
