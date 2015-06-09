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
    :ok
  end
 
  test "can use behav" do
    g_name = "some graph"
    g = Foo.new(g_name)
    assert g != nil
    r = Foo.add_edge(g,:foo,:bar,:baz)
    IO.inspect g
    fail = Foo.new(:foo)
  end
  test "can filter by time" do
    assert false, "can we filter nodes and edges by %ModTime{} via timex and exquisite"
  end 
  test "can use ecto as backend" do
    assert false, "mockup ecto as backend"
  end
  test "ideas" do
    assert false, """
    ensure we don't try to pass the wrong types like outE |> outE
    """
  end
  test "mnesia uses disk_copies to replicatate" do
    assert false, "need to setup replication param"
  end
  test "mnesia backup restore" do
    assert false, "backup restore from disk would be nice"
  end
  test "mnesia fails correctly" do
    assert false, "test failure modes and things like {:majority,true}"
  end
  test "can we use Tasks to work in parallel?" do
    assert false, "test using Tasks to do some work mid stream"
  end
end
