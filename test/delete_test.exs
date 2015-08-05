defmodule DeleteTest do
  use ExUnit.Case

  setup_all do
    Trabant.backend(Ddb)
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
  def create_and_link do
    m = create_v(graph,@m,:unf)
    n = create_child( m.id,@m2,:snusnu)
    x = create_child(n.id,@m3,:joy)
    {m,n,x}
  end
  test "test del middle node works" do
    {m,n,x} = create_and_link
    #IO.puts "starting point"
    #IO.puts inspect( all( graph,:better),pretty: true)
    #IO.puts "you are dead! " <>  inspect n
    del_v(graph,n) 
    #IO.puts "getting all map"
    map = Trabant.all graph
    #IO.puts "printing graph"
    #{out,nil} = Trabant.Util.print :publish
    #IO.puts out

    #IO.puts "\n\nm,n,x: \t\t" <> inspect %{m: m.id, n: n.id, x: x.id}
    assert map["Count"] == 2,"bad result full scan: \n\n" <> inspect( all( graph,:better),pretty: true) #<> Trabant.Util.print
  end
  test "del top node works" do
    {m,n,x} = create_and_link
    del_v(graph,m)
    map = Trabant.all graph
    #{out,nil} = Trabant.Util.print :publish
    #IO.puts out
    assert map["Count"] == 6
  end
  test "del bottom node works" do
    {m,n,x} = create_and_link
    del_v(graph,x)
    map = Trabant.all graph
    #{out,nil} = Trabant.Util.print :publish
    #IO.puts out
    assert map["Count"] == 6
  end
end
