defmodule DdbTest do
  use ExUnit.Case
  setup_all do
    Trabant.start
  end
  test "some stuff from hacking" do


    graph = Ddb.new
    # note we merge so any attribute will be pushed into ddb
    # should look at the actual table to see if they are unique attributs vs the map as json
    Ddb.create_v(graph,%{id: "1",name: :foo})
    Ddb.v_id(graph,"1") |> Ddb.data
  end
end
