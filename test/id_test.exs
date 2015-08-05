defmodule IdTest do
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

  test "ids work as expected" do
    id = create_binary_id(:node)
    assert byte_size(id) == 17
    parsed = parse_id(id)
    assert byte_size(parsed.bid) == 16
    assert is_bitstring(id)
    id = create_string_id()
    s = parse_id(id)
    assert is_binary(s.sid) , "wrong type: #{inspect s.sid}"
    assert byte_size(s.sid) == 32
    id_type = id_type?(id)
    assert id_type == :node
    #IO.puts inspect parsed,pretty: true
  end
  test "create edge id works correctly" do
    id = create_string_id(:out_edge)
    parsed = parse_id(id)
    id_type = id_type?(id)
    assert id_type == :out_edge
    id = cast_id(id,:node)
    assert id_type?(id) == :node
  end
end
