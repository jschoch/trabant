defmodule BackupTest do
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
  import Trabant
  import Hel
  @m Hel.maps.m
  test "dump works" do
    Hel.create_data
    name = "test-BackupTest"
    {res,backup_name} = dump(name)
    assert res == :ok
    {res,txt} = File.read( backup_name )
    assert res == :ok
    assert txt != nil
    assert txt != "[]"
  end
  test "restore works" do
    x = restore "backups/backup-test-BackupTest-1438864946934923015"
    IO.puts inspect( all( graph), pretty: true)
    assert x != nil
    res = v_id(@m.id) |> data
    assert res != []
    [node] = res
    assert node.id == @m.id
  end
end
