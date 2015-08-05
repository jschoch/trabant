defmodule TrabantUtilTests do
  use ExUnit.Case
  test "test" do
    something = Trabant.Util.Agent.all
    assert something != nil
  end
end
