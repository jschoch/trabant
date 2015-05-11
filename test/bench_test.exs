defmodule BenchesTest do
  require Benchmark
  import Trabant
  use ExUnit.Case
  def start_agent do
    Agent.start_link(fn -> nil end,name: :foo) 
  end
  def aput(g) do
    Agent.update(:foo,fn _ -> g end)
  end
  def aget do
    Agent.get(:foo, fn x -> x end)
  end
  def bench1(g,count \\10000,msg \\"") do
    IO.puts "bench1 #{msg}"
    r = Benchmark.times count, do: v(g,%{id: 1}) |> res
    IO.puts inspect r, pretty: true
    r = Benchmark.times count, do: v_id(g,1) |> res
    IO.puts inspect r, pretty: true 
  end
  def bench2(g,count \\10000,msg \\"") do
    IO.puts "bench2 #{msg}"
    r = Benchmark.times count, do: v_id(g,:random.uniform(100)) |> outE |> data
    IO.puts inspect r,pretty: true
  end
  def bench3(g,count \\10000,msg \\"") do
    IO.puts "bench3 out #{msg}"
    r = Benchmark.times count, do: v_id(g,:random.uniform(100)) |> out |> data
    IO.puts inspect r,pretty: true
  end
  def bench4(g,count \\10000,msg \\"") do
    IO.puts "bench4 out #{msg}"
    r = Benchmark.times count, do: v_id(g,:random.uniform(100)) |> out |> limit(5)|> data
    IO.puts inspect r,pretty: true
  end
  setup_all do
    #Trabant.backend(Mdigraph)    
    import Trabant
    case Trabant.backend do
      Digraph -> g = new
      Mdigraph -> 
        mdg = Trabant.get_graph
        g = %Trabant.G{g: mdg}
        case Trabant.v(g,1) do
          %{id: 1} ->
            IO.puts "found table"
          _ -> 
            IO.puts "creating table"
            g = new      
        end
    end
    create_v(g,%{id: 1})
    bench1(g,10000,"in setup")
    bench2(g,10000,"in setup bench2")
    start_agent
    {timed,:ok} = :timer.tc(fn-> 
      g = Hel.veryBig 
      aput(g)
      :ok
    end)
    IO.puts "setup took: #{timed}" 
    :ok
  end
  @tag timeout: 1000000
  test "can do big stuff" do
    graph = aget
    bench1(graph,10000,"in test")
    bench2(graph,10000,"in test")
    bench3(graph,10000,"in test")
    bench4(graph,10000,"in test")
    assert match?( %Trabant.G{},graph), "bad match: #{inspect graph}"
    [node] = graph |> v_id(1)  |> data
    assert node != nil
    assert false, "need some tests" 
  end
  test "use 10 threads" do
    assert false, "use 10 concurrent threads"
  end
end
