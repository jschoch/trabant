defmodule Trabant.Super do
  use Supervisor
  def start_link(backend) do
    IO.puts "Backend: #{inspect backend}"
    #Trabant.Util.Agent.start_link
  end
  def start(_type,_args) do
    import Supervisor.Spec,warn: false
    children = [worker(Trabant.Util.Agent,[])]
    opts = [strategy: :one_for_one, name: BlogTrabant.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
