defmodule Trabant.Util.Agent do
  @moduledoc """
    Agent for putting and getting dagre graphs from trabant

  """
  require Logger
  def start_link do
    Logger.info "#{__MODULE__} started"
    Agent.start_link(fn -> HashDict.new end, name: __MODULE__)
  end

  @doc "Checks if the task has already executed"
  def get_graph(id) do
    Agent.get(__MODULE__, &HashDict.get(&1,id))
  end
  def all() do
    Agent.get(__MODULE__,&(&1))
  end
  def ids do
    Agent.get(__MODULE__, &HashDict.keys(&1))
  end
  @doc "Marks a task as executed"
  def put_graph(id, dot) do
    Logger.error "TODO: you need to limit this somehow, should set a max and prune > max"
    Agent.update(__MODULE__, &HashDict.put(&1, id,dot))
  end
  def stop() do
    Agent.stop(__MODULE__)
  end
end

