defmodule Trabant.Super do
  def start_link(backend) do
    IO.puts "Backend: #{inspect backend}"
  end
end
