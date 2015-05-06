defmodule Trabant.V do
  @t_name "graph"
  defstruct table: @t_name,
    id: nil,
    mod: %ModTime{}

  #use Zdb.Base.PK
end
