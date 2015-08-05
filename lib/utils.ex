defmodule Trabant.Util do
  require Logger
  import Trabant
  @doc " prints all nodes and edges in graphviz dot format, default returns a 2 tuple with the text, and the raw maps {text,raw_map_list}"
  def printNode(node) do
    start = [~s"""
      <button class='btn btn-xs nodeDetailHide' >Hide</button>
      <button class='btn btn-xs nodeDelete' editNodeId='#{node["id"]}' >Delete</button>
      <button class='btn btn-xs nodeSelect' >Select</button>
      <button class='btn btn-xs nodeSubmit'>Submit Changes</button>
    <ul>

    """]
    strings = Map.keys(node)
      |> Enum.filter(&(!&1 in ~w(r v_type)))
      |> Enum.map( fn(key) ->
        editable = true
        if key in ~w(id created_at label), do: editable = false
        ~s(\t<li>#{key}: <span id='#{key}' contentEditable=#{editable}> #{node[key]}<span></li>)
    end)
    strings = start ++ strings
    out = Enum.join(strings, "\n")
    out = out <> "</ul>"
  end
  def print(test \\:shell) do
    all = Trabant.all(Trabant.graph(),true)
    sorted = sort_all(all)
    IO.puts "url: http://cpettitt.github.io/project/dagre-d3/latest/demo/interactive-demo.html \n\n\n"

    out = "/* Graph */\n\n\ndigraph {\n"
    out = out <> "edge[labelpos=c]\n"
    strings = Enum.map(sorted.nodes, fn(x) ->
      #{trunc_id, _} = << binary.size(8) :: binary>
      << trunc_id :: binary-size(8), _ :: binary>> = x["id"]
      detail = ~s(
        <div class='detail' style='display:inline'>
          #{printNode(x)}
        </div>
      )
      icon = ~s(<i class='sicon glyphicon glyphicon-info-sign nodeToggle'> X </i>)
      trunc = ~s(<div class='nodeLabel'> truncated id: #{trunc_id} <br> name: #{x["name"]} label: #{x["label"]} </div>)
      label = ~s(<div> #{trunc} #{icon} #{detail}  </div>)
      ~s("#{x["id"]}" [labelType="html" label="#{label}"];)
    end)
    out = out <> Enum.join(strings,"\n")
    strings = Enum.map(sorted.edges, fn(x) ->
      source = Trabant.cast_id(x["id"],:node)
      target = Trabant.cast_id(x["target_id"],:node)
      label = ~s(<span class='edgeLabel'>#{x["label"]}</span>)
      ~s("#{source}" -> "#{target}" [ labelType="html" label="#{label}" ];)
    end)
    out = out <> Enum.join(strings,"\n")
    out = out <> "\n}\n"
    case test do
      :shell ->
        IO.puts out
        {nil, nil}
      :publish ->
        {out,nil}
      :maps ->
        sorted
    end
  end
  def clean do
    Logger.warn "Scanning entire datastore.  This could take a very long time"
    clean_edges |> Enum.to_list
    clean_nbrs |> Enum.to_list
  end
  def clean_nbrs do
    x = Ddb.all_nbrs graph
    Stream.each(x.stream,fn(nbr) ->
      if !vertex_exists?(nbr.r), do: ExAws.Dynamo.delete_item(Ddb.t_name(),[id: nbr.id,r: nbr.r])
    end)
  end
  def clean_edges do
    map = all_v( graph()) |> outE
    Stream.each(map.stream,fn(edge_pointer) ->
      edge = parse_pointer(edge_pointer)
      Logger.warn inspect edge
      if !verify_edge(edge), do: del_e(graph(),edge_pointer) 
    end)
  end
  @doc "tests both sides of the edge, removes the edge if either side doesn't have a vertex"
  def verify_edge(edge) do
    test = vertex_exists?(edge.aid) && vertex_exists?(edge.bid)
  end
  @doc "return true if a node with the id arg exists"
  def vertex_exists?(v_id) do
    Logger.warn "testing v_id: #{v_id}"
    case v_id(v_id)|> data do
      [] -> 
        Logger.warn "Bad vertex #{v_id} does not exist"
        false
      [_] -> true
    end
  end
  def publish do
    {dot,nil} = print(:publish)
    id = Trabant.create_string_id
    Idgon.DagreAgent.put_graph(id,dot)
    Logger.info "published id: #{id}"
    "/admin/dagre/#{id}"
  end
  def sort_all(list,nbrs \\true) do
    Enum.reduce(list,%{nodes: [], edges: []},fn(x,acc) ->
      case x do
        %{"v_type" =>  "node"} ->
          IO.puts "node"
          acc = Map.put(acc,:nodes,[x|acc.nodes])
        %{"e_type" => et} ->
          IO.puts "edge :#{inspect et}"
          case et do
            "out" ->
              acc = Map.put(acc,:edges,[x|acc.edges])
            "in" -> nil
          end
        %{"nbr_type" => nt} -> 
          case nbrs do
            false -> IO.puts "skip neighbor"
            true ->
              Logger.debug inspect x
              n = %{"id" =>  x["id"],"target_id" => x["r"]}
              acc = Map.put(acc,:edges,[n|acc.edges])
          end
        nil -> raise "WTF NIL!"
        doh -> IO.puts "unknown type, what to do? \ndoh: #{inspect doh}\nx: #{inspect x}"
      end
      acc
    end)
  end
end

