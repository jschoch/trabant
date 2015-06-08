defmodule Hel do
  import Trabant
  @m %{id: create_string_id,name: "Bob",r: "0",i: "m"}
  @m2  %{id: create_string_id,name: "Biff",r: "0",i: "m2"}
  @m3  %{id: create_string_id,nick: "Brock",r: "0",i: "m3"}
  def maps do
    %{
      m: @m,
      m2: @m2,
      m3: @m3
    }
  end
  @edge_label %{type: "foo"}
  def create_data do
    graph = Trabant.new("graph")
    create_v(graph,@m)
    create_v(graph,@m2)
    create_v(graph,@m3)
    e = add_edge(graph,@m,@m2,:xxx,@edge_label)
    e2 = add_edge(graph,@m,@m2,:con,%{type: :bar})
    e3 = add_edge(graph,@m,@m2,:unf,%{lbl: :baz})
    e4 = add_edge(graph,@m,@m3,:unf,%{lbl: :unf})
    e5 = add_edge(graph,@m3,@m,:back_at_you,%{lbl: :back_at_you})
    graph
  end
  @gods %{
    saturn: create_string_id,
    alcmene: create_string_id,
    jupiter: create_string_id,
    hurcules: create_string_id,
    pluto: create_string_id
  }
  def gods do
    @gods
  end
  

  def createG do
    saturn = %{id: @gods.saturn,r: "0",name: "Saturn",age: 10000,type: :titan}
    jupiter = %{id: @gods.jupiter,r: "0",name: "Jupiter", type: :god, age: 5000}
    sky = %{id: create_string_id,r: "0",name: "sky",type: :location}
    sea = %{id:  create_string_id,r: "0",name: "sea",type: :location}
    hercules = %{id: @gods.hurcules,r: "0",name: "Hercules", type: :demigod, age: 30}
    nemean = %{id: create_string_id,r: "0",name: "Nemean", type: :monster}
    cerberus = %{id: create_string_id,r: "0",name: "Cerberus", type: :monster}
    hydra = %{id: create_string_id,r: "0",name: "Hydra", type: :monster}
    # old id 9
    alcmene = %{id: @gods.alcmene,r: "0",name: "Alcmene",age: 45,type: :human}
    pluto = %{id: @gods.pluto,r: "0",name: "Pluto",age: 4000, type: :god}
    nodes = [
      saturn,
      sky,
      sea,
      jupiter,
      hercules,
      alcmene,
      cerberus,
      hydra,
      pluto,
      nemean
    ]
    edges = [
      {jupiter,saturn,:father,%{relation: "father"}},
      {jupiter,sky,:lives,%{relation: "lives",reason: "loves fresh breezes"}},
      {jupiter,pluto,:brother,%{relation: "brother"}},
      {pluto, jupiter,:brother,%{relation: "brother"}},
      {pluto, cerberus,:pet,%{relation: "pet"}},
      {hercules,jupiter,:father,%{relation: "father"}},
      {hercules,alcmene,:mother,%{relation: "mother"}},
      {hercules, nemean,:battled, %{relation: "battled"}},
      {hercules, hydra,:battled,%{relation: "battled"}},
      {hercules, cerberus,:battled,%{relation: "battled"}}
    ]
    g = new("graph")
    Enum.each(nodes, &(Trabant.create_v(g,&1)))
    #IO.puts inspect Trabant.all_v(g) |> data
    #Enum.each(edges, &(Trabant.add_edge(g,&1)))
    Enum.each(edges, fn({a,b,label,map}) ->
      Trabant.add_edge(g,a,b,label,map)
    end)
    g
  end
  def veryBig() do
    g = new
    Enum.each(1..1000,&(create_v(g,%{id: &1,type: :user})))
    Enum.each(1..100,fn(id) ->
      Enum.each(1..10000,fn(x) ->
        random_id = :random.uniform(10000000) * x
        v = %{id: random_id,type: :image}
        create_child(g,%{id: id, child: v,label: :image})
      end)
    end)
    g
  end
end
