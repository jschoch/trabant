defmodule Hel do
  import Trabant
  def createG do
    saturn = %{id: 1,name: "Saturn",age: 10000,type: :titan}
    jupiter = %{id: 2,name: "Jupiter", type: :god, age: 5000}
    sky = %{id: 3,name: "sky",type: :location}
    sea = %{id: 4,name: "sea",type: :location}
    hercules = %{id: 5,name: "Hercules", type: :demigod, age: 30}
    nemean = %{id: 6,name: "Nemean", type: :monster}
    cerberus = %{id: 7,name: "Cerberus", type: :monster}
    hydra = %{id: 8,name: "Hydra", type: :monster}
    alcmene = %{id: 9,name: "Alcmene",age: 45,type: :human}
    pluto = %{id: 10,name: "Pluto",age: 4000, type: :god}
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
      {jupiter,saturn,%{relation: :father}},
      {jupiter,sky,%{relation: :lives,reason: "loves fresh breezes"}},
      {jupiter,pluto,%{relation: :brother}},
      {pluto, jupiter,%{relation: :brother}},
      {pluto, cerberus,%{relation: :pet}},
      {hercules,jupiter,%{relation: :father}},
      {hercules,alcmene,%{relation: :mother}},
      {hercules, nemean, %{relation: :battled}},
      {hercules, hydra,%{relation: :battled}},
      {hercules, cerberus,%{relation: :battled}}
    ]
    g = new("graph")
    Enum.each(nodes, &(Trabant.create_v(g,&1)))
    IO.puts inspect Trabant.all_v(g) |> data
    Enum.each(edges, &(Trabant.add_edge(g,&1)))
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
