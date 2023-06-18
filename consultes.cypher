//1

MATCH (p:Individual)-[:VIU]->(h:Habitatge)
WHERE h.any_padro = 1866 AND h.municipi = "CR"
RETURN count(p) AS Num_Habs, collect(p.surname) AS Cognoms;

//2

MATCH (p:Individual)-[:VIU]->(h:Habitatge)
WHERE h.municipi = "SFLL"
RETURN h.any_padro AS Any_Padro, count(p) AS Num_Habs, collect(DISTINCT p.surname) AS Cognoms;

//3

MATCH (h:Habitatge)<-[:VIU]-(i:Individual)
WHERE h.municipi = 'SFLL' AND h.any_padro > 1800 AND h.any_padro < 1845
RETURN h.municipi AS Poblacion, h.any_padro AS AnoPadron, collect(h.Id_llar) AS ListaHabitatges
ORDER BY h.any_padro;

//4 municipio null -> (no changes, no records)

MATCH (i:Individual {name: 'rafel', surname: 'marti', year: 1838})-[:VIU]->(h:Habitatge)<-[:VIU]-(i2:Individual)
WHERE h.municipi = 'SFLL'
RETURN i2.name AS Nombre, i2.surname AS Apellido, i2.second_surname AS SegundoApellido;

//5

MATCH (i:Individual)-[:SAME_AS*]-(i2:Individual {name: 'miguel', surname: 'estape', second_surname: 'bofill'})
RETURN i;

//6

MATCH (i:Individual)-[:SAME_AS*]-(i2:Individual {name: 'miguel', surname: 'estape', second_surname: 'bofill'})
RETURN i2.name AS Nombre, collect(DISTINCT i2.surname) AS Apellidos, collect(DISTINCT i2.second_surname) AS SegundosApellidos;

//7

MATCH (i:Individual {name: 'benito', surname: 'julivert'})-[r]->(i2:Individual)
RETURN i2.name AS Nom, i2.surname AS Cognom1, i2.second_surname AS Cognom2, type(r) AS TipoRelacion;

//8

MATCH (i:Individual {name: 'benito', surname: 'julivert'})-[r:FAMILIA {relacio_harmonitzada: 'fill'}]->(i2:Individual)
RETURN i2.name AS Nombre, i2.surname AS Apellido1, i2.second_surname AS Apellido2
ORDER BY i2.name;

//9

MATCH ()-[r:FAMILIA]->()
RETURN DISTINCT r.relacio_harmonitzada AS TipoRelacionFamiliar;

//10

MATCH (h:Habitatge {municipi: 'SFLL'})
WHERE h.carrer IS NOT NULL AND h.numero IS NOT NULL
WITH h.carrer AS Carrer, h.numero AS Numero, collect(h.any_padro) AS Anos, collect(h.Id_llar) AS IdsLlar
RETURN Carrer, Numero, size(Anos) AS TotalPadrons, Anos, IdsLlar
ORDER BY TotalPadrons DESC
LIMIT 15;

//11
MATCH (p:Individual)<-[:FAMILIA]-(p)-[r:FAMILIA]->(f:Individual)-[:VIU]->(h:Habitatge)
WITH size(collect(f.name)) AS num_fills, r, h, p, f
WHERE 
    r.relacio_harmonitzada =~ "fill*" OR
    r.relacio =~ "hij*" AND
    num_fills > 3 AND
    h.municipi = "SFLL" 
RETURN 
    p.name AS Nom_Cap, 
    p.surname as Cognoms_Cap, 
    collect(f.name) as Nom_fill
ORDER BY size(Nom_fill) DESC
LIMIT 20;

//12

MATCH (p:Individual)<-[:FAMILIA]-(p)-[r:FAMILIA]->(f:Individual)-[:VIU]->(h:Habitatge)
WHERE
    r.relacio_harmonitzada =~ "fill*" OR
    r.relacio =~ "hij*" AND
    p.year < 1881 AND
    f.year = 1881 AND
    h.municipi = "SFLL"
CALL { MATCH (h:Habitatge) WHERE h.municipi = "SFLL" RETURN size(collect(h)) AS Num_Habitatges }
RETURN
    size(collect(f)) AS Tot_fills,
    Num_Habitatges,
    size(collect(f))/Num_Habitatges AS Mitja_fills_hab;

//13

CALL {
    MATCH (i:Individual)-[:VIU]->(h:Habitatge)
    WHERE h.municipi = "SFLL" 
    WITH h.any_padro AS year , h.carrer AS carrer, count(i.name) AS habs_carrer
    RETURN year, min(habs_carrer) AS min_habs
}
MATCH (i:Individual)-[:VIU]->(h:Habitatge)
WITH h, h.any_padro AS Year , h.carrer AS carrer, count(i.name) AS habs_carrer, min_habs
WHERE h.municipi = "SFLL" AND habs_carrer = min_habs
RETURN DISTINCT Year, carrer, min_habs
ORDER BY Year ASC;
