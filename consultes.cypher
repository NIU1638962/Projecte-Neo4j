//1

MATCH (p:Individual)-[r:FAMILIA]-(p)-[:VIU]-(h:Habitatge)
WHERE 
    r.relacio_harmonitzada = "jefe" AND 
    p.year = 1866 AND
    h.municipi = "CR"  
RETURN p.name AS Nom_Padro

//2
//TODO: Eliminar null is NaN's

MATCH (p:Individual)-[r:FAMILIA]-(p2:Individual)-[:VIU]-(h:Habitatge)
WHERE 
    r.relacio_harmonitzada = "jefe" 
    AND h.municipi = "SFLL"
RETURN DISTINCT(p.name) AS Nom_Padro, p.year AS Any_Padro, collect(DISTINCT p2.surname) as Cognoms, size(collect(p2.surname)) as Num_Habitants

//3

MATCH (h:Habitatge)<-[:VIU]-(i:Individual)
WHERE h.municipi = 'SFLL' AND h.any_padro > 1800 AND h.any_padro < 1845
RETURN h.municipi AS Poblacion, h.any_padro AS AnoPadron, collect(h.Id_llar) AS ListaHabitatges
ORDER BY h.any_padro

//4 municipio null -> (no changes, no records)

MATCH (i:Individual {name: 'rafel', surname: 'marti', year: 1838})-[:VIU]->(h:Habitatge)<-[:VIU]-(i2:Individual)
WHERE h.municipi = 'SFLL'
RETURN i2.name AS Nombre, i2.surname AS Apellido, i2.second_surname AS SegundoApellido

//5

MATCH (i:Individual)-[:SAME_AS*]-(i2:Individual {name: 'miguel', surname: 'estape', second_surname: 'bofill'})
RETURN i

//6

MATCH (i:Individual)-[:SAME_AS*]-(i2:Individual {name: 'miguel', surname: 'estape', second_surname: 'bofill'})
RETURN i2.name AS Nombre, collect(DISTINCT i2.surname) AS Apellidos, collect(DISTINCT i2.second_surname) AS SegundosApellidos

//7

MATCH (i:Individual {name: 'benito', surname: 'julivert'})-[r]->(i2:Individual)
RETURN i2.name AS Nom, i2.surname AS Cognom1, i2.second_surname AS Cognom2, type(r) AS TipoRelacion

//8

MATCH (i:Individual {name: 'benito', surname: 'julivert'})-[r:FAMILIA {relacio_harmonitzada: 'fill'}]->(i2:Individual)
RETURN i2.name AS Nombre, i2.surname AS Apellido1, i2.second_surname AS Apellido2
ORDER BY i2.name

//9

MATCH ()-[r:FAMILIA]->()
RETURN DISTINCT r.relacio_harmonitzada AS TipoRelacionFamiliar

//10

MATCH (h:Habitatge {municipi: 'SFLL'})
WHERE h.carrer IS NOT NULL AND h.numero IS NOT NULL
WITH h.carrer AS Carrer, h.numero AS Numero, collect(h.any_padro) AS Anos, collect(h.Id_llar) AS IdsLlar
RETURN Carrer, Numero, size(Anos) AS TotalPadrons, Anos, IdsLlar
ORDER BY TotalPadrons DESC
LIMIT 15

//11
MATCH (p:Individual)<-[:FAMILIA]-(p)-[r:FAMILIA]->(f:Individual)-[:VIU]->(h:Habitatge)
WITH size(collect(f.name)) AS num_fills, r, h, p, f // ESTO NO FUNCIONA
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
LIMIT 20

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
    size(collect(f))/Num_Habitatges AS Mitja_fills_hab

//13
