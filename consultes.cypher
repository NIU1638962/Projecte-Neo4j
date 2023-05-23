//1

MATCH (p:Individual)-[r:FAMILIA]-(p)-[:VIU]-(h:Habitatge)
WHERE 
    r.relacio_harmonitzada = "jefe" AND 
    p.year = 1866 AND
    h.municipi = "CR"  
RETURN p.name AS Nom_Padro

//2

MATCH (p:Individual)-[r:FAMILIA]-(p2:Individual)-[:VIU]-(h:Habitatge)
WHERE 
    r.relacio_harmonitzada = "jefe" 
    AND h.municipi = "SFLL"
RETURN DISTINCT(p.name) AS Nom_Padro, p.year AS Any_Padro, collect(DISTINCT p2.surname) as Cognoms, size(collect(p2.surname)) as Num_Habitants

//3
//4
//5
//6
//7
//8
//9
//10
//11
//12
//13
