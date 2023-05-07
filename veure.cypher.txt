//###### INDIVIDUAL ######
LOAD CSV WITH HEADERS FROM 'https://docs.google.com/spreadsheets/d/e/2PACX-1vTfU6oJBZhmhzzkV_0-avABPzHTdXy8851ySDbn2gq32WwaNmYxfiBtCGJGOZsMgCWjzlEGX4Zh1wqe/pub?output=csv' AS row
WITH toInteger(row.Year) AS y, row.second_surname AS ss, row.surname AS s, row.name AS n, toInteger(row.Id) AS Id
MERGE (i:Individual {Id: Id})
SET i.year = y, i.second_surname = ss, i.surname = s, i.name = n;

CREATE CONSTRAINT UniqueIndividual FOR (i:Individual) REQUIRE i.Id IS UNIQUE;

//### HABITATGES ###
LOAD CSV WITH HEADERS FROM 'https://docs.google.com/spreadsheets/d/e/2PACX-1vT0ZhR6BSO_M72JEmxXKs6GLuOwxm_Oy-0UruLJeX8_R04KAcICuvrwn2OENQhtuvddU5RSJSclHRJf/pub?output=csv' AS row
WITH toInteger(row.Numero) AS n, toInteger(row.Any_Padro) AS ap, row.Carrer AS c, row.Municipi AS m, toInteger(row.Id_Llar) AS Idl
MERGE (i:Habitatge {Id_llar: Idl})
SET i.numero = n, i.any_padro = ap, i.carrer = c, i.municipi = m;

CREATE CONSTRAINT UniqueHabitatge FOR (i:Habitatge) REQUIRE i.Id_llar IS UNIQUE;

//### FAMILIA ###
LOAD CSV WITH HEADERS FROM 'https://docs.google.com/spreadsheets/d/e/2PACX-1vRVOoMAMoxHiGboTjCIHo2yT30CCWgVHgocGnVJxiCTgyurtmqCfAFahHajobVzwXFLwhqajz1fqA8d/pub?output=csv' AS row
WITH toInteger(row.ID_1) AS i1, toInteger(row.ID_2) AS i2, row.Relacio AS r, row.Relacio_Harmonitzada AS rh
MATCH (I1:Individual {Id: i1}), (I2:Individual {Id: i2})
MERGE (I1)-[rel:FAMILIA]-(I2)
SET rel.relacio = r, rel.relacio_harmonitzada = rh;

//### VIU ###
LOAD CSV WITH HEADERS FROM 'https://docs.google.com/spreadsheets/d/e/2PACX-1vRM4DPeqFmv7w6kLH5msNk6_Hdh1wuExRirgysZKO_Q70L21MKBkDISIyjvdm8shVixl5Tcw_5zCfdg/pub?output=csv' AS row
WITH toInteger(row.Year) AS y, toInteger(row.HOUSE_ID) AS hid, toInteger(row.IND) AS iid, row.Location AS l
MATCH (I:Individual {Id: iid}), (H:Habitatge {Id_llar: hid})
MERGE (I)-[rel:VIU]->(H)
SET rel.year = y, rel.location = l;

//### SAME_AS ###
LOAD CSV WITH HEADERS FROM 'https://docs.google.com/spreadsheets/d/e/2PACX-1vTgC8TBmdXhjUOPKJxyiZSpetPYjaRC34gmxHj6H2AWvXTGbg7MLKVdJnwuh5bIeer7WLUi0OigI6wc/pub?output=csv' AS row
WITH toInteger(row.Id_A) AS ida, toInteger(row.Id_B) AS idb
MATCH (ia:Individual {Id: ida}), (ib:Individual {Id: idb})
MERGE (ia)-[:SAME_AS]->(ib);

DROP CONSTRAINT UniqueIndividual;
DROP CONSTRAINT UniqueHabitatge;
