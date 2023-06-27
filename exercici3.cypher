//Activitat a
//Comanda 1
:param limit => ( 42);
:param config => ({relationshipWeightProperty: null, includeIntermediateCommunities: false, seedProperty: ''});
:param communityNodeLimit => ( 10);
:param graphConfig => ({nodeProjection: '*', relationshipProjection: {relType: {type: '*', orientation: 'UNDIRECTED', properties: {}}}});
:param generatedName => ('in-memory-graph-1687899940718');

CALL gds.graph.project($generatedName, $graphConfig.nodeProjection, $graphConfig.relationshipProjection, {});

CALL gds.louvain.stream($generatedName, $config)
YIELD nodeId, communityId AS community, intermediateCommunityIds AS communities
WITH gds.util.asNode(nodeId) AS node, community, communities
WITH community, communities, collect(node) AS nodes
RETURN community, communities, nodes[0..$communityNodeLimit] AS nodes, size(nodes) AS size
ORDER BY size DESC
LIMIT toInteger($limit);

//Comanda 2
CALL gds.louvain.stream($generatedName, $config)
YIELD nodeId, communityId AS community, intermediateCommunityIds AS communities
WITH gds.util.asNode(nodeId) AS node, community, communities
WITH community, collect(node) AS nodes
WITH collect(community) as allcommunities
RETURN size(allcommunities);

//Comanda 3
:param limit => ( 42);
:param config => ({relationshipWeightProperty: null});
:param communityNodeLimit => ( 10);
:param graphConfig => ({nodeProjection: '*',  relationshipProjection: {relType: {type: '*', orientation: 'NATURAL', properties: {}}}});
:param generatedName => ('in-memory-graph-2384364960721');

CALL gds.graph.project($generatedName, $graphConfig.nodeProjection, $graphConfig.relationshipProjection, {});

CALL gds.labelPropagation.stream($generatedName, $config)
YIELD nodeId, communityId AS community
WITH gds.util.asNode(nodeId) AS node, community
WITH community, collect(node) AS nodes
RETURN community,  nodes[0..$communityNodeLimit] AS nodes, size(nodes) AS size
ORDER BY size DESC
LIMIT toInteger($limit);

//Comanda 4
CALL gds.labelPropagation.stream($generatedName, $config)
YIELD nodeId, communityId AS community
WITH gds.util.asNode(nodeId) AS node, community
WITH community, collect(node) AS nodes
WITH collect(community) as allcommunities
RETURN size(allcommunities)

//Comanda 5
:param limit => ( 42);
:param config => ({});
:param communityNodeLimit => ( 10);
:param graphConfig => ({nodeProjection: '*',  relationshipProjection: {relType: {type: '*', orientation: 'NATURAL', properties: {}}}});
:param generatedName => ('in-memory-graph-1687902777499');

CALL gds.graph.project($generatedName, $graphConfig.nodeProjection, $graphConfig.relationshipProjection, {});

CALL gds.wcc.stream($generatedName, $config)
YIELD nodeId, componentId AS community
WITH gds.util.asNode(nodeId) AS node, community
WITH community, collect(node) AS nodes
RETURN community,  nodes[0..$communityNodeLimit] AS nodes, size(nodes) AS size
ORDER BY size DESC
LIMIT toInteger($limit);

//Comanda 6
CALL gds.wcc.stream($generatedName, $config)
YIELD nodeId, componentId AS community
WITH gds.util.asNode(nodeId) AS node, community
WITH community, collect(node) AS nodes
WITH collect(community) as allcommunities
RETURN size(allcommunities)

//Apartat b
// Comanda 7
:param limit => ( 42);
:param graphConfig => ({nodeProjection: '*', relationshipProjection: {relType: {type: '*', orientation: 'NATURAL', properties: {}}}});
:param config => ({similarityMetric: 'Jaccard', similarityCutoff: 0.1, degreeCutoff: 1, writeProperty: 'score', writeRelationshipType: 'SIMILAR'});
:param communityNodeLimit => ( 10);
:param generatedName => ('in-memory-graph-1687903714067');

CALL gds.graph.project($generatedName, $graphConfig.nodeProjection, $graphConfig.relationshipProjection, {});

CALL gds.nodeSimilarity.write($generatedName, $config);

MATCH (from)-[rel:`SIMILAR`]-(to)
WHERE rel.`score` IS NOT NULL
RETURN from, to, rel.`score` AS similarity
ORDER BY similarity DESC
LIMIT toInteger($limit);
