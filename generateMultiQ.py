from SPARQLWrapper import SPARQLWrapper,JSON
sparql = SPARQLWrapper("https://dbpedia.org/sparql")
dbr = 'http://dbpedia.org/resource/'
dbo = 'http://dbpedia.org/ontology/'
dbp = 'http://dbpedia.org/property/'
sf = open('multiQ', 'w')
query = 'SELECT DISTINCT ?u1 ?p0 ?x0 ?p1 ?x1 ?p2 ?u2 FROM <http://dbpedia.org> WHERE { {?u1 ?p0 ?x0} UNION {?x0 ?p0 ?u1} ?u1 a dbo:Actor.{?x0 ?p1 ?x1} UNION {?x1 ?p1 ?x0}.{?x1 ?p2 ?u2} UNION {?u2 ?p2 ?x1}. ?u2 a dbo:Actor.\
    FILTER(regex(?p0,\"dbpedia.org\") && !regex(?p0,"wikiPage") && !regex(?p0,\"before\") && !regex(?p0,\"after\")\
    && regex(?p1,"dbpedia.org") && !regex(?p1,"wikiPage") && !regex(?p1,"before") && !regex(?p1,"after")\
    && regex(?p2,"dbpedia.org") && !regex(?p2,"wikiPage") && !regex(?p2,"before") && !regex(?p2,"after")\
    && ?u1 != ?x0 && ?u1 != ?x1 && ?u1 != ?u2 && ?u2 != ?x0 && ?u2 != ?x1)}'
sparql.setQuery(query)
sparql.setTimeout(200)
sparql.setReturnFormat(JSON)
results = sparql.query().convert()
final = []
for t in results['results']['bindings']:
    final.append(t['u1']['value'].replace(dbr, 'dbr:'))
    final.append(t['p0']['value'].replace(dbo, 'dbo:').replace(dbp, 'dbp:'))
    final.append(t['x1']['value'].replace(dbr, 'dbr:'))
    final.append(t['p1']['value'].replace(dbo, 'dbo:').replace(dbp, 'dbp:'))
    final.append(t['x2']['value'].replace(dbr, 'dbr:'))
    final.append(t['p2']['value'].replace(dbo, 'dbo:').replace(dbp, 'dbp:'))
    final.append(t['u2']['value'].replace(dbr, 'dbr:'))
    sf.write(' '.join(final)+'\n')