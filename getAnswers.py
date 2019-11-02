from SPARQLWrapper import SPARQLWrapper,JSON
sparql = SPARQLWrapper("https://dbpedia.org/sparql")
sf = open('LCQ_SS')
slines = sf.readlines()
saf = open('LCQ_SSA', 'w')
for sline in slines:
    sparql.setQuery(sline.strip())
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
    for t in results['results']['bindings']:
        if 'uri' in t:
            saf.write(t['uri']['value']+';')
        elif 'callret-0' in t:
            saf.write(t['callret-0']['value']+';')
    saf.write('\n')