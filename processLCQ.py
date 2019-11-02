import json
table = {'http://dbpedia.org/property/':'dbp:', 'http://dbpedia.org/ontology/':'dbo:', 'http://dbpedia.org/resource/':'dbr:', 'https://www.w3.org/1999/02/22-rdf-syntax-ns#type':'rdf:type'}
dbr = 'http://dbpedia.org/resource/'
rdft1 = 'https://www.w3.org/1999/02/22-rdf-syntax-ns#type'
rdft2 = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
yesnowords = {'is', 'are', 'were', 'was', 'been', 'do', 'does', 'did', 'has', 'have', 'had'}
# f = open('./FullyAnnotated_LCQuAD5000left.json')
f = open('train-data.json')
# gdq = open('LCQ_CDQ', 'w')
# gde = open('LCQ_CDE', 'w')
# gds = open('LCQ_CDS', 'w')

data = json.load(f)

# newdata2 = [d for d in data if len(d['entity mapping'])==2]

def process(newdata, gq, gs, ge):
    for d in newdata:
        if d['question'].split()[0].lower() not in yesnowords:
            gq.write(d['question']+'\n')
            gs.write(d['sparql_query']+'\n')
            es = ''
            for e in d['entity mapping']:
                es += e['label']+'|<'+e['uri']+'>;'
            es = es[:-1]
            ge.write(es+'\n')

def processTrain(data):
    for d in data:
        sline = d['sparql_query'].strip()
        sline = sline[sline.index('{')+1:sline.index('}')].strip()
        if sline.count(dbr) == 2:
            sq = [s for s in sline.split('. ') if len(s.strip())!=0 and rdft1 not in s and rdft2 not in s]
            if len(sq) == 3:
                print(d['corrected_question'])
                # for t in sq:
                #     for k, v in table.items():
                #         t = t.replace(k, v)
                #     r = t.split()[1]

processTrain(data)
# process(newdata2, gdq, gds, gde)