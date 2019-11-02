import string
import json

table = {'http://dbpedia.org/property/':'dbp:', 'http://dbpedia.org/ontology/':'dbo:', 'http://dbpedia.org/resource/':'dbr:', 'https://www.w3.org/1999/02/22-rdf-syntax-ns#type':'rdf:type'}

f = open("qald-9-train-multilingual.json")
qf = open("qald-9_question.txt", "w")
# rf = open("qald-9_relation.txt", "w")
# ef =  open("qald-9_entity.txt", "w")
data = json.load(f)['questions']
for d in data:
    qls = d['question']
    for ql in qls:
        if ql['language'] == 'en':
            q = ql['string']
            break
    line = d['query']['sparql']
    query = ""
    while "{" in line:
        line = line[line.index('{')+1:]
        line1 = line[:line.index('}')]
        if "filter" in line1:
            line1 = line1[:line.index("filter")]
        elif "FILTER" in line1:
            line1 = line1[:line.index("FILTER")]
        query += line1 + " . "
    for k, v in table.items():
        query = query.replace(k, v)
    print(query)
    line = query.split('. ')
    entities = set()
    for l in line:
        if l == ' ':
            continue
        triples = l.split(';')
        for triple in triples:
            triple = tuple(triple.split())
            # print(triple)
            if len(triple) == 3:
                e1, pred, e2 = triple
                if 'res:' in e1 or 'dbr:'in e1:
                    entities.add(e1)
                if 'res:' in e2 or 'dbr:'in e2:
                    entities.add(e2)
            elif len(triple) == 2:
                pred, e = triple
                if 'res:' in e or 'dbr:'in e:
                    entities.add(e)
            else:
                continue
    if len(entities) == 2:
        qf.write(q+'\n')
        qf.write(query+'\n')
        qf.write('\n')
            # if pred not in relations and "type" not in pred:
            #     relations.append(pred)
