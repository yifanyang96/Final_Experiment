table = {'http://dbpedia.org/property/':'dbp:', 'http://dbpedia.org/ontology/':'dbo:', 'http://dbpedia.org/resource/':'dbr:', 'https://www.w3.org/1999/02/22-rdf-syntax-ns#type':'rdf:type'}
sf = open('LCQ_DS')
slines = sf.readlines()
ef = open('LCQ_DE2')
elines = ef.readlines()
rf = open('LCQ_DR', 'w')
for i in range(len(slines)):
    sline = slines[i].strip()
    eline = elines[i].strip()
    e1, e2 = tuple(map(lambda x:x.split('|')[1], eline.split(';')))
    sline = sline.strip()
    sline = sline[sline.index('{')+1:sline.index('}')].strip()
    if sline.index(e1) < sline.index(e2):
        sline = sline.split('. ')
    else:
        sline = sline.split('. ')[::-1]    
    print(sline)
    for t in sline:
        if len(t) != 0:
            for k, v in table.items():
                t = t.replace(k, v)
            r = t.split()[1]
            if 'rdf:type' not in r:
                rf.write(r+' ')
    rf.write('\n')