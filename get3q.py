table = {'http://dbpedia.org/property/':'dbp:', 'http://dbpedia.org/ontology/':'dbo:', 'http://dbpedia.org/resource/':'dbr:'}
rdft = 'https://www.w3.org/1999/02/22-rdf-syntax-ns#type'
sf = open('LCQ_CDS')
slines = sf.readlines()
ef = open('LCQ_CDE')
elines = ef.readlines()
qf = open('LCQ_CDQ')
qlines = qf.readlines()
s3f = open('LCQ_D3S', 'w')
e3f = open('LCQ_D3E', 'w')
q3f = open('LCQ_D3Q', 'w')
r3f = open('LCQ_D3R', 'w')
for i in range(len(slines)):
    sline = slines[i].strip()
    eline = elines[i].strip()
    e1, e2 = tuple(map(lambda x:x.split('|')[1], eline.split(';')))
    sq = sline[sline.index('{')+1:sline.index('}')].strip()
    if sq.index(e1) < sq.index(e2):
        sq = [s for s in sq.split('. ') if len(s.strip())!=0 and rdft not in s]
    else:
        sq = [s for s in sq.split('. ')[::-1] if len(s.strip())!=0 and rdft not in s]
    if len(sq) == 3:
        print(sline)
        s3f.write(sline+'\n')
        e3f.write(eline+'\n')
        q3f.write(qlines[i])
        for t in sq:
            for k, v in table.items():
                t = t.replace(k, v)
            r = t.split()[1]
            if 'rdf:type' not in r:
                r3f.write(r+' ')
        r3f.write('\n')