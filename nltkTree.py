from stanfordcorenlp import StanfordCoreNLP
from nltk.tree import ParentedTree
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from hierarchy import Hierarchy
import ast
import string
from bert_serving.client import BertClient

bc = BertClient()
nlp = StanfordCoreNLP('http://localhost', port=9000)
lemmatizer = WordNetLemmatizer()
specificStopWords = {'Give', 'Show', 'List', 'Name', 'Tell', 'many', 'much', 'all', 'whose', 'often', 'also', 'Also', 'Count', 'Find', 'amongst', 'Amongst', 'among', 'Among', 'would', 'named', 'count'}
whwords = {'How', 'Which', 'Who', 'Whose', 'Whom', 'When', 'Where', 'What', 'Give', 'Show', 'List', 'Name', 'Tell', 'Count', 'Find', 'what'}
newstopwords = [line.strip() for line in open('stopwords').readlines()]
yesnowords = {'is', 'are', 'were', 'was', 'been', 'do', 'does', 'did', 'has', 'have', 'had'}
types = [(t.strip().upper(),t.strip()) for t in open('hierarchy.txt').readlines()]
misstypes = [tuple(t.strip().split(',')) for t in open('./misstypes').readlines()]
typedict = dict(types+misstypes)
misstypedict = dict(misstypes)
del typedict['TYPE']
del typedict['AREA']
table = dict.fromkeys(string.punctuation)
del table['<']
del table['>']
table = str.maketrans(table)

def splitPre(pre):
    ind = 0
    preList = []
    for i in range(len(pre)):
        if pre[i].isupper() and i - ind > 1:
            preList.append(pre[ind:i].lower())
            ind = i
    if len(preList)==0:
        preList.append(pre.lower())
    else:
        preList.append(pre[ind:].lower())
    return preList

def findPOS(ptree, w):
    w = w.translate(table)
    if w in ptree.leaves():
        return ptree[ptree.leaf_treeposition(ptree.leaves().index(w))[:-1]].label()[0]
    else:
        return 'P'  

def findQType(line):
    line = line[0].upper() + line[1:-1] + ' ' + line[-1]
    lineSplitOrig = line.split()
    if lineSplitOrig[1] == 'which':
        lineSplitOrig[1] = 'Which'
    elif lineSplitOrig[1] == 'what':
        lineSplitOrig[1] = 'What'
    line = ' '.join(lineSplitOrig)
    ptree = ParentedTree.fromstring(nlp.parse(line))
    firstw = lineSplitOrig[0].lower()
    if firstw == 'who' or firstw == 'whom' or firstw == 'whose':
        qt = 'Person'
    elif firstw == 'when':
        qt = 'Time'
    elif firstw == 'where':
        qt = 'Place'
    else:
        qt = ''
    lineSplit = [x for x in lineSplitOrig if x.lower() not in newstopwords]
    linetypes = {}
    j = 0
    while j < len(lineSplit):
        for k in range(len(lineSplit), j, -1):
            catl = lineSplit[j:k]
            cat = ''.join(map(lambda x:lemmatizer.lemmatize(x.translate(table).lower(), 'n').upper(), catl))
            if cat in typedict:
                word = ' '.join(catl)
                currtype = typedict[cat]
                if word in linetypes or\
                    len(catl) == 1 and (word == 'place' and lineSplitOrig[lineSplitOrig.index(word)-1] in ['take', 'takes', 'took']\
                        or findPOS(ptree, word) != 'N'\
                        or lineSplit.index(word) != len(lineSplit) - 1 and findPOS(ptree, lineSplitOrig[lineSplitOrig.index(word)+1]) == 'N'):
                    continue
                if len(catl) > 1:
                    linetypes[currtype.lower()] = currtype
                    line = line.replace(word, currtype.lower())
                else:
                    linetypes[word.translate(table)] = currtype
                j = k - 1
                break
        j += 1
    return line, qt, linetypes

def findType(path, linetypes):
    for r in path:
        if r.translate(table) in linetypes:
            return linetypes[r]
    if ('ones' in path or 'one' in path) and ('who' in path or 'whose' in path)\
        or 'someone' in path or 'everyone' in path:
        return 'Person'
    return ''

def findName(final):
    nps = []
    for i in range(len(final)):
        if final[i][1] == 'Name':
            nps.append(i-len(nps))
    return nps

def mergeType(final):
    tps = []
    for i in range(len(final)-1):
        if final[i+1][1] != '' and final[i][1] == '':
            tps.append(i-len(tps))
    return tps

def filterFinal(final, ptree, answerp):
    if final[0][0][-1] == '<Entity>' or final[0][0][-1] == '<E1>' or final[0][0][-1] == '<E2>':
        if findPOS(ptree, final[0][0][-2]) == 'N':
            final = final[1:]
        elif final[0][1] != '' and final[0][0][-2] in ['is', 'was', 'are', 'were', 'as', 'named']:
            final[0] = (final[0][0], '')
    if final[-1][0][0] == 'How' and final[-1][0][1] != 'many' and final[-1][0][1] != 'much':
        if answerp == len(final) - 1:
            answerp -= 1
        final.pop()
    nps = findName(final)
    for np in nps:
        if answerp >= np:
            answerp -= 1
        del final[np]
    tps = mergeType(final)
    for tp in tps:
        # final[tp] = (final[tp][0], final[tp+1][1])
        if answerp >= tp:
            answerp -= 1
        del final[tp]
    return final, answerp

def replaceType(stl, linetypes):
    currstl = stl
    for w in stl:
        if w in linetypes:
            wid = currstl.index(w)
            currstl = currstl[:wid] + splitPre(linetypes[w]) + currstl[wid+1:]
    return currstl

def remove(final, ptree, linetypes):
    if len(final) == 0:
        return []
    prevstl, prevtype = final[0]
    newfinal = [(replaceType(prevstl, linetypes), prevtype)]
    prevstl = (prevstl, '')
    for currstl, currtype in final[1:]:
        currpath = (' '.join(currstl)).replace(' '.join(prevstl[0]), '').split()
        if ('Which' in currpath or 'What' in currpath)\
            and (currpath[-1] in yesnowords and (currpath[-2] in yesnowords or findPOS(ptree, currpath[-2])=='N') or findPOS(ptree, currpath[-1])=='N')\
            and prevtype=='':
            newfinal[-1] = (replaceType((' '.join(currstl)).replace(' '.join(prevstl[1]), '').split(), linetypes), currtype)
        elif 'How' in currpath\
            and (currpath[-1] in yesnowords and (currpath[-2] in yesnowords or findPOS(ptree, currpath[-2])=='N') or findPOS(ptree, currpath[-1])=='N')\
            and (prevtype== '' or currtype == ''):
            newfinal[-1] = (replaceType((' '.join(currstl)).replace(' '.join(prevstl[1]), '').split(), linetypes), currtype if prevtype=='' else prevtype)
        elif 'total' in currpath and 'number' in currpath:
            newfinal[-1] = (replaceType((' '.join(currstl)).replace(' '.join(prevstl[1]), '').split(), linetypes), prevtype)
        else:
            newfinal.append((replaceType(currpath, linetypes), currtype))
        prevstl = (currstl, prevstl[0])
        prevtype = currtype
    return newfinal


def singleEMain(line, h):
    tokens = bc.encode([line], show_tokens=True)[1][0]
    line, qt, linetypes = findQType(line)
    print(line, linetypes)
    ptree = ParentedTree.fromstring(nlp.parse(line))
    entitySubTrees = []
    for subtree in ptree.subtrees():
        stl = subtree.leaves()
        if 'Entity' in stl:
            stl = [l for l in stl if l.translate(table) != '' and l != '\'s']
            if len(entitySubTrees) == 0 or len(stl) != len(entitySubTrees[-1]):
                entitySubTrees.append(stl)
    print(entitySubTrees)
    final = []
    prevstl = entitySubTrees[-1]
    answerp = -1
    for i in range(len(entitySubTrees)-2,-1,-1):
        currstl = entitySubTrees[i]
        path = (' '.join(currstl)).replace(' '.join(prevstl), '').split()
        relations = [p for p in path if p.lower() not in stopwords.words('english') and p not in specificStopWords]
        if len(relations) == 0:
            continue
        currtype = findType(path, linetypes)
        final.append((currstl, currtype))
        if answerp == -1 and len(set(path) & whwords) != 0:
            answerp = len(final) - 1
        prevstl = currstl
    if len(final) == 0:
        final = [(entitySubTrees[0],'')]
        answerp = 0
    elif len(final) > 1:
        final, answerp = filterFinal(final, ptree, answerp)
    else:
        answerp = 0
    if qt == 'Time' or (qt != '' and not h.isChild(final[-1][1], qt)):
        final[-1] = (final[-1][0], qt)
    final = remove(final, ptree, linetypes)
    return final, answerp

def singleE(line, entity, pf, h):
    line = line.replace(entity, 'Entity')
    final, answerp = singleEMain(line, h)
    pf.write(str(final)+'|'+str(answerp)+'\n')

def doubleE(line, entity, pf, h):
    qline = line.replace(entity[0].split('|')[0], '<E1>', 1).replace(entity[1].split('|')[0], '<E2>').replace('&', 'and')
    assert '<E1>' in qline and '<E2>' in qline
    print(qline)
    line = qline.translate(table).split()
    if line[0].lower() in yesnowords:
        newline = qline[qline.index('<E1>')+4:].replace('<E2>', '<Entity>')
        final, answerp = singleEMain(newline, h)
    elif ' and ' in qline:
        E2R = line[line.index('and'):line.index('<E2>')]
        E2Rsub = [r for r in E2R if r.lower() not in specificStopWords and r.lower() not in stopwords.words('english')]
        if len(E2Rsub) == 0:
            newline = qline.replace(qline[qline.index('<E1>'):qline.index('<E2>')+4], '<Entity>')
            final, answerp = singleEMain(newline, h)
            if len(final) == 1:
                final = [final[0], (final[0][0],'')]
            else:
                final = [final[0], (final[0][0],'')] + final[1:]
            answerp += 1
            resource2p = 1
        else:
            r1 = qline[:qline.index(' and ')].replace('<E1>', '<Entity>')
            final1, _ = singleEMain(r1, h)
            r2 = qline[qline.index(' and ')+5:].replace('<E2>', '<Entity>')
            final2, _ = singleEMain(r2, h)
            final1[-1] = (final1[-1][0], h.findParent(final1[-1][1], final2[-1][1]))
            final2[-1] = (final2[-1][0], '')
            final = final1 + final2[::-1]
            answerp = len(final1) - 1
            resource2p = len(final) - 1
    elif 'which' in line and line.index('which') != 1 and '<E1>' in qline[:qline.index('which')] and '<E2>' in qline[qline.index('which'):]:
        r1 = qline[:qline.index('which')].replace('<E1>', '<Entity>')
        final1, _ = singleEMain(r1, h)
        r2 = qline[qline.index('which')+6:].replace('<E2>', '<Entity>')
        final2, _ = singleEMain(r2, h)
        final1[-1] = (final1[-1][0], h.findParent(final1[-1][1], final2[-1][1]))
        final2[-1] = (final2[-1][0], '')
        final = final1 + final2[::-1]
        answerp = len(final1) - 1
        resource2p = len(final) - 1
    else:
        line, qt, linetypes = findQType(qline)
        ptree = ParentedTree.fromstring(nlp.parse(line))
        entitySubTrees = []
        for subtree in ptree.subtrees():
            stl = subtree.leaves()
            if '<E1>' in stl or '<E2>' in stl:
                stl = [l for l in stl if l.translate(table) != '' and l != '\'s']
                if len(entitySubTrees) == 0 or len(stl) != len(entitySubTrees[-1]):
                    entitySubTrees.append(stl)
        print(entitySubTrees)
        final1 = []
        final2 = []
        for i in range(len(entitySubTrees)-1,-1,-1):
            currstl = entitySubTrees[i]
            if '<E1>' in currstl and '<E2>' in currstl:
                if len(final2) != 0 or len(final1) != 0:
                    break
                else:
                    biggerstl = entitySubTrees[1]
                    # sub = [stl for stl in biggerstl if stl not in currstl]
                    final1 = [(biggerstl,findType(biggerstl,linetypes))]
                    break
            if currstl == ['<E2>']:
                final = final2
                prevstl = currstl
                continue
            elif currstl == ['<E1>']:
                final = final1
                prevstl = currstl
                continue
            path = [p for p in currstl if p not in prevstl]
            relations = [p for p in path if p.lower() not in stopwords.words('english') and p not in specificStopWords]
            if len(relations) != 0:
                currtype = findType(relations, linetypes)
                final.append((currstl,currtype))
            prevstl = currstl
        if len(final1) > 1:
            final1, _ = filterFinal(final1, ptree, len(final1)-1)
        final1 = remove(final1, ptree, linetypes)
        if len(final2) > 1:
            final2, _ = filterFinal(final2, ptree, len(final2)-1)
        final2 = remove(final2, ptree, linetypes)
        if len(final1) != 0 and len(final2) != 0:
            final1[-1] = (final1[-1][0], h.findParent(qt, h.findParent(final1[-1][1], final2[-1][1])))
            final2[-1] = (final2[-1][0], '')
        if len(final1) == 0:
            answerp = 0
        else:
            answerp = len(final1) - 1
        final = final1 + final2[::-1]
        resource2p = len(final) - 1
    print(final, answerp)
    pf.write(str(final)+'|'+str(answerp)+'\n')

# qf = open('qald_CompleteQ')
# qlines = qf.readlines()
# ef = open('qald_CompleteE')
# elines = ef.readlines()
# pf = open('qald_CompleteP', 'w')
# qf = open('Question')
# qlines = qf.readlines()
# ef = open('Entity')
# elines = ef.readlines()
# pf = open('Path', 'w')
qf = open('LCQ_DQ')
qlines = qf.readlines()
ef = open('LCQ_DE2')
elines = ef.readlines()
pf = open('LCQ_DP', 'w')
h = Hierarchy('./hierarchy.txt')
for id in range(len(qlines)):
    if ';' in elines[id]:
        doubleE(qlines[id].strip(), elines[id].split(';'), pf, h)
    else:
        singleE(qlines[id].strip(), elines[id].split('|')[0], pf, h)
# singleE(qlines[24].strip(), elines[24].split('|')[0], pf, h)