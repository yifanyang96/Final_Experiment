from stanfordcorenlp import StanfordCoreNLP
from nltk.tree import ParentedTree
from nltk.corpus import stopwords
from gensim.models import Word2Vec, KeyedVectors
import numpy as np
from SPARQLWrapper import SPARQLWrapper,JSON
from sklearn.preprocessing import normalize
from nltk.stem import WordNetLemmatizer
# from GstoreConnector import GstoreConnector
from hierarchy import Hierarchy
from PyDictionary import PyDictionary
import string
import ast
import json

nlp = StanfordCoreNLP('http://localhost', port=9000)
sparql = SPARQLWrapper("https://dbpedia.org/sparql")
# sparql = GstoreConnector("dbpedia.gstore-pku.com", 80, "endpoint", "123")
specificStopWords = {'Give', 'Show', 'List', 'Name', 'Tell', 'many', 'much', 'all', 'whose', 'often', 'also', 'Also', 'count', 'Count', 'Find', 'amongst', 'Amongst', 'among', 'Among', 'would', 'named', 'number'}
whwords = {'How', 'Which', 'Who', 'Whose', 'Whom', 'When', 'Where', 'What', 'Give', 'Show', 'List', 'Name', 'Tell', 'Count', 'Find', 'what'}
yesnowords = {'is', 'are', 'were', 'was', 'been', 'do', 'does', 'did', 'has', 'have', 'had'}
types = [(t.strip().upper(),t.strip()) for t in open('hierarchy.txt').readlines()]
misstypes = [tuple(t.strip().split(',')) for t in open('./misstypes').readlines()]
typedict = dict(types+misstypes)
misstypedict = dict(misstypes)
del typedict['TYPE']
del typedict['AREA']
timetypes = {'Time','Year'}
literaltypes = {'Population', 'Name'}
timexmltypes = {'duration', 'dateTime', 'time', 'date', 'gYearMonth', 'gYear', 'gMonthDay', 'gDay', 'gMonth'}

model = KeyedVectors.load_word2vec_format('~/word2vec/GoogleNews-vectors-negative300.bin',binary=True)
lemmatizer = WordNetLemmatizer()
table = dict.fromkeys(string.punctuation)
del table['<']
del table['>']
table = str.maketrans(table)
newstopwords = {line.strip() for line in open('stopwords').readlines()}
otherwords = {'<Entity>', '<E1>', '<E2>', 'someone', 'everyone'}
totalstopwords = newstopwords | otherwords | specificStopWords
bigtotalstopwords = set(stopwords.words('english')) | otherwords | specificStopWords

d = PyDictionary()

epsilon = 0.0001

neighborhashes = {}

def splitType(pre):
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
    line = line[0].upper() + line[1:]
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
            # for l in range(j, k+1):
            #     wl = lineSplit[l]
            #     cat += lemmatizer.lemmatize(wl.lower(), 'n').upper()
            #     catl.append(wl)
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
            currstl = currstl[:wid] + splitType(linetypes[w]) + currstl[wid+1:]
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
    line, qt, linetypes = findQType(line)
    print(line, linetypes)
    ptree = ParentedTree.fromstring(nlp.parse(line))
    entitySubTrees = []
    for subtree in ptree.subtrees():
        stl = subtree.leaves()
        if '<Entity>' in stl:
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
    return final, answerp, set(linetypes.values())

def singleE(line, entity, h):
    line = line.replace(entity, '<Entity>')
    return singleEMain(line, h)

def doubleE(line, entity, h):
    qline = line.replace(entity[0].split('|')[0], '<E1>', 1).replace(entity[1].split('|')[0], '<E2>').replace('&', 'and')
    assert '<E1>' in qline and '<E2>' in qline
    print(qline)
    line = qline.translate(table).split()
    if line[0].lower() in yesnowords:
        newline = qline[qline.index('<E1>')+4:].replace('<E2>', '<Entity>')
        final, answerp = singleEMain(newline, h)
        resource2p = len(final) - 1
    elif ' and ' in qline:
        E2R = line[line.index('and'):line.index('<E2>')]
        E2Rsub = [r for r in E2R if r not in specificStopWords and r.lower() not in stopwords.words('english')]
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
    return final, answerp, resource2p

def lem(word):
    if word in model.vocab:
        return word
    else:
        vlem = lemmatizer.lemmatize(word, 'v')
        if vlem != word and vlem in model.vocab:
            return vlem
        nlem = lemmatizer.lemmatize(word, 'n')
        if nlem != word and nlem in model.vocab:
            return nlem
        alem = lemmatizer.lemmatize(word, 'a')
        if alem != word and alem in model.vocab:
            return alem
        return ''

def TA(simVecL, adjMats, init=0):
    if len(simVecL) == 2:
        rightLinkIds = np.array([j for j in range(adjMats[0].shape[1]) if sum(adjMats[0][:,j])!=0])
        newSimVecL = [simVecL[0], simVecL[1][rightLinkIds]]
        minLen = min(map(len, newSimVecL))
        sortIds = np.array(list(map(lambda x:x[:minLen], map(lambda x:x[::-1], map(np.argsort, newSimVecL)))))
        sortIds[1] = rightLinkIds[sortIds[1]]
        sortIds = np.transpose(sortIds)
        maxSim = 0
        result = None
        for k in sortIds:
            leftSim = simVecL[0][k[0]]
            rightSim = simVecL[1][k[1]]
            threshold = init + leftSim + rightSim
            if threshold <= maxSim:
                return (maxSim, *result)
            rightIds = [i for i,j in enumerate(adjMats[0][:,k[1]]) if j == 1]
            # if len(rightIds) == 0:
            #     continue
            rightSims = simVecL[0][rightIds]
            rightMaxSim = init + rightSim + max(rightSims)
            rightMaxId = (rightIds[np.argmax(rightSims)], k[1])
            leftIds = [i for i,j in enumerate(adjMats[0][k[0]]) if j == 1]
            leftSims = simVecL[1][leftIds]
            leftMaxSim = init + leftSim + max(leftSims)
            leftMaxId = (k[0], leftIds[np.argmax(leftSims)])
            if leftMaxSim > rightMaxSim and leftMaxSim > maxSim:
                maxSim = leftMaxSim
                result = leftMaxId
            elif leftMaxSim <= rightMaxSim and rightMaxSim > maxSim:
                maxSim = rightMaxSim
                result = rightMaxId
    else:
        middleLinkIds = np.array([j for j in range(adjMats[0].shape[1]) if sum(adjMats[0][:,j])!=0 and sum(adjMats[1][j])!=0])
        rightLinkIds = np.array([j for j in range(adjMats[1].shape[1]) if sum(adjMats[1][:,j])!=0])
        newSimVecL = [simVecL[0], simVecL[1][middleLinkIds], simVecL[2][rightLinkIds]]
        minLen = min(map(len, newSimVecL))
        sortIds = np.array(list(map(lambda x:x[:minLen], map(lambda x:x[::-1], map(np.argsort, newSimVecL)))))
        sortIds[1] = middleLinkIds[sortIds[1]]
        sortIds[2] = rightLinkIds[sortIds[2]]
        sortIds = np.transpose(sortIds)   
        maxSim = 0
        result = None     
        for k in sortIds:
            leftSim = simVecL[0][k[0]]
            middleSim = simVecL[1][k[1]]
            rightSim = simVecL[2][k[2]]
            threshold = leftSim + middleSim + rightSim
            if threshold <= maxSim:
                return (maxSim, *result)
            leftMiddleIds = [i for i,j in enumerate(adjMats[0][k[0]]) if j == 1]
            leftMat = adjMats[1][leftMiddleIds]
            leftMiddleSims = simVecL[1][leftMiddleIds]
            leftMaxSim, leftMaxMiddleId, leftMaxRightId = TA([leftMiddleSims, simVecL[2]], [leftMat], leftSim)
            leftMaxId = (k[0], leftMiddleIds[leftMaxMiddleId], leftMaxRightId)
            middleLeftIds = [i for i,j in enumerate(adjMats[0][:,k[1]]) if j == 1]
            middleRightIds = [i for i,j in enumerate(adjMats[1][k[1]]) if j == 1]
            middleMat = np.ones((len(middleLeftIds), len(middleRightIds)))
            middleLeftSims = simVecL[0][middleLeftIds]
            middleRightSims = simVecL[2][middleRightIds]
            middleMaxSim, middleMaxLeftId, middleMaxRightId = TA([middleLeftSims, middleRightSims], [middleMat], middleSim)
            middleMaxId = (middleLeftIds[middleMaxLeftId], k[1], middleRightIds[middleMaxRightId])
            rightMiddleIds = [i for i,j in enumerate(adjMats[1][:,k[2]]) if j == 1]
            rightMat = np.transpose(adjMats[0])[rightMiddleIds]
            rightMiddleSims = simVecL[1][rightMiddleIds]
            rightMaxSim, rightMaxMiddleId, rightMaxLeftId = TA([rightMiddleSims, simVecL[0]], [rightMat], rightSim)
            rightMaxId = (rightMaxLeftId, rightMiddleIds[rightMaxMiddleId], k[2])
            currMaxSims = (leftMaxSim, middleMaxSim, rightMaxSim)
            currMaxSim = max(currMaxSims)
            if currMaxSim > maxSim:
                maxSim = currMaxSim
                currMaxIds = (leftMaxId, middleMaxId, rightMaxId)
                result = currMaxIds[currMaxSims.index(currMaxSim)]

    return (maxSim, *result)

def splitPre(pre):
    ind = 0
    preList = set()
    for i in range(len(pre)):
        if pre[i].isupper():
            if i - ind>1:
                word = lem(pre[ind:i].lower())
                if word != '':
                    preList.add(word)
                ind = i
    if len(preList)==0:
        word = lem(pre.lower())
        if word != '':
            preList.add(word)
    else:
        word = lem(pre[ind:].lower())
        if word != '':
            preList.add(word)
    return preList

def generatePaths(path):
    lineSplit = set()
    for x in path:
        x = x.translate(table)
        if x!='' and x not in lineSplit and x not in totalstopwords and x.lower() not in totalstopwords:
            word = lem(x.lower())
            if word != '':
                lineSplit.add(word)
    return lineSplit


def generateSPARQL(pline, resource, resource2p, resource2=''):
    qstart = 'SELECT DISTINCT '
    qmiddle1 = 'WHERE {'
    qmiddle2 = '} UNION {'
    qend2 = '}. FILTER (isLiteral('
    prevE = resource
    query = qmiddle1
    qstart1 = qstart
    pathL = []
    typeL = []
    flag = False
    for pi in range(len(pline)):
        pv = '?p' + str(pi)
        if pi == resource2p:
            if resource2 != '':
                ev = resource2
                flag = True
            else:
                ev = '?x' + str(pi)
        else:
            ev = '?x' + str(pi)
        path, answerT = pline[pi]
        newpath = generatePaths(path)
        if len(newpath) == 0:
            if len(pathL) != 0:
                pathL.append(pathL[-1])
            else:
                pathL.append([lem(p) for p in path if lem(p) != ''])
        else:
            pathL.append(newpath)
        typeL.append(answerT)
        qstart1 += pv + ' '
        query += '{' + prevE + ' ' + pv + ' ' + ev + qmiddle2 + ev + ' ' + pv + ' ' + prevE
        if flag:
            query += '}.'
            flag = False
        else:
            if answerT in literaltypes or answerT in timetypes:
                query += qend2 + ev + ')).'
            elif answerT == '':
                query += '}.'
            else:
                query += '}.' + ev + ' a <http://dbpedia.org/ontology/' + answerT + '>.'
            prevE = ev
    query = qstart1 + query + '}'
    print(query)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
    return results, pathL, typeL

def generatePreds(results, plen):
    predL = [[] for i in range(plen)]
    predSplitL = [[] for i in range(plen)]
    idL = [[] for i in range(1, plen)]
    # print(len(results['results']['bindings']))
    for t in results['results']['bindings']:
        flag = False
        currPreds = []
        currPredSplits = []
        for i in range(plen):
            currPred = t['p'+str(i)]['value']
            if 'dbpedia.org' not in currPred or 'wikiPage' in currPred:
                flag = True
                break
            currPredSplit = splitPre(currPred.split('/')[-1].split('#')[-1])
            if len(currPredSplit) == 0:
                flag = True
                break
            currPreds.append(currPred)
            currPredSplits.append(currPredSplit)
        if flag:
            continue
        prevId = -1
        flag = False
        for i in range(plen):
            if currPreds[i] not in predL[i]:
                if prevId != -1:
                    idL[i-1].append((prevId, len(predL[i])))
                prevId = len(predL[i])
                predL[i].append(currPreds[i])
                predSplitL[i].append(currPredSplits[i])
                # pvsL[i].append(splitVec(splitPre(currPreds[i].split('/')[-1].split('#')[-1])))
            else:
                currId = (prevId, predL[i].index(currPreds[i]))
                if prevId != -1 and currId not in idL[i-1]:
                    idL[i-1].append(currId)
                prevId = currId[1]
    return predL, predSplitL, idL

def removeType(pline, h):
    maxD = -1
    maxId = -1
    for pi in range(len(pline)):
        pt = pline[pi][1]
        if pt != '':
            depth = h.getDepth(pt)
            if depth > maxD:
                maxD = depth
                maxId = pi
    assert maxId != -1
    pline[maxId] = (pline[maxId][0],'')
    return pline

def dicReplace(path, linetypes):
    newpath = {for w in path if w not in bigtotalstopwords and w not in linetypes}


def dicReplaceSimVec(lineSplit, predSplit, linetypes):


def computeSimVecNew(lineSplit, predSplit, linetypes):
    print(lineSplit)
    print(predSplit)
    lvs = np.transpose(normalize(np.stack([model[ls] for ls in lineSplit])))
    final = np.array([np.average(np.max(np.dot(normalize(np.stack([model[ps] for ps in pred])), lvs), axis=1))*(1+(len(pred)-1)*epsilon) for pred in predSplit])
    if max(final) < 0.3:
        final = dicReplaceSimVec(lineSplit, predSplit, linetypes)
    return final

def generateAnswerSPARQL(resource, typeL, finalPreds):
    qstart = 'SELECT DISTINCT '
    qmiddle1 = 'FROM <http://dbpedia.org> WHERE {'
    qmiddle2 = '} UNION {'
    qend2 = '}. FILTER (isLiteral('
    query = qstart + '?x' + str(answerp) + ' ' + qmiddle1
    prevE = resource
    for pi in range(len(typeL)):
        answerT = typeL[pi]
        pv = '<' + finalPreds[pi] + '>'
        ev = '?x' + str(pi)
        query += '{' + prevE + ' ' + pv + ' ' + ev + qmiddle2 + ev + ' ' + pv + ' ' + prevE
        if answerT in literaltypes or answerT in timetypes:
            query += qend2 + ev + ')).'
        elif answerT == '':
            query += '}.'
        else:
            query += '}.' + ev + ' a <http://dbpedia.org/ontology/' + answerT + '>.'
        prevE = ev
    query += '}'
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
    return results

def singleEMultiRQuick(qline, eline, pline, rline, sline, answerp, h, af):
    sf, resource = tuple(eline.split('|'))
    pline, answerp, linetypes = singleE(qline, sf, h)
    linetypes = {w for w in splitType(t) for t in linetypes}
    qline = qline.replace(sf, '')
    results, pathL, typeL = generateSPARQL(pline, resource, len(pline))
    if len(pline) != 1:
        predL, predSplitL, idL = generatePreds(results)
        while len(predL[0]) == 0:
            pline = removeType(pline, h)
            results, pathL, typeL = generateSPARQL(pline, resource, len(pline))
            predL, predSplitL, idL = generatePreds(results, len(pline))
        simVecL = [computeSimVecNew(pathL[i], predSplitL[i], linetypes) for i in range(len(pline))]
        adjMats = [np.zeros((len(predL[i]), len(predL[i+1]))) for i in range(len(pline)-1)]
        # print(predL)
        for i in range(len(pline)-1):
            for index in idL[i]:
                adjMats[i][index] = 1
        # print(adjMats)
        result = TA(simVecL, adjMats)
        maxSim = result[0]
        result = result[1:]
        finalPreds = [predL[i][j] for i,j in enumerate(result)]
    else:
        currPreds = []
        currPredSplits = []
        for t in results['results']['bindings']:
            currPred = t['p0']['value']
            currPredSplit = splitPre(currPred.split('/')[-1].split('#')[-1])
            if 'dbpedia.org' in currPred and 'wikiPage' not in currPred and len(currPredSplit) != 0:
                currPreds.append(currPred)
                currPredSplits.append(currPredSplit)
        while len(currPreds) == 0:
            pline = [(pline[0][0], '')]
            results, pathL, typeL = generateSPARQL(pline, resource, len(pline))
            for t in results['results']['bindings']:
                currPred = t['p0']['value']
                currPredSplit = splitPre(currPred.split('/')[-1].split('#')[-1])
                if 'dbpedia.org' in currPred and 'wikiPage' not in currPred and len(currPredSplit) != 0:
                    currPreds.append(currPred)
                    currPredSplits.append(currPredSplit)
        simMat = computeSimVecNew(pathL[0],currPredSplits)
        maxSingleInd = np.argmax(simMat)
        maxSim = simMat[maxSingleInd]
        finalPreds = [currPreds[maxSingleInd]]
    print('MaxSim:', maxSim)
    af.write('MaxSim: ' + str(maxSim) + '\n')
    print('FinalPreds:', finalPreds)
    af.write('FinalPreds: ' + str(finalPreds) + '\n')
    print('TruePreds:', rline)
    af.write('TruePreds: ' + str(rline) + '\n')
    # af.write('FinalAnswers:\n')
    # results = generateAnswerSPARQL(resource, typeL, finalPreds)
    # finalAnswers = set()
    # for t in results['results']['bindings']:
    #     currAnswer = t['x'+str(answerp)]['value']
    #     finalAnswers.add(currAnswer)
    #     af.write(currAnswer+'\n')
    # sparql.setQuery(query)
    # sparql.setReturnFormat(JSON)
    # results = sparql.query().convert()


def doubleEMultiR(qline, eline, pline, rline, answerp, resource2p, h, af):
    e1, e2 = tuple(eline)
    try:
        if len(pline) == 1:
            singleEMultiRQuick(qline, e2, pline, rline, sline, answerp, h, af)
        else:
            sf1, resource = tuple(e1.split('|'))
            sf2, resource2 = tuple(e2.split('|'))
            qline = qline.replace(sf1, '').replace(sf2, '')
            results, pathL, typeL = generateSPARQL(pline, resource, resource2p, resource2)
            predL, predSplitL, idL = generatePreds(results)
            while len(predL[0]) == 0:
                pline = removeType(pline, h)
                results, pathL, typeL = generateSPARQL(pline, resource, resource2p, resource2)
                predL, predSplitL, idL = generatePreds(results, len(pline))
            simVecL = [computeSimVecNew(pathL[i], predSplitL[i]) for i in range(len(pline))]
            adjMats = [np.zeros((len(predL[i]), len(predL[i+1]))) for i in range(len(pline)-1)]
            # print(predL)
            for i in range(len(pline)-1):
                for index in idL[i]:
                    adjMats[i][index] = 1
            # print(adjMats)
            result = TA(simVecL, adjMats)
            maxSim = result[0]
            result = result[1:]
            finalPreds = [predL[i][j] for i,j in enumerate(result)]
            print('MaxSim:', maxSim)
            af.write('MaxSim: ' + str(maxSim) + '\n')
            print('FinalPreds:', finalPreds)
            af.write('FinalPreds: ' + str(finalPreds) + '\n')
            print('TruePreds:', rline)
            af.write('TruePreds: ' + str(rline) + '\n')
    except:
        print('ERROR')
        af.write('ERROR\n')


qf = open('LCQ_SQ')
qlines = qf.readlines()
ef = open('LCQ_SE')
elines = ef.readlines()
pf = open('LCQ_SP')
plines = pf.readlines()
rf = open('LCQ_SR')
rlines = rf.readlines()
sf = open('LCQ_SS')
slines = sf.readlines()
h = Hierarchy('./hierarchy.txt')

# qline = qlines[38].strip()
# eline = elines[38].strip()
# pline, answerp = tuple(plines[38].strip().split('|'))
# pline = ast.literal_eval(pline)
# rline = rlines[38].strip()
# answerp = int(answerp)
# singleEMultiRQuick(qline, eline, pline, rline, answerp)
af = open('LCQ_SA', 'w')

for i in range(len(qlines)):
    print(i)
    qline = qlines[i].strip()
    eline = elines[i].strip()
    # eline = eline.split(';')
    # pline, answerp, resource2p = doubleE(qline, eline, h)
    pline, answerp = tuple(plines[i].strip().split('|'))
    pline = ast.literal_eval(pline)
    answerp = int(answerp)
    rline = rlines[i].strip()
    sline = slines[i].strip()
    af.write(str(i)+'\n')
    # doubleEMultiR(qline, eline, pline, rline, answerp, resource2p, h, af)
    singleEMultiRQuick(qline, eline, pline, rline, sline, answerp, h, af)