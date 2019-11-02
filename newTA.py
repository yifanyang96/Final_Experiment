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
from bisect import bisect_left, bisect_right
from queue import PriorityQueue as PQ
import heapq

model = KeyedVectors.load_word2vec_format('~/word2vec/GoogleNews-vectors-negative300.bin',binary=True)
lemmatizer = WordNetLemmatizer()
N = 10
K = 10
L = 300


epsilon = 0.0001
neighborhashes = np.zeros((N,2))
entubs = np.zeros((N,K))
entId = {}
spacemarks = np.zeros((K,L))

def Hash(e):
    return 0

def HN(ent1, ent2):
    u = Hash(ent2)
    return u >= neighborhashes[ent1][0] and u <= neighborhashes[ent1][1]

class NRAHeap:
    def __init__(self):
        self.entities = set()
        self.queue = []
    
    def push(self, entity, pred, sim, UB):
        if entity not in self.entities:
            heapq.heappush(self.queue, (UB, entity, pred, sim))
    
    def update(self, entity, sim):
        resultS = 0
        resultP = None
        for i in range(len(self.queue)):
            _, currE, currP, currS = self.queue[i]
            currUB = sim + currS
            if currE == entity:
                resultS = currUB
                resultP = currP
            self.queue[i] = (currUB, currE, currP, currS)
        return resultP, resultS

    def getMaxUB(self):
        return self.queue[-1][2]

class NRA3Heap(NRAHeap):
    def __init__(self):
        super().__init__()
    
    def updateWithNeighbor(self, entity, sim):
        result = []
        for i in range(len(self.queue)):
            _, currE, currP, currS = self.queue[i]
            currUB = sim + currS
            if HN(currE, entity):
                result.append(currP, currE, currS)
            self.queue[i] = (currUB, currE, currP, currS)
        return result
    
    def updateOnly(self, entity, sim):
        self.queue = [(sim+currS, currE, currP, currS) for _, currE, currP, currS in self.queue]
    
    def search(self, entity):
        for  _, currE, currP, currS in self.queue:
            if currE == entity:
                return currP, currS
        return None, 0

def getN(e):
    return np.zeros((8,10)), [], [], []

def getNT(t):
    return [], []

def getP(e):
    return []

def getR(e1, e2):
    return []

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

def generatePreds(results, plen):
    predL = [[] for i in range(plen)]
    predSplitL = [[] for i in range(plen)]
    idL = [[] for i in range(1, plen)]
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
            else:
                currId = (prevId, predL[i].index(currPreds[i]))
                if prevId != -1 and currId not in idL[i-1]:
                    idL[i-1].append(currId)
                prevId = currId[1]
    return predL, predSplitL, idL

def dicReplaceSimVec(lineSplit, predSplit, linetypes):
    return np.empty()

def computeSimVec(lineSplit, predSplit):
    print(lineSplit)
    print(predSplit)
    lvs = np.transpose(normalize(np.stack([model[ls] for ls in lineSplit])))
    final = np.array([np.average(np.max(np.dot(normalize(np.stack([model[ps] for ps in pred])), lvs), axis=1))*(1+(len(pred)-1)*epsilon) for pred in predSplit])
    return final

def computeSingleSimVec(p, R):
    currPredSplits = []
    for currPred in p:
        currPredSplit = splitPre(currPred.split('/')[-1].split('#')[-1])
        if 'dbpedia.org' in currPred and 'wikiPage' not in currPred and len(currPredSplit) != 0:
            currPredSplits.append(currPredSplit)
    return computeSimVec(R, currPredSplits)

def getMaxPE(adjMat, entId, simVec):
    predIds = np.where(adjMat[entId]!=0)[0]
    sims = simVec[predIds]
    maxSimId = np.argmax(sims)
    maxSim = sims[maxSimId]
    maxId = predIds[maxSimId]
    return maxId, maxSim

def filterPreds(adjMat, entIds, preds):
    adjMatR = adjMat[entIds]
    predIdsR = np.where(adjMatR.any(axis=0))[0]
    adjMatR = adjMatR[:,predIdsR]
    predsR = [preds[i] for i in predIdsR]
    return adjMatR, predsR

def filterEntsPreds(e, r):
    adjMat, preds, ents, hashes = getN(e)
    hashIdm, hashIdM = bisect_left(hashes, r[0]), bisect_right(hashes, r[1])
    entsR = ents[hashIdm:hashIdM]
    adjMatR, predsR = filterPreds(adjMat, range(hashIdm, hashIdM), preds)
    return adjMatR, predsR, entsR

def hasPath(e1, e2, R3):
    p = getR(e1, e2)
    if not p is None:
        simVec = computeSingleSimVec(p, R3)
        maxSingleInd = np.argmax(simVec)
        maxSingleSim = simVec[maxSingleInd]
        maxSinglePred = p[maxSingleInd]
        return maxSinglePred, maxSingleSim
    return None

def getMaxP(e, R):
    preds = getP(e)
    simVec = computeSingleSimVec(preds, R)
    maxId = np.argmax(simVec)
    maxSim = simVec[maxId]
    maxPred = preds[maxId]
    return maxSim, maxPred

def getCN(e, t):
    r = (max(neighborhashes[e][0], neighborhashes[t][0]),
         min(neighborhashes[e][1], neighborhashes[t][1]))
    adjMat1, preds1, ents1, hashes1 = getN(e)
    hashId1m, hashId1M = bisect_left(hashes1, r[0]), bisect_right(hashes1, r[1])
    entst, hashest = getNT(t)
    entstR = entst[bisect_left(hashest, r[0]):bisect_right(hashest, r[1])]
    entsR = []
    ent1Ids = []
    for ent1id in range(hashId1m, hashId1M):
        ent1 = ents1[ent1id]
        enttid = bisect_left(entstR, ent1)
        if entstR[enttid] == ent1:
            entsR.append(ent1)
            ent1Ids.append(ent1id)
    adjMatR, predsR = filterPreds(adjMat1, ent1Ids, preds1)
    return adjMatR, predsR, entsR

def getPreds(e, t, R):
    adjMat, preds, ents = getCN(e, t)
    simVec = computeSingleSimVec(preds, R)
    sortId = np.argsort(simVec)[::-1]
    return adjMat, preds, ents, simVec, sortId

def TAComputeMax(adjMat, currId, simVec, lr):
    if lr == 0:
        currAdjVec = adjMat[currId]
    else:
        currAdjVec = adjMat[:,currId]
    currIds = np.where(currAdjVec!=0)[0]
    currSims = simVec[currIds]
    currMaxSimId = np.argmax(currSims)
    return currMaxSimId, currSims[currMaxSimId]

def newTAPre(e1, R1, e2, R2, t):
    r = (max(neighborhashes[e1][0], neighborhashes[e2][0], neighborhashes[t][0]),
         min(neighborhashes[e1][1], neighborhashes[e2][1], neighborhashes[t][1]))
    adjMat1, preds1, ents1, hashes1 = getN(e1)
    hashId1m, hashId1M = bisect_left(hashes1, r[0]), bisect_right(hashes1, r[1])
    adjMat2, preds2, ents2, hashes2 = getN(e2)
    entst, hashest = getNT(t)
    ents2R = ents2[bisect_left(hashes2, r[0]):bisect_right(hashes2, r[1])]
    entstR = entst[bisect_left(hashest, r[0]):bisect_right(hashest, r[1])]
    entsR = []
    ent1Ids = []
    ent2Ids = []
    for ent1id in range(hashId1m, hashId1M):
        ent1 = ents1[ent1id]
        ent2id = bisect_left(ents2R, ent1)
        enttid = bisect_left(entstR, ent1)
        if ents2R[ent2id] == ent1 and entstR[enttid] == ent1:
            entsR.append(ent1)
            ent1Ids.append(ent1id)
            ent2Ids.append(ent2id)
    adjMatR1, predsR1 = filterPreds(adjMat1, ent1Ids, preds1)
    adjMatR2, predsR2 = filterPreds(adjMat2, ent2Ids, preds2)
    adjMat = np.array(np.dot(np.transpose(adjMatR1), adjMatR2)!=0, dtype=int)
    simVec1, simVec2 = computeSingleSimVec(predsR1, R1), computeSingleSimVec(predsR2, R2)
    minLen = min(len(simVec1), len(simVec2))
    sortIds1, sortIds2 = np.argsort(simVec1)[::-1][:minLen], np.argsort(simVec2)[::-1][:minLen]
    maxSim = 0
    result = None
    for i in range(minLen):
        id1, id2 = sortIds1[i], sortIds2[i]
        leftSim, rightSim = simVec1[id1], simVec2[id2]
        threshold = leftSim + rightSim
        if threshold <= maxSim:
            break
        leftMaxId, leftMaxSim = TAComputeMax(adjMat, id1, simVec1, 0)
        leftMaxSim += leftSim
        leftMaxId = (id1, leftMaxId)
        rightMaxId, rightMaxSim = TAComputeMax(adjMat, id2, simVec2, 1)
        rightMaxSim += rightSim
        rightMaxId = (rightMaxId, id2)
        if leftMaxSim > rightMaxSim and leftMaxSim > maxSim:
            maxSim = leftMaxSim
            result = leftMaxId
        elif leftMaxSim <= rightMaxSim and rightMaxSim > maxSim:
            maxSim = rightMaxSim
            result = rightMaxId
    return maxSim, predsR1[result[0]], predsR2[result[1]]

def search(ent1, ents2, entst):
    try:
        ent2id = ents2.index(ent1)
        enttid = entst.index(ent1)
        return ent2id
    except:
        return -1

def TAUpdate(adjMat1, adjMat2, simVec, currId, maxPreds, ents1, ents2, entst):
    maxSim = 0
    maxId = None
    for entid in np.where(adjMat1[:,currId]!=0)[0]:
        ent = ents1[entid]
        if entid not in maxPreds:
            ent2id = search(ent, ents2, entst)
            if ent2id != -1:
                currMaxId, currMaxSim = getMaxPE(adjMat2, ent2id, simVec)
                maxPreds[entid] = (currMaxId, currMaxSim)
        else:
            currMaxId, currMaxSim = maxPreds[entid]
        if currMaxSim > maxSim:
            maxSim = currMaxSim
            maxId = currMaxId
    return maxSim, maxId

def newTAInstant(e1, R1, e2, R2, t):
    r = (max(neighborhashes[e1][0], neighborhashes[e2][0], neighborhashes[t][0]),
         min(neighborhashes[e1][1], neighborhashes[e2][1], neighborhashes[t][1]))
    adjMatR1, predsR1, entsR1 = filterEntsPreds(e1, r)
    adjMatR2, predsR2, entsR2 = filterEntsPreds(e2, r)
    entst, hashest = getNT(t)
    hashIdtm, hashIdtM = bisect_left(hashest, r[0]), bisect_right(hashest, r[1])
    entsRt = entst[hashIdtm:hashIdtM]
    simVec1, simVec2 = computeSingleSimVec(predsR1, R1), computeSingleSimVec(predsR2, R2)
    minLen = min(len(simVec1), len(simVec2))
    sortIds1, sortIds2 = np.argsort(simVec1)[::-1][:minLen], np.argsort(simVec2)[::-1][:minLen]
    maxSim = 0
    result = None
    ent1MaxPreds = {}
    ent2MaxPreds = {}
    for i in range(minLen):
        id1, id2 = sortIds1[i], sortIds2[i]
        leftSim, rightSim = simVec1[id1], simVec2[id2]
        threshold = leftSim + rightSim
        if threshold <= maxSim:
            return (maxSim, *result)
        rightMaxSim, rightMaxId = TAUpdate(adjMatR1, adjMatR2, simVec2, id1, ent1MaxPreds, entsR1, entsR2, entsRt)
        rightMaxSim += leftSim
        rightMaxId = (id1, rightMaxId)
        leftMaxSim, leftMaxId = TAUpdate(adjMatR2, adjMatR1, simVec1, id2, ent2MaxPreds, entsR2, entsR1, entsRt)
        leftMaxSim += rightSim
        rightMaxId = (leftMaxId, id2)
        if leftMaxSim > rightMaxSim and leftMaxSim > maxSim:
            maxSim = leftMaxSim
            result = leftMaxId
        elif leftMaxSim <= rightMaxSim and rightMaxSim > maxSim:
            maxSim = rightMaxSim
            result = rightMaxId
    return maxSim, predsR1[result[0]], predsR2[result[1]]

def newNRA(e1, R1, e2, R2, t):
    r = (max(neighborhashes[e1][0], neighborhashes[e2][0], neighborhashes[t][0]),
         min(neighborhashes[e1][1], neighborhashes[e2][1], neighborhashes[t][1]))
    adjMatR1, predsR1, entsR1 = filterEntsPreds(e1, r)
    adjMatR2, predsR2, entsR2 = filterEntsPreds(e2, r)
    entst, hashest = getNT(t)
    hashIdtm, hashIdtM = bisect_left(hashest, r[0]), bisect_right(hashest, r[1])
    entsRt = entst[hashIdtm:hashIdtM]
    simVec1, simVec2 = computeSingleSimVec(predsR1, R1), computeSingleSimVec(predsR2, R2)
    vl1 = len(simVec1)
    vl2 = len(simVec2)
    maxLen, minLen, maxId = (vl1, vl2, 0) if vl1 > vl2 else (vl2, vl1, 1)
    sortIds1, sortIds2 = np.argsort(simVec1)[::-1], np.argsort(simVec2)[::-1]
    maxSim = 0
    result = None
    heaps1, heaps2 = NRAHeap(), NRAHeap()
    for i in range(maxLen):
        flag1 = i <= minLen or maxId == 0
        flag2 = i <= minLen or maxId == 1
        if flag1:
            id1 = sortIds1[i]
            leftSim = simVec1[id1]
        else:
            leftSim = 0
        if flag2:
            id2 = sortIds2[i]
            rightSim = simVec2[id2]
        else:
            rightSim = 0
        threshold = leftSim + rightSim
        if flag1:
            pred1ents = [entsR1[ent1id] for ent1id in np.where(adjMatR1[:id1]!=0)[0] if entsR1[ent1id] in entsRt]
            for ent1 in pred1ents:
                heaps1.push(ent1, id1, leftSim, threshold)
        if flag2:
            pred2ents = [entsR2[ent2id] for ent2id in np.where(adjMatR2[:id1]!=0)[0] if entsR2[ent2id] in entsRt]
            for ent2 in pred2ents:
                heaps2.push(ent2, id2, rightSim, threshold)
        if flag1:
            for ent1 in pred1ents:
                resultP, resultS = heaps2.update(ent1, leftSim)
                if not resultP is None and resultS > maxSim:
                    result = id1, resultP
        if flag2:
            for ent2 in pred2ents:
                resultP, resultS = heaps1.update(ent2, rightSim)
                if not resultP is None and resultS > maxSim:
                    result = resultP, id2
        if maxSim >= heaps1.getMaxUB() and maxSim >= heaps2.getMaxUB():
            break
    return maxSim, predsR1[result[0]], predsR2[result[1]]

def getUBs(ents, R):
    ubs = entubs[ents]
    rsm = np.max(np.dot(spacemarks, np.transpose(normalize(np.stack([model[ls] for ls in R])))), axis=1)
    ubs = ubs + rsm
    ubSortIds = np.argsort(ubs)[::-1]
    return ubs, ubSortIds
    
def getCS2(simVec, ubs, c):
    return [simVec[pred1Id]+ubs[ent1Id] for pred1Id, ent1Id in c]

def newTA2(e1, t, R1, R2):
    adjMat1, preds1, ents1, simVec1, sortIds1 = getPreds(e1, t, R1)
    ubs, ubSortIds = getUBs(ents1, R2)
    maxSim = 0
    result = None
    entMaxP = {}
    for i in range(len(sortIds1)):
        pred1Id = sortIds1[i]
        ubId = ubSortIds[i]
        threshold = simVec1[pred1Id] + ubs[ubId]
        if maxSim > threshold:
            break
        cs = []
        for ent1Id in np.where(adjMat1[:,pred1Id]!=0)[0]:
            heapq.heappush(cs, (simVec1[pred1Id]+ubs[ent1Id], pred1Id, ent1Id))
        for c2pred1Id in np.where(adjMat1[ubId]!=0)[0]:
            heapq.heappush(cs, (simVec1[c2pred1Id]+ubs[ubId], c2pred1Id, ubId))
        for j in range(len(cs)-1, 0):
            currSim, p1, e = cs[j]
            if currSim < maxSim:
                break
            if e not in entMaxP:
                p2, maxSim2 = getMaxP(e, R2)
                entMaxP[e] = (p2, maxSim2)
            else:
                p2, maxSim2 = entMaxP[e]
            currSim = simVec1[p1] + maxSim2
            if currSim < maxSim:
                continue
            maxSim = currSim
            result = (preds1[p1], p2)
            if maxSim > cs[j-1][0]:
                break
    return (maxSim, *result)

def newNRA2(e1, t, R1, R2):
    adjMat1, preds1, ents1, simVec1, sortIds1 = getPreds(e1, t, R1)
    ubs, ubSortIds = getUBs(ents1, R2)
    vl1, vl2 = len(simVec1), len(ubs)
    maxLen, minLen, maxId = (vl1, vl2, 0) if vl1 > vl2 else (vl2, vl1, 1)
    maxSim = 0
    result = None
    heaps1, heaps2 = NRAHeap(), NRAHeap()
    entMaxP = {}
    for i in range(maxLen):
        flag1 = i <= minLen or maxId == 0
        flag2 = i <= minLen or maxId == 1
        if flag1:
            id1 = sortIds1[i]
            leftSim = simVec1[id1]
        else:
            leftSim == 0
        if flag2:
            id2 = ubSortIds[i]
            rightSim = ubs[id2]
        else:
            rightSim == 0
        threshold = leftSim + rightSim
        if flag1:
            ent1ids = np.where(adjMat1[:id1]!=0)[0]
            for ent1id in ent1ids:
                heaps1.push(ents1[ent1id], id1, leftSim, threshold)
        if flag2:
            heaps2.push(ents1[id2], id2, rightSim, threshold)
        if flag1:
            for ent1id in ent1ids:
                resultP, resultS = heaps2.update(ents1[ent1id], leftSim)
                if not resultP is None and resultS > maxSim:
                    if ent1id not in entMaxP:
                        p2, maxSim2 = getMaxP(ent1id, R2)
                        entMaxP[ent1id] = (p2, maxSim2)
                    else:
                        p2, maxSim2 = entMaxP[ent1id]
                    currSim = simVec1[id1] + maxSim2
                    if currSim > maxSim:
                        maxSim = currSim
                        result = (preds1[id1], p2)
        if flag2:
            resultP, resultS = heaps1.update(ents1[id2], rightSim)
            if not resultP is None and resultS > maxSim:
                if id2 not in entMaxP:
                    p2, maxSim2 = getMaxP(id2, R2)
                    entMaxP[id2] = (p2, maxSim2)
                else:
                    p2, maxSim2 = entMaxP[id2]
                currSim = simVec1[resultP] + maxSim2
                if currSim > maxSim:
                    maxSim = currSim
                    result = (preds1[resultP], p2)
        if maxSim >= heaps1.getMaxUB() and maxSim >= heaps2.getMaxUB():
            break
    return (maxSim, *result)

def getHN(ents1, ents2):
    ents1HNs = {}
    ents2HNs = {}
    for ent1id, ent1 in enumerate(ents1):
        for ent2id, ent2 in enumerate(ents2):
            if HN(ent1, ent2) and HN(ent2, ent1):
                if ent1id in ents1HNs:
                    ents1HNs[ent1id].add(ent2id)
                else:
                    ents1HNs[ent1id] = {ent2id}
                if ent2id in ents2HNs:
                    ents2HNs[ent2id].add(ent1id)
                else:
                    ents2HNs[ent2id] = {ent1id}
    return ents1HNs, ents2HNs

def getEntHNs(ent1, ents2):
    return {ent2id for ent2id, ent2 in enumerate(ents2) if HN(ent1, ent2)}

def getCS3(simVec1, ubs, simVec2, c):
    return [simVec1[pred1Id]+ubs[ent1Id]+simVec2[pred2Id] for pred1Id, ent1Id, _, pred2Id in c]

def newTA3Pre(e1, t1, R1, e2, t2, R2, R3):
    adjMat1, preds1, ents1, simVec1, sortIds1 = getPreds(e1, t1, R1)
    adjMat2, preds2, ents2, simVec2, sortIds2 = getPreds(e2, t2, R2)
    ubs, ubSortIds = getUBs(ents1, R3)
    ents1HNs, ents2HNs = getHN(ents1, ents2)
    maxSim = 0
    result = None
    cs = []
    for i in range(len(sortIds1)):
        pred1Id = sortIds1[i]
        ubId = ubSortIds[i]
        pred2Id = sortIds2[i]
        threshold = simVec1[pred1Id] + ubs[ubId] + simVec2[pred2Id]
        if maxSim > threshold:
            break
        for ent1Id in np.where(adjMat1[:,pred1Id]!=0)[0]:
            for ent2Id in ents1HNs[ent1Id]:
                for c1pred2Id in np.where(adjMat2[ent2Id]!=0)[0]:
                    heapq.heappush(cs, (simVec1[pred1Id]+ubs[ent1Id]+simVec2[c1pred2Id], pred1Id, ent1Id, ent2Id, c1pred2Id))
        for c2pred1Id in np.where(adjMat1[ubId]!=0)[0]:
            for ent2Id in ents1HNs[ubId]:
                for c2pred2Id in np.where(adjMat2[ent2Id]!=0)[0]:
                    heapq.heappush(cs, (simVec1[c2pred1Id]+ubs[ubId]+simVec2[c2pred2Id], c2pred1Id, ubId, ent2Id, c2pred2Id))
        for ent2Id in np.where(adjMat2[:,pred2Id]!=0)[0]:
            for ent1Id in ents2HNs[ent2Id]:
                for c3pred1Id in np.where(adjMat1[ent1Id]!=0)[0]:
                    heapq.heappush(cs, (simVec1[c3pred1Id]+ubs[ent1Id]+simVec2[pred2Id], c3pred1Id, ent1Id, ent2Id, pred2Id))        
        for c2pred1Id in np.where(adjMat1[ubId]!=0)[0]:
            heapq.heappush(cs, (simVec1[c2pred1Id]+ubs[ubId], c2pred1Id, ubId))
        for j in range(len(cs)-1, 0):
            currSim, lp, le, re, rp = cs[j]
            if currSim < maxSim:
                break   
            path = hasPath(ents1[le], ents2[re], R3)
            if not path is None:
                currSim = simVec1[lp] + path[1] + simVec2[rp]
                if currSim < maxSim:
                    continue
                maxSim = currSim
                result = (preds1[lp], path[0], preds2[rp])
                if maxSim > cs[j-1][0]:
                    break
    return (maxSim, *result)

def newTA3Instant(e1, t1, R1, e2, t2, R2, R3):
    adjMat1, preds1, ents1, simVec1, sortIds1 = getPreds(e1, t1, R1)
    adjMat2, preds2, ents2, simVec2, sortIds2 = getPreds(e2, t2, R2)
    ubs, ubSortIds = getUBs(ents1, R3)
    maxSim = 0
    result = None
    e1HNs = {}
    e2HNs = {}
    e1e2 = {}
    e1MaxP = {}
    e2MaxP = {}
    for i in range(len(sortIds1)):
        pred1Id = sortIds1[i]
        ubId = ubSortIds[i]
        pred2Id = sortIds2[i]
        threshold = simVec1[pred1Id] + ubs[ubId] + simVec2[pred2Id]
        if maxSim > threshold:
            break
        determinedMax = 0
        determinedMaxId = None
        undetermined = []
        for e1id in np.where(adjMat1[:,pred1Id]!=0)[0]:
            if e1id in e1HNs:
                e2ids = e1HNs[e1id]
            else:
                e2ids = getEntHNs(ents1[e1id], ents2)
                e1HNs[e1id] = e2ids
            for e2id in e2ids:
                if e2id in e2MaxP:
                    rightPId, rightSim = e2MaxP[e2id]
                else:
                    rightPId, rightSim = getMaxPE(adjMat2, e2id, simVec2)
                    e2MaxP[e2id] = (rightPId, rightSim)
                if (e1id, e2id) in e1e2:
                    middle = e1e2[(e1id, e2id)]
                    if middle is None:
                        continue
                    middleP, middleSim = middle
                    currDetermined = simVec1[pred1Id] + middleSim + rightSim
                    if currDetermined > determinedMax:
                        determinedMax = currDetermined
                        determinedMaxId = (pred1Id, middleP, rightPId)
                else:
                    heapq.heappush(undetermined, (simVec1[pred1Id] + ubs[e1id] + rightSim, pred1Id, e1id, e2id, rightPId))
        if ubId in e1MaxP:
            middleLeftPId, middleLeftSim = e1MaxP[ubId]
        else:
            middleLeftPId, middleLeftSim = getMaxPE(adjMat1, ubId, simVec1)
            e1MaxP[ubId] = (middleLeftPId, middleLeftSim)
        if ubId in e1HNs:
            e2ids = e1HNs[ubId]
        else:
            e2ids = getEntHNs(ents1[ubId], ents2)
            e1HNs[ubId] = e2ids
        for e2id in e2ids:
            if e2id in e2MaxP:
                middleRightPId, middleRightSim = e2MaxP[e2id]
            else:
                middleRightPId, middleRightSim = getMaxPE(adjMat2, e2id, simVec2)
                e2MaxP[e2id] = (middleRightPId, middleRightSim)
            if (ubId, e2id) in e1e2:
                middle = e1e2[(ubId, e2id)]
                if middle is None:
                    continue
                middleP, middleSim = middle
                currDetermined = middleLeftSim + middleSim + middleRightSim
                if currDetermined > determinedMax:
                    determinedMax = currDetermined
                    determinedMaxId = (pred1Id, middleP, middleRightPId)
            else:
                heapq.heappush(undetermined, (middleLeftSim + ubs[ubId] + rightSim, middleLeftPId, ubId, e2id, middleRightPId))
        for e2id in np.where(adjMat2[:,pred2Id]!=0)[0]:
            if e2id in e2HNs:
                e1ids = e2HNs[e2id]
            else:
                e1ids = getEntHNs(ents2[e2id], ents1)
                e2HNs[e2id] = e1ids
            for e1id in e1ids:
                if e1id in e1MaxP:
                    leftPId, leftSim = e1MaxP[e1id]
                else:
                    leftPId, leftSim = getMaxPE(adjMat1, e1id, simVec1)
                    e1MaxP[e1id] = (leftPId, leftSim)
                if (e1id, e2id) in e1e2:
                    middle = e1e2[(e1id, e2id)]
                    if middle is None:
                        continue
                    middleP, middleSim = middle
                    currDetermined = leftSim + middleSim + simVec2[pred2Id]
                    if currDetermined > determinedMax:
                        determinedMax = currDetermined
                        determinedMaxId = (leftPId, middleP, pred2Id)
                else:
                    heapq.heappush(undetermined, (leftSim + ubs[e1id] + simVec2[pred2Id], leftPId, e1id, e2id, pred2Id))
        undeterminedMaxSim, p1id, e1id, e2id, p2id = undetermined.pop()
        undeterminedMaxSim = -undeterminedMaxSim
        if undeterminedMaxSim <= determinedMax and determinedMax <= maxSim:
            continue
        elif undeterminedMaxSim <= determinedMax and determinedMax > maxSim:
            maxSim = determinedMax
            result = determinedMaxId
        elif determinedMax > maxSim:
            currMaxSim = determinedMax
            currMaxId = determinedMaxId
            while (undeterminedMaxSim > currMaxSim and currMaxSim > maxSim):
                if (e1id, e2id) in e1e2:
                    middle = e1e2[(e1id, e2id)]
                    if not middle is None:
                        middleP, middleSim = middle
                    else:
                        undeterminedMaxSim, p1id, e1id, e2id, p2id = undetermined.pop()
                        undeterminedMaxSim = -undeterminedMaxSim
                        continue
                else:
                    path = hasPath(ents1[e1id], ents2[e2id], R3)
                    if not path is None:
                        middleP, middleSim = path
                        e1e2[(e1id, e2id)] = path
                    else:
                        e1e2[(e1id, e2id)] = None
                        undeterminedMaxSim, p1id, e1id, e2id, p2id = undetermined.pop()
                        undeterminedMaxSim = -undeterminedMaxSim
                        continue
                currSim = simVec1[p1id] + middleSim + simVec2[p2id]
                if currSim > currMaxSim:
                    currMaxSim = currSim
                    currMaxId = (p1id, middleP, p2id)
                undeterminedMaxSim, p1id, e1id, e2id, p2id = undetermined.pop()
                undeterminedMaxSim = -undeterminedMaxSim
            maxSim = currMaxSim
            result = currMaxId
        else:
            while undeterminedMaxSim > maxSim:
                if (e1id, e2id) in e1e2:
                    middle = e1e2[(e1id, e2id)]
                    if not middle is None:
                        middleP, middleSim = middle
                    else:
                        undeterminedMaxSim, p1id, e1id, e2id, p2id = undetermined.pop()
                        undeterminedMaxSim = -undeterminedMaxSim
                        continue
                else:
                    path = hasPath(ents1[e1id], ents2[e2id], R3)
                    if not path is None:
                        middleP, middleSim = path
                        e1e2[(e1id, e2id)] = path
                    else:
                        e1e2[(e1id, e2id)] = None
                        undeterminedMaxSim, p1id, e1id, e2id, p2id = undetermined.pop()
                        undeterminedMaxSim = -undeterminedMaxSim
                        continue
                currSim = simVec1[p1id] + middleSim + simVec2[p2id]
                if currSim > maxSim:
                    maxSim = currSim
                    result = (p1id, middleP, p2id)
                undeterminedMaxSim, p1id, e1id, e2id, p2id = undetermined.pop()
                undeterminedMaxSim = -undeterminedMaxSim
    return maxSim, preds1[result[0]], result[1], preds2[result[2]]
    
def newNRA3(e1, t1, R1, e2, t2, R2, R3):
    adjMat1, preds1, ents1, simVec1, sortIds1 = getPreds(e1, t1, R1)
    adjMat2, preds2, ents2, simVec2, sortIds2 = getPreds(e2, t2, R2)
    ubs, ubSortIds = getUBs(ents1, R3)
    maxSim = 0
    result = None
    e1e2 = {}
    e1MaxP = {}
    e2MaxP = {}
    heaps1, heaps2, heaps3 = NRA3Heap(), NRA3Heap(), NRA3Heap()
    vls = (len(simVec1), len(ubs), len(simVec2))
    minId, middleId, maxId = np.argsort(vls)
    minLen, middleLen, maxLen = vls[minId], vls[middleId], vls[minId]
    for i in range(maxLen):
        minFlag = i <= minLen
        middleFlag = i<=middleLen
        flag1 = minFlag or maxId == 0 or (middleFlag and middleId==0)
        flag2 = minFlag or maxId == 1 or (middleFlag and middleId==1)
        flag3 = minFlag or maxId == 2 or (middleFlag and middleId==2)
        if flag1:
            id1 = sortIds1[i]
            leftSim = simVec1[id1]
        else:
            leftSim == 0
        if flag2:
            id2 = ubSortIds[i]
            ubSim = ubs[id2]
        else:
            ubSim == 0
        if flag3:
            id3 = sortIds2[i]
            rightSim = simVec2[id3]
        else:
            rightSim == 0
        threshold = leftSim + ubSim + rightSim
        if flag1:
            ent1ids = np.where(adjMat1[:id1]!=0)[0]
            for ent1id in ent1ids:
                heaps1.push(ents1[ent1id], id1, leftSim, threshold)
        if flag2:
            heaps2.push(ents1[id2], id2, ubSim, threshold)
        if flag3:
            ent2ids = np.where(adjMat2[:id3]!=0)[0]
            for ent2id in ent2ids:
                heaps3.push(ents2[ent2id], id3, rightSim, threshold)
        if flag1:
            for ent1id in ent1ids:
                resultP1, resultS1 = heaps2.update(ents1[ent1id], leftSim)
                if not resultP1 is None:
                    results2 = heaps3.updateWithNeighbor(ents1[ent1id], leftSim)
                    for j in range(len(results2)-1, -1):
                        resultP2, ent2id, resultS2 = results2[j]
                        currSim = resultS1 + resultS2
                        if currSim < maxSim:
                            break
                        if (ent1id, ent2id) in e1e2:
                            middle = e1e2[(ent1id, ent2id)]
                            if not middle is None:
                                middleP, middleSim = middle
                        else:
                            path = hasPath(ents1[ent1id], ents2[ent2id], R3)
                            if not path is None:
                                middleP, middleSim = path
                                e1e2[(ent1id, ent2id)] = path
                            else:
                                e1e2[(ent1id, ent2id)] = None
                                continue
                        currSim = leftSim + middleSim + resultS2
                        if currSim > maxSim:
                            maxSim = currSim
                            result = id1, middleP, resultP2
                else:
                    heaps3.updateOnly(ents1[ent1id], leftSim)
        if flag2:
            resultP1, resultS1 = heaps1.update(ents1[id2], ubSim)
            if not resultP1 is None:
                results2 = heaps3.updateWithNeighbor(ents1[id2], ubSim)
                for j in range(len(results2)-1, -1):
                    resultP2, ent2id, resultS2 = results2[j]
                    currSim = resultS1 + resultS2
                    if currSim < maxSim:
                        break
                    if (id2, ent2id) in e1e2:
                        middle = e1e2[(id2, ent2id)]
                        if not middle is None:
                            middleP, middleSim = middle
                    else:
                        path = hasPath(ents1[id2], ents2[ent2id], R3)
                        if not path is None:
                            middleP, middleSim = path
                            e1e2[(id2, ent2id)] = path
                        else:
                            e1e2[(id2, ent2id)] = None
                            continue
                    currSim = resultS1 - ubSim + middleSim + resultS2
                    if currSim > maxSim:
                        maxSim = currSim
                        result = resultP1, middleP, resultP2
            else:
                heaps3.updateOnly(ents1[id2], ubSim)
        if flag3:
            for ent2id in ent2ids:
                results2 = heaps2.updateWithNeighbor(ents2[ent2id], rightSim)
                for j in range(len(results2)-1, -1):
                    resultP2, ent1id, resultS2 = results2[j]
                    resultP1, resultS1 = heaps1.search(ents1[ent1id])
                    if not resultP1 is None:
                        currSim = resultS1 + resultS2
                        if currSim < maxSim:
                            continue
                        if (ent1id, ent2id) in e1e2:
                            middle = e1e2[(ent1id, ent2id)]
                            if not middle is None:
                                middleP, middleSim = middle
                        else:
                            path = hasPath(ents1[ent1id], ents2[ent2id], R3)
                            if not path is None:
                                middleP, middleSim = path
                                e1e2[(ent1id, ent2id)] = path
                            else:
                                e1e2[(ent1id, ent2id)] = None
                                continue
                        currSim = resultS1 + middleSim + rightSim
                        if currSim > maxSim:
                            maxSim = currSim
                            result = id1, middleP, resultP2
                heaps1.updateOnly(ents2[ent2id], rightSim)
        if maxSim >= heaps1.getMaxUB() and maxSim >= heaps2.getMaxUB() and maxSim >= heaps3.getMaxUB():
            break    
