class HNode:
    def __init__(self, child):
        self.child = child
        self.next = None

class HBox:
    def __init__(self, line, parentId):
        self.data = line
        self.parent = parentId
        self.firstchild = None

class Hierarchy:
    nodes = []
    classes = []
    def __init__(self, filename):
        with open(filename) as f:
            currId = 0
            previousIndentNum = -1
            parentStack = [-1]
            for line in f.readlines():
                currIndentNum = 0
                for i in range(len(line)):
                    w = line[i]
                    if w == '\t':
                        currIndentNum += 1
                    else:
                        strStart = i
                        break
                indentDif = previousIndentNum - currIndentNum + 1
                if indentDif != 0:
                    for i in range(indentDif):
                        parentStack.pop()
                parentId = parentStack[-1]
                parentStack.append(currId)
                previousIndentNum = currIndentNum
                line = line[strStart:-1].upper()
                newBox = HBox(line, parentId)
                self.nodes.append(newBox)
                self.classes.append(line)
                if parentId != -1:
                    newNode = HNode(currId)
                    currChild = self.nodes[parentId].firstchild
                    if currChild == None:
                        self.nodes[parentId].firstchild = newNode
                    else:
                        while currChild.next != None:
                            currChild = currChild.next
                        currChild.next = newNode
                currId += 1

    def to_string(self):
        s = ""
        for n in self.nodes:
            s += "Current: (" + n.data + ")\n\tChildrens are: "
            currChild = n.firstchild
            while currChild != None:
                s += "(" + str(currChild.child) + ", " + self.nodes[currChild.child].data + ") "
                currChild = currChild.next
            s += "\n"
        return s

    def findType(self, t):
        t = t.upper()
        if t in self.classes:
            return self.classes.index(t)
        else:
            return -1
    
    def getDepthMain(self, index):
        if index != -1:
            depth = 0
            while index != -1:
                depth += 1
                index = self.nodes[index].parent
            return depth
        else:
            return -1

    def getDepth(self, t):
        index = self.findType(t)
        return self.getDepthMain(index)

    def isChildMain(self, t1, t2):
        if t1 == t2:
            return True
        elif t2 == -1:
            return False
        else:
            return self.isChildMain(t1, self.nodes[t2].parent)
    
    def isChild(self, t1, t2):
        i1 = self.findType(t1)
        i2 = self.findType(t2)
        if i1 == -1 or i2 == -1:
            return False
        if i1 == i2:
            return True
        d1 = self.getDepthMain(i1)
        d2 = self.getDepthMain(i2)
        if d1 < d2:
            return self.isChildMain(i1, i2)
        else:
            return self.isChildMain(i2, i1)
    
    def findParent(self, t1, t2):
        if t1 == '':
            return t2
        if t2 == '':
            return t1
        if self.getDepth(t1) < self.getDepth(t2):
            return t2
        else:
            return t1