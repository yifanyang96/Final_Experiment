qf = open('LCQ_CDQ')
qlines = qf.readlines()
ef = open('LCQ_CDE')
elines = ef.readlines()
#ef2 = open('LCQ_DE2', 'w')
for id in range(len(qlines)):
    print(id)
    line = qlines[id].strip()
    entity = elines[id].strip().split(';')
    if entity[0].split('|')[0] not in line or entity[1].split('|')[0] not in line:
    #if line.index(entity[0].split('|')[0]) == line.index(entity[1].split('|')[0]):
        print('ERROR!')
    # if line.index(entity[0].split('|')[0]) < line.index(entity[1].split('|')[0]):
    #     ef2.write(entity[0]+';'+entity[1]+'\n')
    # else:
    #     ef2.write(entity[1]+';'+entity[0]+'\n')