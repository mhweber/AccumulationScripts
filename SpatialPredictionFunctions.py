import string
from collections import deque, defaultdict
import struct, decimal, itertools
from datetime import datetime

def dbfreader(f):
    """Returns an iterator over records in a Xbase DBF file.

    The first row returned contains the field names.
    The second row contains field specs: (type, size, decimal places).
    Subsequent rows contain the data records.
    If a record is marked as deleted, it is skipped.

    File should be opened for binary reads.

    """
    # See DBF format spec at:
    #     http://www.pgts.com.au/download/public/xbase.htm#DBF_STRUCT

    numrec, lenheader = struct.unpack('<xxxxLH22x', f.read(32))
    numfields = (lenheader - 33) // 32

    fields = []
    for fieldno in xrange(numfields):
        name, typ, size, deci = struct.unpack('<11sc4xBB14x', f.read(32))
        name = name.replace('\0', '')       # eliminate NULLs from string
        fields.append((name, typ, size, deci))
    yield [field[0] for field in fields]
    yield [tuple(field[1:]) for field in fields]

    terminator = f.read(1)
    assert terminator == '\r'

    fields.insert(0, ('DeletionFlag', 'C', 1, 0))
    fmt = ''.join(['%ds' % fieldinfo[2] for fieldinfo in fields])
    fmtsiz = struct.calcsize(fmt)
    for i in xrange(numrec):
        record = struct.unpack(fmt, f.read(fmtsiz))
        if record[0] != ' ':
            continue                        # deleted record
        result = []
        for (name, typ, size, deci), value in itertools.izip(fields, record):
            if name == 'DeletionFlag':
                continue
            if typ == "N":
                value = value.replace('\0', '').lstrip()
                if value == '':
                    value = 0
                elif deci:
                    value = decimal.Decimal(value)
                else:
                    value = int(value)
            elif typ == 'D':
                y, m, d = int(value[:4]), int(value[4:6]), int(value[6:8])
                value = datetime.date(y, m, d)
            elif typ == 'L':
                value = (value in 'YyTt' and 'T') or (value in 'NnFf' and 'F') or '?'
            result.append(value)
        yield result

def children(token, tree):
    "returns a list of every child"
    visited = set()
    to_crawl = deque([token])
    while to_crawl:
        current = to_crawl.popleft()
        if current in visited:
            continue
        visited.add(current)
        node_children = set(tree[current])
        to_crawl.extendleft(node_children - visited)
    return list(visited)

def ContinuousAllocation(Allocation, Accumulation, FullCOMs):
    startAllocTime = datetime.now()
    infile = open(Allocation,'r')
    first_line = infile.readline()
    data=infile.readlines()
    infile.close()
    outFile = open(Accumulation,'w')
    outFile.write("COMID,CatAreaSqKm,CatMean,CatPctFull,CatCount,CatSum,UpCatAreaSqKm,UpCatMean,UpCatPctFull,UpCatCount,UpCatSum\n")
    paramDict = defaultdict(list)
    print "Make dictionary from value file. " + str(datetime.now()- startAllocTime)
    comidList = []
    first_line = first_line.replace('"','').replace('\n', '')
    first_line = string.split(first_line,',')
    ind_dict = dict()
    for index,element in enumerate(first_line):
        ind_dict[element] = index
    for row in data:
        row = row.replace('NA','0')
        paramArray = string.split(row,',')
        # I'm passing index below using field names I'm looking for
        paramList = [float(paramArray[ind_dict['CatAreaSqKM']]), float(paramArray[ind_dict['CatMean']]),float(paramArray[ind_dict['CatPctFull']]),int(paramArray[ind_dict['CatCount']]),float(paramArray[ind_dict['CatSum']])]
        paramDict[int(paramArray[0])] = paramList
        comidList.append(paramArray[0])

    print "Calculating upstream allocations " + str(datetime.now()- startAllocTime)
    #allocDict = defaultdict(list)
    for item in comidList:
        item = int(item)
#        allocatedArea = 0.0
        fullArea = 0.0
        upcount = 0.0
        upsum = 0.0
        try:
            if len(FullCOMs[item]) > 1:
                weightedAvg = 0.0
                weightedUpFull = 0.0
                for upComID in str(FullCOMs[item]):
                    if upComID != item:
                        paramResult = paramDict[upComID]
                        if len(paramResult) > 1:
                            fullArea += paramResult[0]
                            allocatedArea += (paramResult[0]* paramResult[2])/100.00
                            upcount += paramResult[3]
                            upsum += paramResult[4]
                if allocatedArea != 0.0:
                    weightedUpFull = (allocatedArea / fullArea) * 100.00
                else:
                    weightedUpFull = 0
                if upcount != 0.0:
                    weightedAvg = upsum / float(upcount)
                else:
                    weightedAvg= 0.0
            else:
                #weightedAvg = float(paramDict[item][0])
                weightedAvg = 0.0
                weightedUpFull = 0.0
                upcount = 0.0
                upsum = 0.0
        except:
            weightedAvg = 0.0
            weightedUpFull = 0.0
            upcount = 0.0
            upsum = 0.0
        outString = "%i,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n" %(item,paramDict[item][0],paramDict[item][1],
                                                      paramDict[item][2],paramDict[item][3],paramDict[item][4],fullArea,
                                                      weightedAvg,weightedUpFull,upcount,upsum)
        outFile.write(outString)
    outFile.close()

def CategoricalAllocation(Allocation, Accumulation, FullCOMs):
    paramFile = open(Allocation,'r')
    paramDict = defaultdict(list)
    outFile = open(Accumulation,'w')
    header = paramFile.readline()
    # format the output file header
    classList = []
    header = header.replace('\n', '').replace('"','')
    classList = header.split(',')
    for items in classList[1:]:
        classList.append('Up' + items)
    outString = ', '.join(classList)
    outString = outString + '\n'
    outFile.write(outString)

    data = paramFile.readlines()
    paramFile.close()

    print "Making accumulations for categorical data"
    comidList = []
    for row in data:
        row = row.replace('NA','0')
        paramArray = string.split(row,',')
        paramList = []
        p = 0
        for param in paramArray:
            if p > 0:
                param = param.replace('\n', '')
                paramList.append(param)
            p = p + 1
        paramDict[int(paramArray[0])] = map(float, paramList)
        comidList.append(paramArray[0])

    # get upstream catchments
    for comid in comidList:
        try:
            comid = int(comid)
            if comid in FullCOMs.keys():
                catchResults = [i for i in paramDict[comid]]
                count=0
                if len(FullCOMs[comid]) > 1:
                    for upComID in FullCOMs[comid]:
                        if upComID != comid:
                            if count==0 and len(paramDict[upComID]) > 1:
                                upResults = [i for i in paramDict[upComID]]
                                count+=1
                            elif count > 0 and len(paramDict[upComID]) > 1:
                                temp = [i for i in paramDict[upComID]]
                                for index, item in enumerate(temp):
                                    if index < len(paramDict[upComID])-1:
                                        upResults[index] += item
                    if upResults[0] != 0:
                        upResults[-1] = int(round(((sum(upResults[1:-1])*1e-06) / upResults[0])* 100))
                    catchString = ','.join(map(str,catchResults))
                    catchString = str(comid) + ',' + catchString
                    upString = ','.join(map(str,upResults))
                    fullString = catchString +',' +  upString + '\n'
                    outFile.write(fullString)

                else:
                    catchString = ','.join(map(str,0))
                    upString = ','.join('0' for i in range(len(catchString.split(','))))
                    catchString = str(comid) +',' + catchString
                    fullString = catchString + upString + '\n'
                    outFile.write(fullString)
        except:
            print 'COMID ' + str(comid)
            break
    outFile.close()

def CountAllocation(Allocation, Accumulation, FullCOMs):
    paramFile = open(Allocation,'r')
    paramDict = defaultdict(list)
    outFile = open(Accumulation,'w')
    header = paramFile.readline()
    # format the output file header
    classList = []
    header = header.replace('\n', '').replace('"','')
    classList = header.split(',')
    for items in classList[1:]:
        classList.append('Up' + items)
    outString = ', '.join(classList)
    outString = outString + '\n'
    outFile.write(outString)

    data = paramFile.readlines()
    paramFile.close()

    print "Making accumulations for count data"
    comidList = []
    for row in data:
        row = row.replace('NA','0')
        paramArray = string.split(row,',')
        paramList = []
        p = 0
        for param in paramArray:
            if p > 0:
                param = param.replace('\n', '')
                paramList.append(param)
            p = p + 1
        paramDict[int(paramArray[0])] = map(float, paramList)
        comidList.append(paramArray[0])

    # get upstream catchments
    for comid in comidList:
        try:
            comid = int(comid)
            if comid in FullCOMs.keys():
                catchResults = [i for i in paramDict[comid]]
                count=0
                if len(FullCOMs[comid]) > 1:
                    for upComID in Full_COMs[comid]:
                        if upComID != comid:
                            if count==0 and len(paramDict[upComID]) > 1:
                                upResults = [i for i in paramDict[upComID]]
                                count+=1
                            elif count > 0 and len(paramDict[upComID]) > 1:
                                temp = [i for i in paramDict[upComID]]
                                for index, item in enumerate(temp):
                                    if index < len(paramDict[upComID])-1:
                                        upResults[index] += item
                    upResults[-1] = upResults[1] / upResults[0]
                    catchString = ','.join(map(str,catchResults))
                    catchString = str(comid) + ',' + catchString
                    upString = ','.join(map(str,upResults))
                    fullString = catchString +',' +  upString + '\n'
                    outFile.write(fullString)

                else:
                    catchString = ', '.join(map(str,catchResults))
                    upString = ','.join('0.0' for i in catchResults)
                    catchString = str(comid) +',' + catchString
                    fullString = catchString +',' + upString + '\n'
                    outFile.write(fullString)
        except:
            pass
    outFile.close()
    return comidList
    return paramDict