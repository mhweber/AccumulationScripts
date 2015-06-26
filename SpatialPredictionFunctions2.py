# Allocation functions:
# Author: Marc Weber & Ryan Hill 
# Date: June 2015
# Python 2.7

import pysal as ps
import numpy as np
import numpy.lib.recfunctions as rfn
import pandas as pd
from datetime import datetime
import string
from collections import deque, defaultdict
import struct, decimal, itertools

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

 ##################################################################################################################### 
def ContinuousAllocation(Allocation, Accumulation, FullCOMs): 
    startfunTime = datetime.now()
    COMIDs = np.array(FullCOMs.keys())#Get FullCOMs from standard script     
    arr = pd.read_csv(Allocation)    
    arr = arr[['COMID','CatAreaSqKM','CatMean','CatPctFull','CatCount','CatSum']] #Rearrange table
    arr = np.array(arr) #Convert to numpy array   
    arr = np.c_[arr, np.zeros((arr.shape[0],5))] #Add 0s columns after Cat results
    arr = np.nan_to_num(arr) #Convert NaN to 0    
    
    for com in COMIDs:
        loc = np.where(arr[:,0] == com) #Get row location in arr        
        UpComs = np.array(FullCOMs[com]) #Make np.array from dictionary  
                
        if UpComs.size > 1:            
            UpComs = UpComs[UpComs <> com] #Remove focal catchment from list (to get Up only)            
            tmparr =  arr[np.in1d(arr[:,0], UpComs, assume_unique=True)] #Select subset of arr that match up COMIDs 
                #Calculate UpCatAreaSqKm(6), UpCatCount(9), and UpCatSum(10)              
            arr[loc, 6] = np.sum(tmparr[:, 1])  
            arr[loc, 9] = np.sum(tmparr[:, 4]) 
            arr[loc, 10] = np.sum(tmparr[:, 5])
            try: #Calculate UpCatMean with CatAreaSqKm & CatPctFull as weights
                arr[loc, 7] = np.average(tmparr[:, 2], weights = (tmparr[:, 1] * tmparr[:, 3]))
            except: 
                arr[loc, 7] = 0            
            try: #Calculate UpCatPctFull with CatAreaSqKm as weight
                arr[loc, 8] = np.average(tmparr[:, 3], weights = tmparr[:, 1])
            except:
                arr[loc, 8] = 0
        
    outDF = pd.DataFrame(arr)    
    print "!!!!elapsed time!!!!! " + str(datetime.now()-startfunTime)
    outDF.columns = ['COMID','CatAreaSqKm','CatMean','CatPctFull','CatCount','CatSum','UpCatAreaSqKm','UpCatMean','UpCatPctFull','UpCatCount','UpCatSum']
      #convert COMID to integer and then string  
    outDF['COMID'] = outDF['COMID'].astype(int).astype(str) 
        #Accumulation = "D:/SSWR/Phase1/SpatialPredFunc/tester.csv"    
    outDF.to_csv(Accumulation, sep=",", index=False)

#####################################################################################################################
def CategoricalAllocation(Allocation, Accumulation, FullCOMs):
    startfunTime = datetime.now()   
    COMIDs = np.array(FullCOMs.keys())#Get FullCOMs from standard script    
    arr = pd.read_csv(Allocation)  
    names = arr.columns #Get column names   
    nCols = names.size - 1 #Get the number of columns we need to add to the array       
    arr = np.c_[arr, np.zeros((arr.shape[0],nCols))] #Add 0s columns after Cat results    
    arr = np.nan_to_num(arr) #Convert NaN to 0   
    
    for com in COMIDs:
        #com = 1861424
        loc = np.where(arr[:,0] == com) #Get row location in arr        
        UpComs = np.array(FullCOMs[com]) #Make np.array from dictionary 

        if UpComs.size > 1:            
            UpComs = UpComs[UpComs <> com] #Remove focal catchment from list (to get Up only)            
            tmparr =  arr[np.in1d(arr[:,0], UpComs, assume_unique=True)] #Select subset of arr that match up COMIDs
                #Calculate all UpSum values for all categories and UpAreaSqKm
            arr[loc, nCols+1:arr.shape[1]-1] = np.sum(tmparr[:, 1:nCols], axis=0)  
            try: #Calculate UpCatPctFull with CatAreaSqKm as weight                       
                    #UpPctFull is last column in table: arr.shape[1]-1]  
                    #Use nCols to specify the CatPctFull column (last column in input table)
                arr[loc, arr.shape[1]-1] = np.average(tmparr[:, nCols], weights = tmparr[:, 1])
            except:
                arr[loc, arr.shape[1]-1] = 0
    
    outDF = pd.DataFrame(arr)    
    print "!!!!elapsed time!!!!! " + str(datetime.now()-startfunTime)
    names = np.concatenate([names.values, 'Up'+names.values[1:]])    
    outDF.columns = names
        #convert COMID to integer and then string  
    outDF['COMID'] = outDF['COMID'].astype(int).astype(str) 
        #Accumulation = "D:/SSWR/Phase1/SpatialPredFunc/tester.csv"    
    outDF.to_csv(Accumulation, sep=",", index=False)

#####################################################################################################################
def CountAllocation(Allocation, Accumulation, FullCOMs):
    startfunTime = datetime.now()   
    COMIDs = np.array(FullCOMs.keys())#Get FullCOMs from standard script
    
    arr = pd.read_csv(Allocation)  
    names = arr.columns #Get column names      
    nCols = names.size - 1 #Get the number of columns we need to add to the array       
    arr = np.c_[arr, np.zeros((arr.shape[0],nCols))] #Add 0s columns after Cat results    
    arr = np.nan_to_num(arr) #Convert NaN to 0   
       
    for com in COMIDs:
        #com = 1861424
        loc = np.where(arr[:,0] == com) #Get row location in arr        
        UpComs = np.array(FullCOMs[com]) #Make np.array from dictionary 
        
        if UpComs.size > 1:
            UpComs = UpComs[UpComs <> com] #Remove focal catchment from list (to get Up only)            
            tmparr =  arr[np.in1d(arr[:,0], UpComs, assume_unique=True)] #Select subset of arr that match up COMIDs
                #Calculate Up sums
            arr[loc, nCols+1:arr.shape[1]-1] = np.sum(tmparr[:, 1:nCols], axis=0)  
            
            try: #Calculate UpCatPctFull with CatAreaSqKm as weight 
                    #UpPctFull is last column in table: arr.shape[1]-1]  
                    #Use nCols to specify the CatPctFull column (last column in input table)            
                arr[loc, arr.shape[1]-1] = np.average(tmparr[:, nCols], weights = tmparr[:, 1])
            except:
                arr[loc, arr.shape[1]-1] = 0 
                
            try: #Calculate UpCatMean with CatAreaSqKm & CatPctFull as weights
                    #Use nCols to specify the CatPctFull column (last column in input table)   
                CatMeanLoc = np.where(names.values == 'CatMean')[0][0]    
                arr[loc, (CatMeanLoc+nCols)] = np.average(tmparr[:, CatMeanLoc], weights = (tmparr[:, 1] * tmparr[:, nCols]))
            except:
                arr[loc, (CatMeanLoc+nCols)] = 0    
                
    outDF = pd.DataFrame(arr)    
    print "!!!!elapsed time!!!!! " + str(datetime.now()-startfunTime)
    names = np.concatenate([names.values, 'Up'+names.values[1:]])    
    outDF.columns = names
        #convert COMID to integer and then string  
    outDF['COMID'] = outDF['COMID'].astype(int).astype(str) 
        #Accumulation = "D:/SSWR/Phase1/SpatialPredFunc/tester.csv"    
    outDF.to_csv(Accumulation, sep=",", index=False)                
            
#####################################################################################################################
#def UpAccum(Allocation, Accumulation, FullCOMs, accum_type='Continuous'):    
#    #startfunTime = datetime.now()   
#    COMIDs = np.array(FullCOMs.keys())#Get FullCOMs from standard script
#    arr = pd.read_csv(Allocation) 
#    names = arr.columns #Get column names 
#    if accum_type == 'Continuous':
#        arr = arr[['COMID','CatAreaSqKM','CatMean','CatPctFull','CatCount','CatSum']] #Rearrange table
#        nCols = 5
#    else:
#        names = arr.columns #Get column names      
#        nCols = names.size - 1 #Get the number of columns we need to add to the array 
#        
#    arr = np.c_[arr, np.zeros((arr.shape[0],nCols))] #Add 0s columns after Cat results    
#    arr = np.nan_to_num(arr) #Convert NaN to 0  
#    
#    for com in COMIDs:
#        print com
#        #com = 1861424    
#    

#####################################################################################################################    
#def ContinuousAllocationIDW(Allocation, Accumulation, FullCOMs, NHDPath, hydroregion, zone, wt_type='none', dist=10): 
#    '''
#    __author__ = "Ryan Hill <hill.ryan@epa.gov>"    
#    
#    Accumulate upstream catchments for each NHDPlusV2 catchment. 
#    Apply an inverse distance weighting scheme to upstream catchment values.    
#    
#    Arguments
#    ---------
#    Allocation      : csv table
#                      Input catchment summaries                      
#    Accumulaiton    : csv table
#                      Output accumulated summaries                      
#    FullCOMs        : Python dictionary 
#                      List catchments with associated upstream catchments
#    NHDPath         : str
#                      Path to NHDPlus files
#    hydroregion     : str
#                      NHDPlusV2 HydroRegion (ex: 'MS' for Mississippi)
#    zone            : str
#                      NHDPlusV2 Vector Processing Unit (ex: '06')
#    wt_type         : str
#                      Type of upstream weighting scheme: 'none', 'negexp', 'cond'                      
#    dist            : number
#                      Distance to use in distance schemes
#    '''
#    startfunTime = datetime.now()
#        #Get the allocation table setup
#    COMIDs = np.array(FullCOMs.keys())#Get FullCOMs from standard script     
#    arr = pd.read_csv(Allocation)   
#    arr = arr[['COMID','CatAreaSqKM','CatMean','CatPctFull','CatCount','CatSum']] #Rearrange table
#        #Get pathlength set up
#    flowlines = NHDPath + "/NHDPlus%s/NHDPlus%s/NHDPlusAttributes/PlusFlowlineVAA.dbf"%(hydroregion, zone)
#    db = ps.open(flowlines)
#    pathLen = dict([(var, db.by_col(var)) for var in db.header])
#    db.close()    
#    pathLen = pd.DataFrame(pathLen)
#    pathLen.columns = map(str.upper, pathLen.columns)
#    pathLen = pathLen[['COMID', 'PATHLENGTH']]
#        #Merge data frames and convert to Numpy array
#    arr = pd.merge(arr, pathLen, left_on='COMID', right_on='COMID')
#    arr = np.array(arr) #Convert to numpy array   
#    arr = np.c_[arr, np.zeros((arr.shape[0],5))] #Add 0s columns after Cat results
#    arr = np.nan_to_num(arr) #Convert NaN to 0    
#    
#     
#    for com in COMIDs:
#        com = 1861424
#        loc = np.where(arr[:,0] == com) #Get row location in arr        
#        UpComs = np.array(FullCOMs[com]) #Make np.array from dictionary  
#        adjDist = arr[loc, 6]
#        
#        if UpComs.size > 1:            
#            UpComs = UpComs[UpComs <> com] #Remove focal catchment from list (to get Up only)            
#            tmparr =  arr[np.in1d(arr[:,0], UpComs, assume_unique=True)] #Select subset of arr that match up COMIDs 
#            if wt_type='negexp':
#                tmparr[:, 6] = np.exp(-((tmparr[:, 6] - adjDist) / 4))
#                arr[loc, 7] = np.sum(tmparr[:, 1] * tmparr[:, 6])  
#                arr[loc, 10] = np.sum(tmparr[:, 4] * tmparr[:, 6]) 
#                arr[loc, 11] = np.sum(tmparr[:, 5] * tmparr[:, 6])
#                
#                #Calculate UpCatAreaSqKm(6), UpCatCount(9), and UpCatSum(10)   
#            arr[loc, 7] = np.sum(tmparr[:, 1])  
#            arr[loc, 10] = np.sum(tmparr[:, 4]) 
#            arr[loc, 11] = np.sum(tmparr[:, 5])
#            try: #Calculate UpCatMean with CatAreaSqKm & CatPctFull as weights
#                arr[loc, 7] = np.average(tmparr[:, 2], weights = (tmparr[:, 1] * tmparr[:, 3]))
#            except: 
#                arr[loc, 7] = 0            
#            try: #Calculate UpCatPctFull with CatAreaSqKm as weight
#                arr[loc, 8] = np.average(tmparr[:, 3], weights = tmparr[:, 1])
#            except:
#                arr[loc, 8] = 0
#        
#    outDF = pd.DataFrame(arr)    
#    print "!!!!elapsed time!!!!! " + str(datetime.now()-startfunTime)
#    outDF.columns = ['COMID','CatAreaSqKm','CatMean','CatPctFull','CatCount','CatSum','UpCatAreaSqKm','UpCatMean','UpCatPctFull','UpCatCount','UpCatSum']
#      #convert COMID to integer and then string  
#    outDF['COMID'] = outDF['COMID'].astype(int).astype(str) 
#        #Accumulation = "D:/SSWR/Phase1/SpatialPredFunc/tester.csv"    
#    outDF.to_csv(Accumulation, sep=",", index=False)
    
    
    
    
    
    
   





