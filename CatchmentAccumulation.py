# Author: Tad Larsen
# Date: June 29, 2012
# Modified by : Marc Weber
# Date: August 8, 2013
# ArcGIS 10.1, Python 2.7
# Modified by : Marc Weber
# Date: March 4, 2014 - October1 1, 2014
# ArcGIS 10.2.1, Python 2.7

# Import system modules
import sys, os, shutil
import struct, decimal, itertools
import shapefile
from collections import  defaultdict, OrderedDict
from datetime import datetime
sys.path.append('K:/Watershed Integrity Spatial Prediction/Scipts')
from SpatialPredictionFunctions import dbfreader, children, ContinuousAllocation, CategoricalAllocation, CountAllocation

#####################################################################################################################
# Variables
inputs = OrderedDict([('10U','MS'),('10L','MS'),('07','MS'),('11','MS'),('06','MS'),('05','MS'),('08','MS'),\
                      ('01','NE'),('02','MA'),('03N','SA'),('03S','SA'),('03W','SA'),('04','GL'),('09','SR'),\
                      ('12','TX'),('13','RG'),('14','CO'),('15','CO'),('16','GB'),('17','PN'),('18','CA')])
#inputs = OrderedDict([('17','PN')])
#accum_type = raw_input('Is data Continuous, Count, or Categorical?')
#NHD_dir = raw_input('Enter the directory path for NHDPlus:')
#Alloc_dir = raw_input('Enter directory for NHDPlus catchment allocations:')
#Accum_dir = raw_input('Enter directory to output catchment accumulation results:')
#landscape_var = raw_input('Enter the allocation file name without extension and without hydroregion name (must be a csv file):')

accum_type = 'Count'
NHD_dir = 'E:/NHDPlusV21'
Alloc_dir = 'L:/Priv/CORFiles/Geospatial_Library/Data/Project/SSWR1.1B/CatResults'
Accum_dir = 'L:/Priv/CORFiles/Geospatial_Library/Data/Project/SSWR1.1B/AccumulationResults'
landscape_var = 'NPDESlnbf100_Ws'

interVPUs = {'14':'15','06':'05','05':'08','10U':'10L','10L':'07','07':'08','11':'08'}

#####################################################################################################################

startTime = datetime.now()

for reg in inputs:
    UpCOMs = defaultdict(list)
    hydroregion = inputs[reg]
    Coastal = []
    zone = reg
    flowtable= NHD_dir + "/NHDPlus%s/NHDPlus%s/NHDPlusAttributes/PlusFlow.dbf"%(hydroregion, zone)
    flowlines = NHD_dir + "/NHDPlus%s/NHDPlus%s/NHDSnapshot/Hydrography/NHDFlowline.dbf"%(hydroregion, zone)
    Allocation = "%s/%s%s.csv"%(Alloc_dir,landscape_var,zone)
    Accumulation = "%s/%s%s.csv"%(Accum_dir,landscape_var,zone)
    if not os.path.exists(Accumulation):
        print "working on zone " + zone
        infile = shapefile.Reader(flowlines)
        data = infile.records()      
        
        for i in range(0,len(data),1):
            COMID=data[i][0]
            FTYPE=data[i][9]
            if FTYPE=='Coastline':
                Coastal.append(COMID)
                
        infile = open(flowtable, 'rb')        
        data = list(dbfreader(infile))
        infile.close()

        print "Make dictionary from dbf list."
        startfunTime = datetime.now()

        for line in data[2:]:
            FROMCOMID=line[0]
            TOCOMID=line[3]
            if FROMCOMID==0:
                UpCOMs[TOCOMID] = []
            if FROMCOMID not in Coastal and TOCOMID != 0 and FROMCOMID!=0:
                UpCOMs[TOCOMID].append(FROMCOMID)
            if zone == '08': #this is a fix for flowline inter-VPU connections that had no corresponding catchments
                UpCOMs[9272756].append(11795899)
                UpCOMs[9277950].append(11802443)
                UpCOMs[15326482].append(22845301)
                UpCOMs[15332018].append(22850037)
            if zone == '15': #this is a fix for flowline inter-VPU connections that had no corresponding catchments
                UpCOMs[20734041].append(18267741)
        print "elapsed time " + str(datetime.now()-startfunTime)

        print "Make full upstream dictionary from previous dictionary."
        startfunTime = datetime.now()
        FullCOMs = dict()
        for key in UpCOMs.keys():
            FullCOMs[key] = children(key, UpCOMs)

        print "elapsed time " + str(datetime.now()-startfunTime)

        startfunTime = datetime.now()
        print "run allocation"
        if accum_type == 'Continuous':
            ContinuousAllocation(Allocation, Accumulation, FullCOMs)
        elif accum_type == 'Categorical':
            CategoricalAllocation(Allocation, Accumulation, FullCOMs)
        elif accum_type == 'Count':
            CountAllocation(Allocation, Accumulation, FullCOMs)
        print "elapsed time " + str(datetime.now()-startfunTime)

        #fixes for inter-VPU issues in NHDPlus
        print "fixing inter-VPU issues " + str(datetime.now()-startfunTime)
        if zone in interVPUs.keys():
                throughVPUs = dict()
                toVPUs = dict()
                infile=open(Accumulation)
                infile.readline()
                data=infile.readlines()
                infile.close()
                for line in data:
                    line=line.replace("\n","").split(",")
                    ID = str(line[0])
                    if ID in ['18267741','20734037','1861888','1862004','1862014','1844789','7227390','11764402','6018266','5093446','11795911','11795899',
                              '11800815','14320629','22845301','22850037','22850051','22850075','941140164','22850577','22850037']:
                        if accum_type == 'Categorical': #Need to modify below for different length categorical variables...
                                throughVPUs[ID] = []
                                line = map(float, line)
                                catch_full_index = (len(line)-1)/2
                                tot_area = float(line[1]) + float(line[len(line)/2+1])
                                for i in range(2, catch_full_index):
                                    throughVPUs[ID].append(line[i] + line[catch_full_index+i])
                                if tot_area > 0:
                                    throughVPUs[ID].append(int(round((sum(throughVPUs[ID])*1e-06) / tot_area)* 100))
                                throughVPUs[ID].insert(0,tot_area)
                        elif accum_type == 'Continuous':
                            throughVPUs[ID] = []
                            # get the area weighted average of both mean value and pct full to pass back to zonal stats for downstream VPU
                            throughVPUs[ID].append(float(line[1]) + float(line[6]))
                            if float(line[4]) + float(line[9]) != 0:
                                throughVPUs[ID].append((float(line[5]) + float(line[10])) / (float(line[4]) + float(line[9])))
                            else:
                                throughVPUs[ID].append(0.0)
                            throughVPUs[ID].append(((float(line[1]) * (float(line[3]) / 100)) + (float(line[6]) * (float(line[8]) / 100))) /(float(line[1]) + float(line[6]))*100)
                            throughVPUs[ID].append((float(line[4]) + float(line[9])))
                            throughVPUs[ID].append(float(line[5]) + float(line[10]))
                        elif accum_type == 'Count':
                            throughVPUs[ID] = []
                            # get the area weighted average of both mean value and pct full to pass back to zonal stats for downstream VPU
                            throughVPUs[ID].append(float(line[1]) + float(line[5]))
                            throughVPUs[ID].append(float(line[2]) + float(line[6]))
                            throughVPUs[ID].append(throughVPUs[ID][1] / throughVPUs[ID][0])
                            throughVPUs[ID].append(round(int((((float(line[1]) * (float(line[4])))+ (float(line[5]) * (float(line[8])))) /(float(line[1]) + float(line[5]))))))
                # Have to do this to deal with the way the Tennesse River enters the Ohio between regions 6 and 5
                if zone == '06':
                    if throughVPUs.has_key('1862004') and accum_type == 'Continuous':
                        throughVPUs['1862004'][3] = throughVPUs['1862004'][3] - throughVPUs['1862014'][3] #Adjust Sum
                        throughVPUs['1862004'][4] = throughVPUs['1862004'][4] - throughVPUs['1862014'][4] #Adjust Count
                        throughVPUs['1862004'][0] = throughVPUs['1862004'][0] - throughVPUs['1862014'][0] #Adjust Area
                    elif throughVPUs.has_key('1862004') and accum_type == 'Count':
                        throughVPUs['1862004'][0] = throughVPUs['1862004'][0] - throughVPUs['1862014'][0] #Adjust Area
                        throughVPUs['1862004'][1] = throughVPUs['1862004'][1] - throughVPUs['1862014'][1] #Adjust Count
                        throughVPUs['1862004'][2] = throughVPUs['1862004'][1] / throughVPUs['1862014'][0] #Adjust Mean
                    elif throughVPUs.has_key('1862004') and accum_type == 'Categorical':
                        for index, item in enumerate(throughVPUs['1862004']):
                            throughVPUs['1862004'][index] = throughVPUs['1862004'][index] -throughVPUs['1862014'][index]
    
                if zone == '11':
                    infile=open("%s/%s%s.csv"%(Alloc_dir,landscape_var,interVPUs[zone]),'r')
                    infile.readline()
                    data=infile.readlines()
                    infile.close()
                    for line in data:
                        line=line.replace("\n","").split(",")
                        checkCOM = str(line[0])
                        if checkCOM in ['15334480','25827824']:
                            if accum_type == 'Categorical': #Need to modify below for different length categorical variables...
                                toVPUs[checkCOM] = []
                                line = map(float, line)
                                catch_full_index = len(line)
                                area = float(line[1])
                                for i in range(2, catch_full_index):
                                    toVPUs[checkCOM].append(line[i])
                                toVPUs[checkCOM].insert(0,area)
                            elif accum_type == 'Continuous':
                                toVPUs[checkCOM] = []
                                # get the area weighted average of both mean value and pct full to pass back to zonal stats for downstream VPU
                                toVPUs[checkCOM].append(float(line[1]))
                                toVPUs[checkCOM].append(float(line[2]))
                                toVPUs[checkCOM].append(float(line[3]))
                                toVPUs[checkCOM].append(float(line[4]))
                                toVPUs[checkCOM].append(float(line[9]))
                            elif accum_type == 'Count':
                                toVPUs[checkCOM] = []
                                # get the area weighted average of both mean value and pct full to pass back to zonal stats for downstream VPU
                                toVPUs[checkCOM].append(float(line[1]))
                                toVPUs[checkCOM].append(float(line[2]))
                                toVPUs[checkCOM].append(float(line[3]))
                                toVPUs[checkCOM].append(float(line[3]))
                    # Two reaches within 8 that need to have returns from 11 subtracted out
                    if toVPUs.has_key('15334480') and accum_type == 'Continuous':
                        toVPUs['15334480'][3] = toVPUs['15334480'][3] - throughVPUs['22850037'][3] #Adjust Sum
                        toVPUs['15334480'][4] = toVPUs['15334480'][4] - throughVPUs['22850037'][4] #Adjust Count
                        toVPUs['15334480'][0] = toVPUs['15334480'][0] - throughVPUs['22850037'][0] #Adjust Area
                    elif toVPUs.has_key('15334480') and accum_type == 'Count':
                        toVPUs['15334480'][0] = toVPUs['15334480'][0] -  throughVPUs['22850037'][0] #Adjust Area
                        toVPUs['15334480'][1] = toVPUs['15334480'][1] - throughVPUs['22850037'][1] #Adjust Count
                        toVPUs['15334480'][2] = toVPUs['15334480'][1] / toVPUs['15334480'][0] #Adjust Mean
                    elif toVPUs.has_key('15334480') and accum_type == 'Categorical':
                        for index, item in enumerate(toVPUs['15334480'][0:-1]):
                            toVPUs['15334480'][index] = toVPUs['15334480'][index] - throughVPUs['22850037'][index]
                    # and second adjustment to return within region 8:
                    if toVPUs.has_key('25827824') and accum_type == 'Continuous':
                        toVPUs['25827824'][3] = toVPUs['25827824'][3] - throughVPUs['11795899'][3] #Adjust Sum
                        toVPUs['25827824'][4] = toVPUs['25827824'][4] - throughVPUs['11795899'][4] #Adjust Count
                        toVPUs['25827824'][0] = toVPUs['25827824'][0] - throughVPUs['11795899'][0] #Adjust Area
                    elif toVPUs.has_key('25827824') and accum_type == 'Count':
                        toVPUs['25827824'][0] = toVPUs['25827824'][0] -  throughVPUs['11795899'][0] #Adjust Area
                        toVPUs['25827824'][1] = toVPUs['25827824'][1] - throughVPUs['11795899'][1] #Adjust Count
                        toVPUs['25827824'][2] = toVPUs['25827824'][1] / toVPUs['25827824'][0] #Adjust Mean
                    elif toVPUs.has_key('25827824') and accum_type == 'Categorical':
                        for index, item in enumerate(toVPUs['25827824'][0:-1]):
                            toVPUs['25827824'][index] = toVPUs['25827824'][index] - throughVPUs['11795899'][index]
                            
                    # Have to do this to deal with the way two channels enter from 11 to 8
                    if throughVPUs.has_key('22850037') and accum_type == 'Continuous':
                        throughVPUs['22850037'][3] = throughVPUs['22850037'][3] - throughVPUs['22845301'][3] #Adjust Sum
                        throughVPUs['22850037'][4] = throughVPUs['22850037'][4] - throughVPUs['22845301'][4] #Adjust Count
                        throughVPUs['22850037'][0] = throughVPUs['22850037'][0] - throughVPUs['22845301'][0] #Adjust Area
                    elif throughVPUs.has_key('22850037') and accum_type == 'Count':
                        throughVPUs['22850037'][0] = throughVPUs['22850037'][0] - throughVPUs['22845301'][0] #Adjust Area
                        throughVPUs['22850037'][1] = throughVPUs['22850037'][1] - throughVPUs['22845301'][1] #Adjust Count
                        throughVPUs['22850037'][2] = throughVPUs['22850037'][1] / throughVPUs['22850037'][0] #Adjust Mean
                    elif throughVPUs.has_key('22850037') and accum_type == 'Categorical':
                        for index, item in enumerate(throughVPUs['22850037'][0:-1]):
                            throughVPUs['22850037'][index] = throughVPUs['22850037'][index] -throughVPUs['22845301'][index]
                    # Have to do this to deal with the way side channel enters Arkansas from 11 to 8
                    if throughVPUs.has_key('22850051') and accum_type == 'Continuous':
                        throughVPUs['22850051'][3] = throughVPUs['22850051'][3] - throughVPUs['22850577'][3] #Adjust Sum
                        throughVPUs['22850051'][4] = throughVPUs['22850051'][4] - throughVPUs['22850577'][4] #Adjust Count
                        throughVPUs['22850051'][0] = throughVPUs['22850051'][0] - throughVPUs['22850577'][0] #Adjust Area
                    elif throughVPUs.has_key('22850051') and accum_type == 'Count':
                        throughVPUs['22850051'][0] = throughVPUs['22850051'][0] -  throughVPUs['22850577'][0] #Adjust Area
                        throughVPUs['22850051'][1] = throughVPUs['22850051'][1] - throughVPUs['22850577'][1] #Adjust Count
                        throughVPUs['22850051'][2] = throughVPUs['22850051'][1] / throughVPUs['22850051'][0] #Adjust Mean
                    elif throughVPUs.has_key('22850051') and accum_type == 'Categorical':
                        for index, item in enumerate(throughVPUs['22850051'][0:-1]):
                            throughVPUs['22850051'][index] = throughVPUs['22850051'][index] - throughVPUs['22850577'][index]
    
                    # Get rid of COMIDs 15334480 and 25827824 in region 8 zonal table so we can append modified version back in
                    # rename original file 'orig'
                    shutil.copy("%s/%s%s.csv"%(Alloc_dir,landscape_var,interVPUs[zone]),"%s/%s%s_original.csv"%(Alloc_dir, landscape_var,interVPUs[zone]))
                    f = open("%s/%s%s.csv"%(Alloc_dir,landscape_var,interVPUs[zone]),"r")
                    lines=f.readlines()
                    f.close()
                    f=open("%s/%s%s.csv"%(Alloc_dir,landscape_var,interVPUs[zone]),"w")
                    for line in lines:
                        test=line.replace("\n","").split(",")
                        if not test[0] in ['15334480','25827824']:
                            f.write(line)
                    f.close()
    
                if '22850577' in throughVPUs.keys():
                    del throughVPUs['22850577'] # get rid of COMID 22850577 dictionary element - only there to subtract from 22850051, don't want to pass to region 8
                if '1862014' in throughVPUs.keys():
                    del throughVPUs['1862014'] # get rid of COMID 1862014 dictionary element - only there to subtract from 1862004, don't want to pass to region 6
                checks = []
                infile=open("%s/%s%s.csv"%(Alloc_dir,landscape_var,interVPUs[zone]),'r')
                infile.readline()
                data=infile.readlines()
                infile.close()
                for line in data:
                    line=line.replace("\n","").split(",")
                    checkID = str(line[0])
                    checks.append(checkID)
    
                outfile=open("%s/%s%s.csv"%(Alloc_dir,landscape_var,interVPUs[zone]),'a')
                alreadyadded = set(checks).intersection(set(throughVPUs.keys()))
                for IDs in throughVPUs:
                    if not IDs in alreadyadded:
                        if accum_type == 'Categorical':
                            outString = ','.join(map(str,throughVPUs[IDs]))
                            outString = str(IDs) + ',' + outString + '\n'
                            outfile.write(outString)
                        elif accum_type =='Continuous':
                            outfile.write("%s,%f,%f,%i,%i,0,0,0,0,%f\n"%(IDs,float(throughVPUs[IDs][0]),float(throughVPUs[IDs][1]),
                                                                         int(throughVPUs[IDs][2]),int(throughVPUs[IDs][3]),float(throughVPUs[IDs][4])))
                        elif accum_type =='Count':
                            outfile.write("%s,%f,%f,%i,%i\n"%(IDs,float(throughVPUs[IDs][0]),float(throughVPUs[IDs][1]),
                                                                         int(throughVPUs[IDs][2]),int(throughVPUs[IDs][3])))
                for IDs in toVPUs:
                    if not IDs in alreadyadded:
                        if accum_type == 'Categorical':
                            outString = ','.join(map(str,toVPUs[IDs]))
                            outString = str(IDs) + ',' + outString + '\n'
                            outfile.write(outString)
                        elif accum_type =='Continuous':
                            outfile.write("%s,%f,%f,%i,%i,0,0,0,0,%f\n"%(IDs,float(toVPUs[IDs][0]),float(toVPUs[IDs][1]),
                                                                         int(toVPUs[IDs][2]),int(toVPUs[IDs][3]),float(toVPUs[IDs][4])))
                        elif accum_type =='Count':
                            outfile.write("%s,%f,%f,%i,%i\n"%(IDs,float(toVPUs[IDs][0]),float(toVPUs[IDs][1]),
                                                                         int(toVPUs[IDs][2]),int(toVPUs[IDs][3])))
                outfile.close()
print "total elapsed time " + str(datetime.now()-startTime)
