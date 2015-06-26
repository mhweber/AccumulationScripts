#--------------------------------
# Name: zonal_stats
# Purpose: run zonal stats on
#          landscape grids for
#          NHDPlus catchments
# Author: Marc Weber
# Created 7/25/2013
# Updated 10/1/2014
# ArcGIS Version:  10.2
# Python Version:  2.7
#--------------------------------
import os, sys, arcpy
from arcpy import env
from arcpy.sa import *
from datetime import datetime

#####################################################################################################################
# Settings
# Check out Spatial Analyst extension license
arcpy.CheckOutExtension("spatial")
arcpy.OverWriteOutput = 1
# Set Geoprocessing environments
# arcpy.env.resamplingMethod = "NEAREST"
arcpy.env.outputCoordinateSystem = "PROJCS['NAD_1983_Albers',GEOGCS['GCS_North_American_1983',DATUM['D_North_American_1983',SPHEROID['GRS_1980',6378137.0,298.257222101]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Albers'],PARAMETER['False_Easting',0.0],PARAMETER['False_Northing',0.0],PARAMETER['Central_Meridian',-96.0],PARAMETER['Standard_Parallel_1',29.5],PARAMETER['Standard_Parallel_2',45.5],PARAMETER['Latitude_Of_Origin',23.0],UNIT['Meter',1.0]]"
# arcpy.env.parallelProcessingFactor = "60%"
arcpy.env.pyramid = "NONE"
#processing cell size
arcpy.env.cellSize = "30"

#####################################################################################################################
# Variables
raster_dir = "L:/Priv/CORFiles/Geospatial_Library/Data/Project/SSWR1.1B/LandscapeRasters/QAComplete"
ingrid = "%s/nlcd2006.tif"%(raster_dir)
NHD_dir = "E:/NHDPlusV21"
zoneField = "FEATUREID" # for NHDPlus catchments, 'FEATUREID' is the COMID, which we want as zone field
out_dir = "E:/GIS/zonalresults/nlcd2006"
#out_dir = "C:/temp"
mask = 'True'
mask_layer = 'RipBuf100'
mask_path = "L:/Priv/CORFiles/Geospatial_Library/Data/Project/SSWR1.1B/PhaseTwo/LandscapeRasters/QAcomplete/WaterMask/"
categorical = 'True'
by_RPU = 'True'
inputs = {'CA':['18'],'CO':['14','15'],'GB':['16'],'GL':['04'],'MA':['02'],'MS':['05','06','07','08','10L','10U','11'],'NE':['01'],'PN':['17'],\
          'RG':['13'],'SA':['03N','03S','03W'],'SR':['09'],'TX':['12']}
#inputs = {'PN':['17']}          
#####################################################################################################################

startTime = datetime.now()
for regions in inputs.keys():
    if by_RPU == 'False':
        for hydro in inputs[regions]:
            print 'on region ' + regions + ' and hydro number ' + hydro
            # Set variables for zonal stats
            inZoneData = "%s/NHDPlus%s/NHDPlus%s/NHDPlusCatchment/cat"%(NHD_dir,regions,hydro)
            arcpy.env.snapRaster = inZoneData
            arcpy.env.resamplingMethod = "NEAREST"
            if mask=='True' and categorical=='True':
                if ingrid.count('.tif') or ingrid.count('.img'):
                    outTable ="%s/zonalstats_%s%s_highslope.dbf"%(out_dir,ingrid.split("/")[-1].split(".")[0],hydro)
                else:
                    outTable ="%s/zonalstats_%s%s_highslope.dbf"%(out_dir,ingrid.split("/")[-1],hydro)
                arcpy.env.mask = mask_path + mask_layer + hydro + '.tif'
                if not os.path.exists(outTable):
                    arcpy.gp.TabulateArea_sa(inZoneData, "VALUE", ingrid, "Value", outTable, "30")
            if mask=='True' and categorical!='True':
                if ingrid.count('.tif') or ingrid.count('.img'):
                    outTable ="%s/zonalstats_%s%srip100.dbf"%(out_dir,ingrid.split("/")[-1].split(".")[0],hydro)
                else:
                    outTable ="%s/zonalstats_%s%srip100.dbf"%(out_dir,ingrid.split("/")[-1],hydro)
                arcpy.env.mask = mask_path + mask_layer + hydro + '.tif'
                if not os.path.exists(outTable):
                    arcpy.gp.ZonalStatisticsAsTable_sa(inZoneData, zoneField, ingrid, outTable, "DATA", "ALL")
            if mask!='True' and categorical=='True':
                # Execute Tabulate Area
                if ingrid.count('.tif') or ingrid.count('.img'):
                    outTable ="%s/zonalstats_%s%s.dbf"%(out_dir,ingrid.split("/")[-1].split(".")[0],hydro)
                else:
                    outTable ="%s/zonalstats_%s%s.dbf"%(out_dir,ingrid.split("/")[-1],hydro)
                if not os.path.exists(outTable):
                    arcpy.gp.TabulateArea_sa(inZoneData, "VALUE", ingrid, "VALUE", outTable, "30")
            elif mask!='True' and categorical!='True':
                # Execute ZonalStatistics
                if ingrid.count('.tif') or ingrid.count('.img'):
                    outTable ="%s/zonalstats_%s%s.dbf"%(out_dir,ingrid.split("/")[-1].split(".")[0],hydro)
                else:
                    outTable ="%s/zonalstats_%s%s.dbf"%(out_dir,ingrid.split("/")[-1],hydro)
                if not os.path.exists(outTable):
                    arcpy.gp.ZonalStatisticsAsTable_sa(inZoneData, zoneField, ingrid, outTable, "DATA", "ALL")
                pass
        
    if by_RPU == 'True':
        for hydro in inputs[regions]:
            hydrodir = "%s/NHDPlus%s/NHDPlus%s"%(NHD_dir,regions, hydro)
            inZoneData = "%s/NHDPlus%s/NHDPlus%s/NHDPlusCatchment/cat"%(NHD_dir,regions,hydro)
            for subdirs in os.listdir(hydrodir):
                if subdirs.count("FdrFac") and not subdirs.count('.txt') and not subdirs.count('.7z'):
                    print 'on region ' + regions + ' and RPU ' + subdirs[-3:]
                    fdr = "%s/%s/fdr"%(hydrodir, subdirs)
                    arcpy.env.snapRaster = fdr
                    arcpy.env.resamplingMethod = "NEAREST"
                    if mask=='True' and categorical=='True':
                        if ingrid.count('.tif') or ingrid.count('.img'):
                            outTable ="%s/zonalstats_%s%s_RpBuf100.dbf"%(out_dir,ingrid.split("/")[-1].split(".")[0],subdirs[-3:])
                        else:
                            outTable ="%s/zonalstats_%s%s_RpBuf100.dbf"%(out_dir,ingrid.split("/")[-1],subdirs[-3:])
                        arcpy.env.mask = mask_path + mask_layer + '_' + subdirs[-3:] + '.tif'
                        if not os.path.exists(outTable):
                            arcpy.gp.TabulateArea_sa(inZoneData, "VALUE", ingrid, "Value", outTable, "30")
                    if mask=='True' and categorical!='True':
                        if ingrid.count('.tif') or ingrid.count('.img'):
                            outTable ="%s/zonalstats_%s%s_RpBuf100.dbf"%(out_dir,ingrid.split("/")[-1].split(".")[0],subdirs[-3:])
                        else:
                            outTable ="%s/zonalstats_%s%sRpBuf100.dbf"%(out_dir,ingrid.split("/")[-1],subdirs[-3:])
                        arcpy.env.mask = mask_path + mask_layer + '_' + subdirs[-3:] + '.tif'
                        if not os.path.exists(outTable):
                            arcpy.gp.ZonalStatisticsAsTable_sa(inZoneData, zoneField, ingrid, outTable, "DATA", "ALL")
                    if mask!='True' and categorical=='True':
                        # Execute Tabulate Area
                        if ingrid.count('.tif') or ingrid.count('.img'):
                            outTable ="%s/zonalstats_%s%s.dbf"%(out_dir,ingrid.split("/")[-1].split(".")[0],subdirs[-3:])
                        else:
                            outTable ="%s/zonalstats_%s%s.dbf"%(out_dir,ingrid.split("/")[-1],hydro)
                        if not os.path.exists(outTable):
                            arcpy.gp.TabulateArea_sa(inZoneData, "VALUE", ingrid, "VALUE", outTable, "30")
                    elif mask!='True' and categorical!='True':
                        # Execute ZonalStatistics
                        if ingrid.count('.tif') or ingrid.count('.img'):
                            outTable ="%s/zonalstats_%s%s.dbf"%(out_dir,ingrid.split("/")[-1].split(".")[0],subdirs[-3:])
                        else:
                            outTable ="%s/zonalstats_%s%s.dbf"%(out_dir,ingrid.split("/")[-1],subdirs[-3:])
                        if not os.path.exists(outTable):
                            arcpy.gp.ZonalStatisticsAsTable_sa(inZoneData, zoneField, ingrid, outTable, "DATA", "ALL")
                        pass
print "elapsed time " + str(datetime.now()-startTime)
