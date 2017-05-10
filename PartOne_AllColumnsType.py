import sys
import numpy as np
import pandas as pd
import itertools
from math import sqrt
from operator import add
from os.path import join, isfile, dirname
from pyspark import SparkConf, SparkContext
from csv import reader
import re
from pyspark.sql import SQLContext

def semantic_type(number):
	Semantic_type_dictionary = {0:"CASE ID", 1: "Start Date", 2: "Start Time", 3: "End Date", 4: "End Time", 5: "Report Date", 6:"KY ID", 7:"KY DESCRIPTION",
	8: "PD ID", 9: "PD DESCRIPTION", 10: "LEVEL", 11: "LAW CATEGORY", 12: "JURIS DESCRIPTION", 13: "BOROUGH", 14: "PRECINCT", 15:"LOCATION", 16:"PREMISE", 17:"PARK",
	18: "HOUSE", 19: "X COORDINATE", 20: "Y COORDINATE", 21: "LAT  COORDINATE", 22: "LON COORDINATE", 23 : "LAT LON COORDINATE"}
	return Semantic_type_dictionary[number]


def data_type(data):
	if re.match("^\d+?\.\d+?$", data) is not None:
	    return "FLOAT"
	    #print ("FLOAT")
	elif re.match("^\s*-?[0-9]{1,10}\s*$", data) is not None: 
	    return "INT"
	    #print ("INT")
	elif re.match('^(([0-1]?[0-9])|([2][0-3])):([0-5]?[0-9]):([0-5]?[0-9])$', data) is not None or re.match('[0-9]{2}/[0-9]{2}/[0-9]{4}', data) is not None:
	    return "DateTime"
	    #print("DateTime")
	else:
        return "TEXT"
	    #print("TEXT")


## label for 0 ###
def CMPLNT_NUM_label(CMPLNT_NUM):
    '''
    identify missing value and invalid value in 'CMPLNT_NUM' 
    A unique length = 9 int
    '''
    if (CMPLNT_NUM) is None or (CMPLNT_NUM == ''):
        label = 'NULL'
    elif len(CMPLNT_NUM) == 9:
        label = 'VALID'
    else:
        label = 'INVALID'
    return label


def CMPLNT_DT_label(CMPLNT_DT):
    '''
    identify missing value and invalid value in 'CMPLNT_FR_DT', 'CMPLNT_TO_DT', and 'RPT_DT'
    A Date until-12/31/2015
    '''
    # if CMPLNT_DT is None:
    # please take a look here
    # Zizhuo Ren revised here
    if (CMPLNT_DT is None) or (CMPLNT_DT == ''):
        label = 'NULL'
    elif re.match('^19\d{2}|201[0-5]$', CMPLNT_DT) is None:
        label = 'INVALID'
    else:
        label = 'VALID'
    return label

def CMPLNT_RPT_DT_label(CMPLNT_DT):
    '''
    identify missing value and invalid value in 'RPT_DT'
    A Date from 01/01/2006 to 12/31/2015
    '''

    if (CMPLNT_DT is None) or (CMPLNT_DT == ''):
        label = 'NULL'
    elif re.match('^200[6-9]|201[0-5]$', CMPLNT_DT) is None:
        label = 'INVALID'
    else:
        label = 'VALID'
    return label

## label for 2
def CMPLNT_TM_label(CMPLNT_TM):
    '''
    identify missing value and invalid value in 'CMPLNT_FR_TM' and 'CMPLNT_TO_TM' 
    A time between 00:00:00-23:59:59
    '''

    if (CMPLNT_TM is None) or (CMPLNT_TM == ''):
        label = 'NULL'
    elif re.match('^(([0-1]?[0-9])|([2][0-3])):([0-5]?[0-9]):([0-5]?[0-9])$', CMPLNT_TM) is None:
        label = 'INVALID'
    else:
        label = 'VALID'
    return label

def CD_label(CD):
    '''
    identify missing value and invalid value in 'KY_CD' and 'PD_CD'
    A length = 3 number after transforming float to int 
    '''
    if CD == '':
        label = 'NULL'
    #elif 100 < int(CD) < 999:
    #Zizhuo Ren revised here
    elif re.match("^\s*-?[0-9]{1,10}\s*$", CD) is not None and len(CD) == 3:
        label = 'VALID'
    else:
        label = 'INVALID'
    return label

def other_label(data):
	#Zizhuo Ren: question about JURIS_DESC#
    '''
    identify missing value and invalid value in 'OFNS_DESC','PD_DESC','JURIS_DESC', 'PREM_TYP_DESC','PARKS_NM', 'HADEVELOPT '
    if is not missing value, then it is valid. 
    '''
    # Zizhuo Ren revised here 
    try:
        basestring
    except NameError:
        basestring = str
    if (data is None) or (data == ''):
        label = 'NULL'
    elif isinstance(data, basestring):
        label = 'VALID'
    else:
        label = 'INVALID'
    return label

def BORO_NM_label(data):
    '''
    identify missing value and invalid value in 'BORO_NM'
    {'BRONX', 'QUEENS', 'MANHATTAN', 'BROOKLYN', 'STATEN ISLAND'}
    '''
    boro_dict = ['BRONX', 'QUEENS','MANHATTAN','BROOKLYN', 'STATEN ISLAND']
    if (data is None) or (data == ''):
        label = 'NULL'
    elif data in boro_dict:
        label = 'VALID'
    else:
        label = 'INVALID'
    return label

def CRM_ATPT_CPTD_CD_label(CRM_ATPT_CPTD):
    '''
    identify missing value and invalid value in 'CRM_ATPT_CPTD_CD'
    {'COMPLETED','ATTEMPTED'} 
    '''
    VALUES = ['COMPLETED','ATTEMPTED']
    
    if (CRM_ATPT_CPTD is None) or (CRM_ATPT_CPTD == ''):
        label = 'NULL'
    elif CRM_ATPT_CPTD in VALUES:
        label = 'VALID'
    else:
        label = 'INVALID'
    return label

def LAW_CAT_CD(LAW_CAT):
    '''
    identify missing value and invalid value in 'LAW_CAT_CD'
    {'MISDEMEANOR','FELONY','VIOLATION'}
    '''
    VALUES = ['MISDEMEANOR','FELONY','VIOLATION']
    
    if (LAW_CAT is None) or (LAW_CAT == ''):
        label = 'NULL'
    elif LAW_CAT in VALUES:
        label = 'VALID'
    else:
        label = 'INVALID'
    return label

def LOC_OF_OCCUR_DESC_label(data):
    '''
    identify missing value and invalid value in 'LOC_OF_OCCUR_DESC'
    {['INSIDE', 'OUTSIDE','FRONT OF', 'OPPOSITE OF', 'REAR OF']}
    '''
    positions = ['INSIDE', 'OUTSIDE', 'FRONT OF', 'OPPOSITE OF', 'REAR OF']
    if (data is None) or (data == ''):
        label = 'NULL'
    elif data in positions:
        label = 'VALID'
    else:
        label = 'INVALID'
    return label

def ADDR_PCT_CD_label(data):
    '''
    identify missing value and invalid value in 'ADDR_PCT_CD'
    A length <3 int after transforming float to int
    '''
    if (data is None) or (data == ''):
        label = 'NULL'
    elif re.match("^\s*-?[0-9]{1,10}\s*$", data) is not None:
        if int(data) < 124:
            label = 'VALID'
        else:
            label = 'INVALID'
    else:
        label = 'INVALID' 
    return label

def coord_label(data):
	if (data is None) or (data == ''):
		label = 'NULL'
	elif re.match("^\s*-?[0-9]{1,10}\s*$", data) is not None:
		label = 'VALID'
	else:
		label = 'INVALID'
	return label

def Latitude_label(data):
	if (data is None) or (data == ''):
		label = 'NULL'
	elif re.match('^[-+]?([1-8]?\d(\.\d+)?|90(\.0+)?)$', data) is not None:
		label = 'VALID'
	else:
		label = 'INVALID'
	return label


def Longitude_label(data):
	if (data is None) or (data == ''):
		label = 'NULL'
	elif re.match('^[-+]?(180(\.0+)?|((1[0-7]\d)|([1-9]?\d))(\.\d+)?)$', data) is not None:
		label = 'VALID'
	else:
		label = 'INVALID'
	return label

def Lat_Lon_label(data):
	try:
		basestring
	except NameError:
		basestring = str
	if (data is None) or (data==''):
		label = 'NULL'
	elif isinstance(data, basestring) and (re.match('^\([-+]?([1-8]?\d(\.\d+)?|90(\.0+)?),\s*[-+]?(180(\.0+)?|((1[0-7]\d)|([1-9]?\d))(\.\d+)?)\)$', data) is not None):
		#geolocator = Nominatim()
		#location = geolocator.reverse(data).raw['address']
		#city = location['city']
		#if city == 'NYC':
		label = 'VALID'
		#else:
			#label = 'INVALID'
	else:
		label = 'INVALID'
	return label


def combine_out(data, number):
	sem_type = semantic_type(number)
	datatype = data_type(data)
	if number == 0:
		label = CMPLNT_NUM_label(data)
	elif number == 1 or number == 3:
		label = CMPLNT_DT_label(data)
	elif number == 5:
                label = CMPLNT_RPT_DT_label(data)
	elif number == 2 or number == 4:
		label = CMPLNT_TM_label(data)
	elif number == 6 or number == 8:
		label = CD_label(data)
	elif number == 7 or number == 9 or number == 12 or number == 16 or number == 17 or number == 18:
		label = other_label(data)
	elif number == 10:
		label = CRM_ATPT_CPTD_CD_label(data)
	elif number == 11:
		label = LAW_CAT_CD(data)
	elif number == 13:
		label = BORO_NM_label(data)
	elif number == 14:
		label = ADDR_PCT_CD_label(data)
	elif number == 15:
		label = LOC_OF_OCCUR_DESC_label(data)
	elif number == 19 or number == 20:
		label = coord_label(data)
	elif number == 21:
		label = Latitude_label(data)
	elif number == 22:
		label = Longitude_label(data)
	else:
		label = Lat_Lon_label(data)
	return data, datatype, sem_type, label


def write_hdfs_csv(df, filename):
    csvwriter = (df.write.format('com.databricks.spark.csv').options(header='false'))
    return csvwriter.save(filename)    

if __name__ == "__main__":


    conf = SparkConf().set('spark.executor.memory', '8g').set('spark.yarn.scheduler.maximum-allocation-mb', '131072').set("spark.yarn.executor.memoryOverhead", "2048")
    sc = SparkContext(conf = conf)
    sqlContext = SQLContext(sc)

    lines = sc.textFile("NYPD_Complaint_Data_Historic.csv")
    header = lines.first()
    lines = lines.filter(lambda line: line != header).mapPartitions(lambda x:reader(x))


    line_data = lines.map(lambda x : x[0])
    line_result = line_data.map(lambda x : combine_out(x, i))

    for i in range(24):
        line_data = lines.map(lambda x : x[i])
        line_result = line_data.map(lambda x : combine_out(x, i))
        write_hdfs_csv(line_result, "column{0}_summary.csv".format(str(i)))

