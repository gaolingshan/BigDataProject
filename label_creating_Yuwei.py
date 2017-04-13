#!/usr/bin/env python

import sys
import numpy as np
import pandas as pd
import itertools
from math import sqrt
from operator import add
from os.path import join, isfile, dirname
from pyspark import SparkConf, SparkContext
from csv import reader


if __name__ == "__main__":

    sc = SparkContext()
    sqlContext = HiveContext(sc)

    # read each line and split with csv reader
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x:reader(x))


    # or you can also choose read csv file into DataFrame directly
    '''
    def read_hdfs_csv(sqlContext, filename, header='true'):
        csvreader = (sqlContext.read.format('com.databricks.spark.csv').options(header = header, inferschema='true'))
        return csvreader.load(filename)

    def write_hdfs_csv(df, filename):
        csvwriter = (df.write.format('com.databricks.spark.csv').options(header='true'))
        return csvwriter.save(filename)
    
    df = read_hdfs_csv(sqlContext, 'NYPD_Complaint_Data_Historic.csv')
    '''
    
    labels = {'VALID', 'INVALID', 'NULL'}
    
    def CMPLNT_NUM_label(CMPLNT_NUM):
        '''
        identify missing value and invalid value in 'CMPLNT_NUM' 
        A unique length = 9 int
        '''
        if CMPLNT_NUM is None:
            label = 'NULL'

        elif len(CMPLNT_NUM) == 9：
            label = 'VALID'
        else:
            label = 'INVALID'

        return label

    def CMPLNT_DT_label(CMPLNT_DT):
        '''
        identify missing value and invalid value in 'CMPLNT_FR_DT', 'CMPLNT_TO_DT', and 'RPT_DT'
        A Date until-12/31/2015
        '''
        if CMPLNT_DT is None:
            label = 'NULL'
        elif re.match('[0-9]{2}/[0-9]{2}/[0-9]{4}', CMPLNT_DT) is None:
            label = 'INVALID'
        else:
            label = 'VALID'
        return label
    
    def CMPLNT_TM_label(CMPLNT_TM):
        '''
        identify missing value and invalid value in 'CMPLNT_FR_TM' and 'CMPLNT_TO_TM' 
        A time between 00:00:00-23:59:59
        '''
        if CMPLNT_TM is None:
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
        
        if CD is None:
            label = 'NULL'
        elif 100 < CD < 999:
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
        
        if CRM_ATPT_CPTD is None:
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
        
        if LAW_CAT is None:
            label = 'NULL'
        elif LAW_CAT in VALUES:
            label = 'VALID'
        else:
            label = 'INVALID'
        return label


    def BORO_NM_label(data):
        '''
        identify missing value and invalid value in 'BORO_NM'
        {'BRONX', 'QUEENS', 'MANHATTAN', 'BROOKLYN', 'STATEN ISLAND'}
        '''

        return label

    def ADDR_PCT_CD_label(data):
        '''
        identify missing value and invalid value in 'ADDR_PCT_CD'
        A length <3 int after transforming float to int
        '''

        return label

    def LOC_OF_OCCUR_DESC_label(data):
        '''
        identify missing value and invalid value in 'LOC_OF_OCCUR_DESC'
        {['INSIDE', 'OUTSIDE','FRONT OF', 'OPPOSITE OF', 'REAR OF']}
        '''

        return label

    def other_label(data):
        '''
        identify missing value and invalid value in 'OFNS_DESC','PD_DESC','JURIS_DESC', 'PREM_TYP_DESC','PARKS_NM', 'HADEVELOPT '
        if is not missing value, then it is valid. 
        '''

        return label

    def Lat_Lon_label(data):
        '''
        identify missing value and invalid value in 'Lat_Lon'
        if is not in NYC, then it is invalid
        '''

        return label



    #TODO: for each column EXCEPT(X_COORD_CD, Y_COORD_CD, Latitude Longitude), create a new column, which claims a label for each cell

    #TODO: create a brief summary for each new column, how many invalid labels in each column

    #output: a new csv with only new columns
    #output: a summary of new csv(summary can be achieved in another new program)


