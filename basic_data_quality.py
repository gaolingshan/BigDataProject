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


    def read_hdfs_csv(sqlContext, filename, header='true'):
        csvreader = (sqlContext.read.format('com.databricks.spark.csv').options(header = header, inferschema='true'))
        return csvreader.load(filename)

    def write_hdfs_csv(df, filename):
        csvwriter = (df.write.format('com.databricks.spark.csv').options(header='true'))
        return csvwriter.save(filename)
    
    data = read_hdfs_csv(sqlContext, 'NYPD_Complaint_Data_Historic.csv')


    columns = pd.DataFrame(list(data.columns.values))
    data_types = pd.DataFrame(data.dtypes, columns=["Data Type"])
    missing_data_counts = pd.DataFrame(data.isnull().sum(), columns=['Missing Values'])
    present_data_counts = pd.DataFrame(data.count(), columns=['Present Values'])
    unique_value_counts = pd.DataFrame(columns=['Unique Values'])
    for v in list(data.columns.values):
        unique_value_counts.loc[v]=[data[v].nunique()]

    
    data_quality_report = data_types.join(present_data_counts).join(missing_data_counts).join(unique_value_counts)
    

    f = open('basic_data_quality_report.txt', 'w')
    print "Data Quality Report" 
    print "Total records: {}".format(len(data.index))
    print data_quality_report 
    f.close()
    
    sc.stop()