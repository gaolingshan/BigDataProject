    
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

    def DESC_label(data):
        '''
        identify missing value and invalid value in 'OFNS_DESC' and 'PD_DESC'
        For each description, if the corresonding KY_CD or PD_CD has multi-value, then the majority is valid, the rest is invalid 
        '''

        return label



      TODO: create a report towards consistent