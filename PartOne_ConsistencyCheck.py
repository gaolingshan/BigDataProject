import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from collections import Counter
from six import string_types


# Consistency checking
'''
In the original dataset, there are a few columns that have "key code" as input, and are followed by columns that have descriptions for the "key code".
This program will examine if these codes are uniquely correspond to the descriptions in the following column.

To reproduce the result, please change the variable "df" to the dictory where the data file is saved.
'''

df = pd.read_csv("NYPD_Complaint_Data_Historic.csv")
# Ideally, we should have equal unique number of "KY_CD" and "OFNS_DESC."
# Check for each unique value in "KY_CD" (classfication code),
# if it corresponds more than one unique description
for val in df['KY_CD'].unique():
    if len(df['OFNS_DESC'][df['KY_CD'] == val].value_counts()) != 1:
        print("%s:\n%s" % (val, df['OFNS_DESC'][
              df['KY_CD'] == val].value_counts()))

df['KY_CD'][df['OFNS_DESC'] == "ENDAN WELFARE INCOMP"].value_counts()

# Check for each unique value in "OFNS_DESC" (classfication description),
# if it corresponds more than one unique classification code
for val in df['OFNS_DESC'].unique():
    # if not a nan
    if isinstance(val, string_types):
        # if more than 1 "KY_CD" code correspond to the class description
        if len(df['KY_CD'][df['OFNS_DESC'] == val].value_counts()) != 1:
            print('--------------------------------------')
            # print the count of different codes correpond to this class
            # description
            print("%s:\n%s" %
                  (val, df['KY_CD'][df['OFNS_DESC'] == val].value_counts()))
            # check if for these codes, they only have this class description.
            # If yes, then the two classes can be merged.
            for code in df['KY_CD'][df['OFNS_DESC'] == val].unique():
                print(df['OFNS_DESC'][(df['KY_CD'] == code)].unique())
