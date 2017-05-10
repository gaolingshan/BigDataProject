import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import holidays
import re
import warnings
warnings.filterwarnings("ignore")
from sklearn.neighbors import KDTree
from datetime import datetime
from datetime import date
from collections import Counter

'''
This program cleans the data set according to the data type/ data quality check in part I.

The input of this program is the original data set in the ".csv" format.
'''

# Load data as a data frame, meanwhile, remove columns "PARKS_NM" and
# "HADEVELOPT"

# Change the directory(file_dir) to the directory where the original data
# file is saved.
file_dir = "NYPD_Complaint_Data_Historic.csv"
with open(file_dir) as t:
    ncols = len(t.readline().split(','))
df = pd.read_csv(file_dir,
                 usecols=[i for j in (range(17), range(19, ncols)) for i in j])

# Clean nan values in 'CMPLNT_FR_DT', 'BORO_NM', 'ADDR_PCT_CD', 'Latitude'
# and 'Longitude'
for col in ['CMPLNT_FR_DT', 'RPT_DT', 'BORO_NM', 'ADDR_PCT_CD', 'Latitude', 'Longitude']:
    df.dropna(subset=[col], inplace=True)


def CMPLNT_DT_label(Val):
    '''
    identify missing value and invalid value in 'CMPLNT_FR_DT', 'CMPLNT_TO_DT'.
    A valid date in between 01/01/1900 to 12/31/2015
    '''
    if (int(Val[-4:]) > 2015) or (int(Val[-4:]) < 1900):
        label = 'INVALID'
    elif (int(Val[0:2]) > 12) or (int(Val[0:2]) < 1):
        label = 'INVALID'
    elif (int(Val[3:5]) > 31) or (int(Val[3:5]) < 1):
        label = 'INVALID'
    else:
        label = 'VALID'
    return label


def RPT_DT_label(CMPLNT_DT):
    '''
    identify missing value and invalid value in 'RPT_DT'
    A Date from 01/01/2006 to 12/31/2015
    '''
    # if CMPLNT_DT is None:
    #print (CMPLNT_DT[-4:])

    if (int(CMPLNT_DT[-4:]) > 2015 or int(CMPLNT_DT[-4:]) < 2006):
        label = 'INVALID'
    elif (int(CMPLNT_DT[0:2]) > 12 or int(CMPLNT_DT[0:2]) < 1):
        label = 'INVALID'
    elif (int(CMPLNT_DT[3:5]) > 31 or int(CMPLNT_DT[3:5]) < 1):
        label = 'INVALID'
    else:
        label = 'VALID'
    return label

# Clean date columns 'CMPLNT_FR_DT' and 'RPT_DT'
CMPLNT_FR_DT_list = []
for dt in df['CMPLNT_FR_DT']:
    CMPLNT_FR_DT_list.append(CMPLNT_DT_label(dt))
df['CMPLNT_FR_DT_Label'] = CMPLNT_FR_DT_list
df = df[df['CMPLNT_FR_DT_Label'] == 'VALID']

RPT_DT_list = []
for dt in df['RPT_DT']:
    RPT_DT_list.append(RPT_DT_label(dt))
df['RPT_DT_Label'] = RPT_DT_list
df = df[df['RPT_DT_Label'] == 'VALID']

df.drop(['CMPLNT_FR_DT_Label', 'RPT_DT_Label'], axis=1, inplace=True)

# Convert 'CMPLNT_FR_DT' to datetime
df.dropna(subset=['CMPLNT_FR_DT'], inplace=True)
df['DateTime'] = df['CMPLNT_FR_DT'].map(
    lambda x: datetime.strptime(x, "%m/%d/%Y"))
# Add holiday column. If 'CMPLNT_FR_DT' is a U.S. public holiday, then
# True, else, then False
us_holidays = holidays.UnitedStates()
df['If_Public_Holiday'] = df['DateTime'].map(lambda x: x.date() in us_holidays)
# Add weekend column. If 'CMPLNT_FR_DT' is a weekend, then True. If it is
# a weekday, then False.
# Use weekday() function in pandas, and 0 is Monday, 5 is Saturday, and 6
# is Sunday
df['If_Weekend'] = df['DateTime'].map(
    lambda x: x.weekday() == 5 or x.weekday() == 6)

# Add zip code column
# Use KDTree to map the latitude and longitude to the zipcode areas


def get_zipcode(tree, value, ziplist):
    dist, ind = tree.query(value, k=3)
    return str(int(ziplist[ind[0, 0]][0]))

ziplist = np.loadtxt("ExternalData/USZIPCODE.txt", delimiter=',')
tree = KDTree(ziplist[:, 1:], leaf_size=2)
a = np.concatenate((np.array(df['Latitude']).reshape(-1, 1),
                    np.array(df['Longitude']).reshape(-1, 1)), axis=1)

final_list = []
count = 1
for i in a:
    count += 1
    if count % 10000 == 0:
        print(count)
    final_list.append(get_zipcode(tree, i.reshape(1,-1), ziplist))
df['ZipCode'] = final_list

# Save the cleaned data file
df.to_csv("Cleaned_data_updated_zipcode.csv", index=False)
