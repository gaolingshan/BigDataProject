import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import holidays
import re
from sklearn.neighbors import KDTree
from datetime import datetime
from datetime import date
from collections import Counter

# The input of this program is the output of the "clean.py". Please make sure to have "Cleaned_data_updated_zipcode.csv" in place before running this program.
# Please change the file directory as needed
file_dir = 'Cleaned_data_updated_zipcode.csv'
df = pd.read_csv(file_dir)
df['DateTime'] = df['CMPLNT_FR_DT'].map(
    lambda x: datetime.strptime(x, "%m/%d/%Y"))
df['Label'] = df['DateTime'].map(lambda x: (x.year < 2016) & (x.year > 2005))
sub_df = df[df['Label']].copy()
df_total = sub_df['ZipCode'].value_counts().reset_index().rename(
    columns={'index': 'ZIPCode', 'zipcode': 'count'})
df_total = df_total.set_index(['ZIPCode']).copy()
# Merge education, income, population, age and sex ratio data.
df_edu = pd.read_csv('ExternalData/Education_data.csv', index_col=0)
df_income = pd.read_csv('ExternalData/Income_data.csv', index_col=0)
df_pop = pd.read_csv('ExternalData/Population_data.csv', index_col=0)
df_age_sex = pd.read_csv('ExternalData/age_sex.csv', index_col=0)

df_total = df_total.merge(
    df_edu, how='left', left_index=True, right_index=True)
df_total = df_total.merge(df_income, how='left',
                          left_index=True, right_index=True)
df_total = df_total.merge(
    df_pop, how='left', left_index=True, right_index=True)
df_total = df_total.merge(df_age_sex, how='left',
                          left_index=True, right_index=True)

# Sort by index
df_total.sort_index(inplace=True)
df_total.fillna(inplace=True, method='ffill')
# Save the output as "Ten_Year_Summary.csv" or the user may update the
# name as needed
df_total.reset_index().to_csv('Ten_Year_Summary.csv', index=False)
