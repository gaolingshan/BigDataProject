import plotly.tools as tls
tls.set_credentials_file(username='gaolingshan', api_key='VtYTYiUcXzZcQ3xM5hzU')
#Note that the key may be invalid when re-run the program


import pandas as pd
import numpy as np
from IPython.display import display

import plotly.plotly as py
import plotly.graph_objs as go# interactive graphing
from plotly.graph_objs import Bar, Scatter, Marker, Layout 
from datetime import datetime
from scipy import stats  

import holidays
us_holidays = holidays.UnitedStates()

df = pd.read_csv("Cleaned_data_updated_zipcode.csv",usecols = [0,1,2,3,4,5])
new_df = pd.DataFrame(df['CMPLNT_NUM'].groupby(df['CMPLNT_FR_DT']).count().reset_index())

#transfor to DateTime type and add weekday/weekend, holiday/non holiday information

us_holidays = holidays.UnitedStates()

for i in range(len(new_df)):
    try:
        new_df.ix[i,'CMPLNT_FR_DT'] = datetime.strptime(new_df.ix[i,'CMPLNT_FR_DT'],"%m/%d/%Y")
        new_df.ix[i,'YEAR'] = new_df.ix[i,'CMPLNT_FR_DT'].year
        new_df.ix[i,'MONTH'] = new_df.ix[i,'CMPLNT_FR_DT'].month
        new_df.ix[i,'DAY'] = new_df.ix[i,'CMPLNT_FR_DT'].day
        new_df.ix[i,'If_Public_Holiday'] = new_df.ix[i,'CMPLNT_FR_DT'] in us_holidays
        new_df.ix[i,'WEEKDAY'] = new_df.ix[i,'CMPLNT_FR_DT'].weekday()
    except:
        print(new_df.ix[i,'CMPLNT_FR_DT'])



#only analyze from 2005 Jan to 2015 Dec
new_df = new_df[(new_df['YEAR'] < 2016) & (new_df['YEAR'] > 2005)]


# visualize the number of crimes per year
df_year = pd.DataFrame(new_df['CMPLNT_NUM'].groupby(new_df['YEAR']).sum().reset_index())
year = go.Scatter(
    x=df_year['YEAR'],
    y=df_year['CMPLNT_NUM'],
    name='CMPLNT_NUM per year'
)

data = [year]
layout = go.Layout(
    title='CMPLNT_NUM per year')
fig = go.Figure(data=data, layout=layout)
py.image.save_as(fig, filename='CMPLNT_NUM per year.png')


# visualize the number of crimes per month
df_month = pd.DataFrame(new_df['CMPLNT_NUM'].groupby(new_df['MONTH']).sum().reset_index())
month = go.Scatter(
    x=df_month['MONTH'],
    y=df_month['CMPLNT_NUM'],
    name='CMPLNT_NUM per month'
)

data = [month]
layout = go.Layout(
    title='CMPLNT_NUM per month')
fig = go.Figure(data=data, layout=layout)
py.image.save_as(fig, filename='CMPLNT_NUM per month.png')


# visualize the number of crimes per monthly day
df_day = pd.DataFrame(new_df['CMPLNT_NUM'].groupby(new_df['DAY']).sum().reset_index())
day = go.Scatter(
    x=df_day['DAY'],
    y=df_day['CMPLNT_NUM'],
    name='CMPLNT_NUM per day'
)

data = [day]
layout = go.Layout(
    title='CMPLNT_NUM per day')
fig = go.Figure(data=data, layout=layout)
py.image.save_as(fig, filename='CMPLNT_NUM per day.png')



# visualize the number of crimes per weekday
df_weekday = pd.DataFrame(new_df['CMPLNT_NUM'].groupby(new_df['WEEKDAY']).sum().reset_index())
df_weekday['WEEKDAY'] = ['MON', 'TUE','WED','THU','FRI','SAT','SUN']
weekday = go.Bar(
    x=df_weekday['WEEKDAY'],
    y=df_weekday['CMPLNT_NUM'],
    name='CMPLNT_NUM per weekday',
    marker=dict(
        color=['rgba(204,204,204,1)', 'rgba(204,204,204,1)','rgba(204,204,204,1)',
               'rgba(204,204,204,1)', 'rgba(204,204,204,1)',
               'rgba(222,45,38,0.8)','rgba(222,45,38,0.8)']),
)

data = [weekday]
layout = go.Layout(
    title='CMPLNT_NUM per weekday')
fig = go.Figure(data=data, layout=layout)
py.image.save_as(fig, filename='CMPLNT_NUM per weekday.png')


#calculate correlation coefficients
weekday  = new_df['CMPLNT_NUM'].groupby([new_df['WEEKDAY'],new_df['YEAR']]).sum().unstack().reset_index()
weekday['If_Weekend'] = weekday['WEEKDAY'].map(lambda x: x ==5 or x ==6)
weekday = weekday.groupby(weekday['If_Weekend']).mean().reset_index()
week = [weekday.ix[0,i] for i in range(3,12)]
weekend = [weekday.ix[1,i] for i in range(3,12)]

stat,pvalue = stats.ttest_ind(week, weekend)
print("The p-value for this weekday/weekend ttest is", pvalue)


#visualize th number of crimes per day in 2015 including holiday/non-holiday
new_df_2015 = new_df[new_df['YEAR'] == 2015].reset_index()

color_list = []
for i in range(len(new_df_2015)):
    if new_df_2015.ix[i,'If_Public_Holiday'] is True:
        color_list.append('rgba(222,45,38,0.8)')
    else:
        color_list.append('rgba(204,204,204,1)')
        
plot_2015 = go.Bar(
    x=new_df_2015['CMPLNT_FR_DT'],
    y=new_df_2015['CMPLNT_NUM'],
    name='CMPLNT_NUM per day in 2015(Holiday)',
    marker=dict(
        color=color_list),
)
data = [plot_2015]
layout = go.Layout(
    title='CMPLNT_NUM per day in 2015(Holiday)')
fig = go.Figure(data=data, layout=layout)
py.iplot(fig)
py.image.save_as(fig, filename='CMPLNT_NUM per day in 2015(Holiday).png')

#calculate correlation coefficients
holiday = new_df['CMPLNT_NUM'].groupby([new_df['If_Public_Holiday'],new_df['YEAR']]).mean().unstack().reset_index()
holiday_mean = [holiday.ix[0,i] for i in range(2,11)]
non_holiday_mean = [holiday.ix[1,i] for i in range(2,11)]
stat,pvalue = stats.ttest_ind(holiday_mean, non_holiday_mean)
print("The p-value for this holiday ttest is", pvalue)

