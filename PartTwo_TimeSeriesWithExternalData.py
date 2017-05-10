import plotly.tools as tls
tls.set_credentials_file(username='gaolingshan', api_key='VtYTYiUcXzZcQ3xM5hzU')
#Note that the key may be invalid when re-run the program

import pandas as pd
import datetime as dt
from IPython.display import display

import plotly.plotly as py
import plotly.graph_objs as go# interactive graphing
from plotly.graph_objs import Bar, Scatter, Marker, Layout 
from datetime import datetime

#Please remind to keep program and external data files in the same folder
#read Condo Price Index Data/Labor Data
condo = pd.read_csv("ExternalData/NYC_Condo_Price_Index.csv").dropna()
labor = pd.read_csv("ExternalData/NYC_LaborForce_Data.csv").dropna()

#transfor into DateTime type
for i in range(len(condo)):
    condo.ix[i,'DATE'] = datetime.strptime(condo.ix[i,'DATE'],"%Y-%m-%d")
    condo.ix[i,'YEAR'] = condo.ix[i,'DATE'].year
    condo.ix[i,'MONTH'] = condo.ix[i,'DATE'].month

for i in range(len(labor)):
        labor.ix[i,'DATE'] = datetime.strptime(labor.ix[i,'Month'],"%b-%y")
        labor.ix[i,'YEAR'] = labor.ix[i,'DATE'].year
        labor.ix[i,'MONTH'] = labor.ix[i,'DATE'].month


#analyze data from 2005 Jan to 2015 Dec
condo = condo[(condo['YEAR'] < 2016) & (condo['YEAR'] > 2005)].reset_index()
labor = labor[(labor['YEAR'] < 2016) & (labor['YEAR'] > 2005)].reset_index()



#read original data
df = pd.read_csv("Cleaned_data_updated_zipcode.csv",usecols = [0,1,2,3,4,5])
#group by date
new_df = pd.DataFrame(df['CMPLNT_NUM'].groupby(df['CMPLNT_FR_DT']).count().reset_index())

#transform DateTime type
for i in range(len(new_df)):
    try:
        new_df.ix[i,'CMPLNT_FR_DT'] = datetime.strptime(new_df.ix[i,'CMPLNT_FR_DT'],"%m/%d/%Y")
        new_df.ix[i,'YEAR'] = new_df.ix[i,'CMPLNT_FR_DT'].year
        new_df.ix[i,'MONTH'] = new_df.ix[i,'CMPLNT_FR_DT'].month
    except:
        print(new_df.ix[i,'CMPLNT_FR_DT'])

# group by(year,month) to prepare for merging
crime_time = new_df['CMPLNT_NUM'].groupby([new_df['YEAR'],new_df['MONTH']]).sum().reset_index()
crime_time = crime_time[(crime_time['YEAR'] < 2016)&(crime_time['YEAR'] >2005)].reset_index()

#merge external dataset with original data
condo_with_crime = pd.concat([condo, crime_time['CMPLNT_NUM']], axis=1)
labor_with_crime = pd.concat([labor, crime_time['CMPLNT_NUM']], axis=1)

'''
Analyze the relationship between Condo Price Index and Number of Crimes
'''

#calculate monthly correlation coefficients
print('-------------------------------------------------')
print('The correlation coefficient between Number of Crimes per month and Conde Price Index per month is: \n')
print(condo_with_crime['NYXRCSA'].corr(condo_with_crime['CMPLNT_NUM']))
print('-------------------------------------------------')
print('The correlation coefficient between Number of Crimes per month and Unemployment Rate per month is: \n')
print(labor_with_crime['Unemp Rate(%)'].corr(condo_with_crime['CMPLNT_NUM']))
print('-------------------------------------------------')
print('The correlation coefficient  between Number of Crimes per month and Labor Force Participation Rate per month is: \n')
print(labor_with_crime['LFPART(%)'].corr(condo_with_crime['CMPLNT_NUM']))
print('-------------------------------------------------')
print('The correlation coefficient  between Number of Crimes per month and Employment/Population ratio  per month is: \n')
print(labor_with_crime['Emp/PopRatio (%)'].corr(condo_with_crime['CMPLNT_NUM']))
print('-------------------------------------------------')

#plotting
# visualize the relationship between the number of crimes and condo price index per month
# from 2005 Jan to 2015 Dec
trace1 = go.Scatter(
    x=condo_with_crime['DATE'],
    y=condo_with_crime['NYXRCSA'],
    name='Conde Price Index'
)
trace2 = go.Scatter(
    x=condo_with_crime['DATE'],
    y=condo_with_crime['CMPLNT_NUM'],
    name='COMPLNT_NUM',
    yaxis='y2'
)
data = [trace1, trace2]
layout = go.Layout(
    title='CMPLNT_NUM vs Condo Price Index',
    yaxis=dict(
        title='Condo Price Index'
    ),
    yaxis2=dict(
        title='Number of Crimes per month',
        titlefont=dict(
            color='rgb(148, 103, 189)'
        ),
        tickfont=dict(
            color='rgb(148, 103, 189)'
        ),
        overlaying='y',
        side='right'
    )
)
fig = go.Figure(data=data, layout=layout)
py.image.save_as(fig, filename='CMPLNT_NUM vs Condo Price Index.png')




# visualize the relationship between the number of crimes and unemployemnt rate per month
# from 2005 Jan to 2015 Dec
trace1 = go.Scatter(
    x=labor_with_crime['DATE'],
    y=labor_with_crime['Unemp Rate(%)'],
    name='Unemployment Rate(%)'
)
trace2 = go.Scatter(
    x=condo_with_crime['DATE'],
    y=condo_with_crime['CMPLNT_NUM'],
    name='COMPLNT_NUM',
    yaxis='y2'
)
data = [trace1, trace2]
layout = go.Layout(
    title='CMPLNT_NUM vs Unemployment Rate(%)',
    yaxis=dict(
        title='Unemployment Rate(%)'
    ),
    yaxis2=dict(
        title='Number of Crimes per month',
        titlefont=dict(
            color='rgb(148, 103, 189)'
        ),
        tickfont=dict(
            color='rgb(148, 103, 189)'
        ),
        overlaying='y',
        side='right'
    )
)
fig = go.Figure(data=data, layout=layout)
py.image.save_as(fig, filename='CMPLNT_NUM vs Unemployment Rate(%).png')




# visualize the relationship between the number of crimes and labor participation rate per month
# from 2005 Jan to 2015 Dec
trace1 = go.Scatter(
    x=labor_with_crime['DATE'],
    y=labor_with_crime['LFPART(%)'],
    name='Labor Participation Rate'
)
trace2 = go.Scatter(
    x=condo_with_crime['DATE'],
    y=condo_with_crime['CMPLNT_NUM'],
    name='COMPLNT_NUM',
    yaxis='y2'
)
data = [trace1, trace2]
layout = go.Layout(
    title='CMPLNT_NUM vs Labor Participation Rate',
    yaxis=dict(
        title='Labor Participation Rate'
    ),
    yaxis2=dict(
        title='Number of Crimes per month',
        titlefont=dict(
            color='rgb(148, 103, 189)'
        ),
        tickfont=dict(
            color='rgb(148, 103, 189)'
        ),
        overlaying='y',
        side='right'
    )
)
fig = go.Figure(data=data, layout=layout)
py.image.save_as(fig, filename='CMPLNT_NUM vs Labor Participation Rate.png')



# visualize the relationship between the number of crimes and employment/population ratio per month
# from 2005 Jan to 2015 Dec
trace1 = go.Scatter(
    x=labor_with_crime['DATE'],
    y=labor_with_crime['Emp/PopRatio (%)'],
    name='Employment/Population ratio'
)
trace2 = go.Scatter(
    x=condo_with_crime['DATE'],
    y=condo_with_crime['CMPLNT_NUM'],
    name='COMPLNT_NUM',
    yaxis='y2'
)
data = [trace1, trace2]
layout = go.Layout(
    title='CMPLNT_NUM vs Employment/Population ratio',
    yaxis=dict(
        title='Employment/Population ratio'
    ),
    yaxis2=dict(
        title='Number of Crimes per month',
        titlefont=dict(
            color='rgb(148, 103, 189)'
        ),
        tickfont=dict(
            color='rgb(148, 103, 189)'
        ),
        overlaying='y',
        side='right'
    )
)
fig = go.Figure(data=data, layout=layout)
py.image.save_as(fig,filename = "CMPLNT_NUM vs Emp_Pop ratio.png")


#calculate yearly correlation coefficients
avg_condo = condo_with_crime['NYXRCSA'].groupby(condo_with_crime['YEAR']).mean()
avg_unemp = labor_with_crime['Unemp Rate(%)'].groupby(labor_with_crime['YEAR']).mean()
avg_lfpart = labor_with_crime['LFPART(%)'].groupby(labor_with_crime['YEAR']).mean()
avg_emp_pop = labor_with_crime['Emp/PopRatio (%)'].groupby(labor_with_crime['YEAR']).mean()
crime_sum = condo_with_crime['CMPLNT_NUM'].groupby(condo_with_crime['YEAR']).sum()


print('-------------------------------------------------')
print('The correlation coefficient between Number of Crimes per year and average Conde Price Index per year is: \n')
print(avg_condo.corr(crime_sum))
print('-------------------------------------------------')
print('The correlation coefficient between Number of Crimes per year and average Unemployment Rate per year is: \n')
print(avg_unemp.corr(crime_sum))
print('-------------------------------------------------')
print('The correlation coefficient  between Number of Crimes per year and average Labor Force Participation Rate per year is: \n')
print(avg_lfpart.corr(crime_sum))
print('-------------------------------------------------')
print('The correlation coefficient  between Number of Crimes per year and average Employment/Population ratio  per year is: \n')
print(avg_emp_pop.corr(crime_sum))
print('-------------------------------------------------')



