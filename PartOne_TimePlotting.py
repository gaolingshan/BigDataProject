import plotly.tools as tls
tls.set_credentials_file(username='YuweiTu', api_key='KXrlSB9iz8kutUHXd4gp')

import pandas as pd
from sqlalchemy import create_engine # database connection
import datetime as dt
from IPython.display import display

import plotly.plotly as py
import plotly.graph_objs as go# interactive graphing
from plotly.graph_objs import Bar, Scatter, Marker, Layout

#load data
date = pd.read_csv('column1_summary.csv', header = None)
time = pd.read_csv('column2_summary.csv', header = None)

#drop invalid records
date = date[date[3] == 'VALID']
time = time[time[3] == 'VALID']


date[0] = date[0].apply(lambda x: str(x).split("/")[2])
time[0] = time[0].apply(lambda x: str(x).split(":")[0])


year_list = ['2006','2007','2008','2009','2010','2011','2012','2013','2014','2015']
date = date[date[0].isin(year_list)]
count = date[1].groupby(date[0]).count()
fig1 = py.iplot([go.Scatter(x = count.index, y = count)], filename='Crimes happend in the last ten years')
fig1.save('Crimes happend in the last ten years.png')


count = pd.DataFrame(time[1].groupby(time[0]).count())
fig2 = py.iplot([Bar(x=count.index, y=count[1])], filename='Crime Count by Hour')
fig2.save('Crime Count by Hour.png')


