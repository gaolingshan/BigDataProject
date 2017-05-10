import plotly.tools as tls
tls.set_credentials_file(username='YuweiTu', api_key='KXrlSB9iz8kutUHXd4gp')
import pandas as pd
from sqlalchemy import create_engine # database connection
import datetime as dt
from IPython.display import display

import plotly.plotly as py
import plotly.graph_objs as go# interactive graphing
from plotly.graph_objs import Bar, Scatter, Marker, Layout 

crime_data = create_engine('sqlite:///crime_data.db')
df = pd.read_csv('NYPD_Complaint_Data_Historic.csv')

for df in pd.read_csv('NYPD_Complaint_Data_Historic.csv', chunksize=chunksize, iterator=True, encoding='utf-8'):
    
    df = df.rename(columns={c: c.replace(' ', '') for c in df.columns}) # Remove spaces from columns
    df.index += index_start

    j+=1
    print('{} seconds: completed {} rows'.format((dt.datetime.now() - start).seconds, j*chunksize))

    df.to_sql('data', crime_data, if_exists='append')
    index_start = df.index[-1] + 1


df_1 = pd.read_sql_query('SELECT KY_CD, COUNT(*) as `num_crimes`'
                       'FROM data '
                       'GROUP BY KY_CD '
                       'ORDER BY -num_crimes', crime_data)

fig1 = plt.bar(df_1.KY_CD, df_1.num_crimes, align='center', alpha=0.5)


df_2 = pd.read_sql_query('SELECT KY_CD, OFNS_DESC,COUNT(*) as `num_crimes`'
                       'FROM data '
                       'GROUP BY KY_CD '
                       'ORDER BY -num_crimes '
                        'LIMIT 10 ', crime_data)
fig2 = py.iplot([Bar(x=df_2.OFNS_DESC, y=df_2.num_crimes)], filename='Top 10 Offense types')
fig2.save('Top 10 Offense types.png')

df_3 = pd.read_sql_query('SELECT PD_CD, COUNT(*) as `num_crimes`'
                       'FROM data '
                       'GROUP BY PD_CD '
                       'ORDER BY -num_crimes', crime_data)
fig3 = py.iplot([Bar(x=df_3.PD_CD, y=df_3.num_crimes)], filename='Most Common Crimes by PD_CD')


df_4= pd.read_sql_query('SELECT PD_CD, PD_DESC,COUNT(*) as `num_crimes`'
                       'FROM data '
                       'GROUP BY PD_CD '
                       'ORDER BY -num_crimes '
                        'LIMIT 10 ', crime_data)
fig4 = py.iplot([Bar(x=df_4.PD_DESC, y=df_4.num_crimes)], filename='Top 10 Offense types with PD')


df_5= pd.read_sql_query('SELECT CRM_ATPT_CPTD_CD,COUNT(*) as `num_crimes`'
                       'FROM data '
                       'GROUP BY CRM_ATPT_CPTD_CD '
                       'ORDER BY -num_crimes ', crime_data)
fig5 = py.iplot([go.Pie(labels=df_5.CRM_ATPT_CPTD_CD, values=df_5.num_crimes)], filename='Percentage of Completed/Attemped')
fig5.save('Percentage of Completed/Attemped.png')


df_6 = pd.read_sql_query('SELECT KY_CD, OFNS_DESC,COUNT(*) as `num_crimes`'
                       'FROM data '
                       'WHERE  CRM_ATPT_CPTD_CD = "ATTEMPTED"'
                        'GROUP BY KY_CD '
                        'ORDER BY -num_crimes ', crime_data)

df_7= pd.read_sql_query('SELECT LAW_CAT_CD,COUNT(*) as `num_crimes`'
                       'FROM data '
                       'GROUP BY LAW_CAT_CD '
                       'ORDER BY -num_crimes ', crime_data)
fig7 = py.iplot([go.Pie(labels=df_7.LAW_CAT_CD, values=df_7.num_crimes)], filename='Most Common Crimes by LAW_CAT_CD')


df_8= pd.read_sql_query('SELECT JURIS_DESC,COUNT(*) as `num_crimes`'
                       'FROM data '
                       'GROUP BY JURIS_DESC '
                       'ORDER BY -num_crimes ', crime_data)
fig8 = py.iplot([Bar(x=df_8.JURIS_DESC, y=df_8.num_crimes)], filename='Most Common Crimes by JURIS_DESC')