import pandas as pd
import numpy as np
import sys
import numpy as np
import pandas as pd
import itertools
from math import sqrt
from operator import add
from os.path import join, isfile, dirname
from csv import reader
import plotly.plotly as py
import pandas as pd
df = pd.read_csv("NYPD_Complaint_Data_Historic.csv")

import plotly.tools as tls
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot
#requires version >= 1.9.0
tls.set_credentials_file(username='YuweiTu', api_key='KXrlSB9iz8kutUHXd4gp')
import plotly.offline as offline
from plotly.graph_objs import *
import random
index = random.sample(range(5000000), 10000)
mapbox_access_token = 'pk.eyJ1Ijoieml6aHVvIiwiYSI6ImNqMWltcXFyMTAxdmEzM29ob3ltM284dHoifQ.OatUa34UQz45teeI9J3TnA'

data = Data([
    Scattermapbox(
        lon = df['Longitude'][index],
        lat = df['Latitude'][index],
        mode='markers',
        marker=Marker(
            size=3
        ),
        text=['New York'],
    )
])

layout = Layout(
    autosize=True,
    hovermode='closest',
    mapbox=dict(
        accesstoken=mapbox_access_token,
        bearing=0,
        center=dict(
            lat=40.828848,
            lon= -73.916661
        ),
        pitch=0,
        zoom=5
    ),
)

fig = dict(data=data, layout=layout)
#py.iplot(fig, filename='NYC CRIME DATA')
offline.plot(fig, filename='file.html')
