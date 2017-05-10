import pandas as pd
import numpy as np
import shapely
import geoplot as gplt
import geoplot.crs as gcrs
import pandas as pd
import geopandas as gpd
import geoplot as gplt
import geoplot.crs as gcrs
import matplotlib.pyplot as plt
import numpy as np

df = pd.read_csv("Ten_Year_Summary.csv")
df.index = df['ZIPCode']
df.index = df.index.map(str)
test_precincts = gpd.read_file("ExternalData/nyc-zip-code-tabulation-areas-polygons.geojson")
test_precincts.index = test_precincts["postalCode"]
test = test_precincts.join(df, how = 'left')
test_precincts = test_precincts.sort_index()
test_precincts['count'] = test['count']
test_precincts['Educated'] = test['Educated']
test_precincts['Income'] = test['Income']
test_precincts['Population']= test['Population']
test_precincts['Median age'] = test['Median age']
test_precincts['Sex ratio (males per 100 females)'] = test['Sex ratio (males per 100 females)'] 
test_precincts = test_precincts.fillna(method='bfill')
f, axarr = plt.subplots(1, 2, figsize=(20, 10), subplot_kw={
    'projection': gcrs.AlbersEqualArea(central_latitude=40.7128, central_longitude=-74.0059)
})

# Educated 
f.suptitle('Comparison between Educated level and number of crimes', fontsize=16)
f.subplots_adjust(top=0.95)
gplt.choropleth(test_precincts, hue = 'Educated', projection= gcrs.AlbersEqualArea(central_latitude=40.7128, central_longitude=-74.0059), linewidth=0,  figsize=(12, 12), scheme = 'Fisher_Jenks', cmap='Reds', 
                    legend = True, legend_kwargs={'loc': 'upper left'}, ax = axarr[0])
gplt.choropleth(test_precincts, hue = 'count', projection= gcrs.AlbersEqualArea(central_latitude=40.7128, central_longitude=-74.0059), linewidth=0,  figsize=(12, 12), scheme = 'Fisher_Jenks', cmap='Blues', 
                    legend = True, legend_kwargs={'loc': 'upper left'}, ax = axarr[1])
f.savefig("educated.png", bbox_inches='tight')

# Income
f.suptitle('Comparison between Income level and number of crimes', fontsize=16)
f.subplots_adjust(top=0.95)
gplt.choropleth(test_precincts, hue = 'Income', projection= gcrs.AlbersEqualArea(central_latitude=40.7128, central_longitude=-74.0059), linewidth=0,  figsize=(12, 12), scheme = 'Fisher_Jenks', cmap='Oranges', 
                    legend = True, legend_kwargs={'loc': 'upper left'}, ax = axarr[0])
f.savefig("income.png", bbox_inches='tight')

# Population
f.suptitle('Comparison between Population and number of crimes', fontsize=16)
f.subplots_adjust(top=0.95)
gplt.choropleth(test_precincts, hue = 'Population', projection= gcrs.AlbersEqualArea(central_latitude=40.7128, central_longitude=-74.0059), linewidth=0,  figsize=(12, 12), scheme = 'Fisher_Jenks', cmap='Greens', 
                    legend = True, legend_kwargs={'loc': 'upper left'}, ax = axarr[0])
f.savefig("population.png", bbox_inches='tight')

# Median Age
f.suptitle('Comparison between Median age and number of crimes', fontsize=16)
f.subplots_adjust(top=0.95)
gplt.choropleth(test_precincts, hue = 'Median age', projection= gcrs.AlbersEqualArea(central_latitude=40.7128, central_longitude=-74.0059), linewidth=0,  figsize=(12, 12), scheme = 'Fisher_Jenks', cmap='YlOrBr', 
                    legend = True, legend_kwargs={'loc': 'upper left'}, ax = axarr[0])
f.savefig("age.png", bbox_inches='tight')

# Sex ratio
f.suptitle('Comparison between Sex ratio and number of crimes', fontsize=16)
f.subplots_adjust(top=0.95)
gplt.choropleth(test_precincts, hue = 'Sex ratio (males per 100 females)', projection= gcrs.AlbersEqualArea(central_latitude=40.7128, central_longitude=-74.0059), linewidth=0,  figsize=(12, 12), scheme = 'Fisher_Jenks', cmap='OrRd', 
                    legend = True, legend_kwargs={'loc': 'upper left'}, ax = axarr[0])
f.savefig("sex_ratio.png", bbox_inches='tight')



