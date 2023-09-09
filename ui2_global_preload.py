import pickle

import geopandas as gpd
import pandas as pd
import panel as pn
import geoviews.feature as gf

# preload data
popular_variables = pickle.load(open("data/pickle/popular_variable.pkl", "rb"))
long_2_short_map = pickle.load(open("data/pickle/file_variable_map.pkl", "rb"))
short_2_long_map = {v: k for k, v in long_2_short_map.items()}

gdf_list = []
for r in ['greenland', 'iceland', 'tri', 'rec']:
    gdf = gpd.read_file(f'data/vector/{r}.geojson')
    gdf['name'] = r
    gdf_list.append(gdf)
gdf_region = pd.concat(gdf_list, ignore_index=True)
gdf_region = gdf_region.set_crs(4326)

df_pid = pd.read_csv('data/vector/pid.csv')
gdf_pid = gpd.GeoDataFrame(df_pid, geometry=gpd.points_from_xy(x=df_pid.longitude, y=df_pid.latitude, crs=4326))

# global component
gv_image = (gf.ocean * gf.land * gf.coastline).opts(global_extent=True)
map_panel = pn.panel(gv_image, width=700, height=500)

# xarray page
xa_info = pn.pane.Str('Xarray info: ')
xa_table = pn.widgets.Tabulator(None, show_index=False)
xa_agg = pn.widgets.Tabulator(None, show_index=False)
xa_page = pn.Column(
    '##### 1. Xarray Info',
    xa_info,
    '##### 2. Xarray as Table',
    xa_table,
    '##### 3. Xarray Aggregation',
    xa_agg,
)