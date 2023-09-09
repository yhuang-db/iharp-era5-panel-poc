from datetime import date, datetime

import geoviews as gv
import geoviews.feature as gf
import panel as pn
import shapely

from ui_query_method import *

live_state = {}

# sidebar components
mk_type = pn.widgets.Select(options=['(time, pixel)', 'time', 'pixel'])
mk_date_range = pn.widgets.DatetimeRangePicker(start=date(2022, 5, 1), end=date(2022, 5, 7), value=(datetime(2022, 5, 1, 0, 0, 0), datetime(2022, 5, 1, 23, 59, 59)))
mk_regions = pn.widgets.Select(options=['Whole World', 'greenland', 'iceland', 'tri', 'rec', 'wkt'])
mk_wkt_input = pn.widgets.TextAreaInput(value='POLYGON((-72 78,-13 81,-44 60,-72 78))')
mk_value_criteria = []
mk_criteria_variables = pn.widgets.Select(options=popular_variables)
mk_predicates_val = pn.widgets.Select(options=['>', '>=', '<', '<=', '=', '<>'], width=50)
mk_input_val = pn.widgets.TextInput(value='0', width=100)
mk_btn_add_value_criteria = pn.widgets.Button(name='Add', button_type='primary', width=100)
mk_value_criteria_str = pn.pane.Str('Variable criteria:')
mk_btn_run = pn.widgets.Button(name='Run', button_type='primary', width=100)
mk_variable_mulit_choice = pn.widgets.MultiChoice(options=popular_variables)
mk_btn_mask_to_xarray = pn.widgets.Button(name='Mask to xarray', button_type='primary', width=100)
mk_plot_variable = pn.widgets.Select(options=[])
mk_btn_refresh_map = pn.widgets.Button(name='Refresh Map', button_type='primary', width=100)
mk_sidebar = pn.Column(
    '###### 1. Mask type',
    mk_type,
    '###### 2. Date range',
    mk_date_range,
    '###### 3. Region',
    mk_regions,
    mk_wkt_input,
    '###### 4. Variable value criteria',
    mk_criteria_variables,
    pn.Row(mk_predicates_val, mk_input_val, mk_btn_add_value_criteria),
    mk_value_criteria_str,
    '###### 5. Generate mask',
    mk_btn_run,
    '###### 6. Mask raster',
    mk_variable_mulit_choice,
    mk_btn_mask_to_xarray,
    '###### 7. Plot',
    mk_plot_variable,
    mk_btn_refresh_map,
)

# main components
mk_mask_table = pn.widgets.Tabulator(None, show_index=False)


# botton on click
def add_value_criteria(event):
    short = long_2_short_map[mk_criteria_variables.value]
    mk_value_criteria.append((short, mk_predicates_val.value, float(mk_input_val.value)))
    output = [f'{short_2_long_map[c[0]]} {c[1]} {c[2]}' for c in mk_value_criteria]
    mk_value_criteria_str.object = 'Variable criteria:'
    for num, i in enumerate(output):
        mk_value_criteria_str.object += f'\n {num + 1}. {i}'


def run_mask(event):
    mask_type = mk_type.value
    start_time, end_time = mk_date_range.value
    region_name = mk_regions.value
    shape = None
    if region_name != 'Whole World':
        if region_name == 'wkt':
            input_wkt = mk_wkt_input.value
            shape = shapely.wkt.loads(input_wkt)
        else:
            shape = gdf_region[gdf_region['name'] == region_name].geometry.values[0]

    df = gen_simple_mask(
        mask_type=mask_type,
        start_time=start_time,
        end_time=end_time,
        shape=shape,
        value_criteria=mk_value_criteria,
    )
    mk_mask_table.value = df.head(25)


def mask_to_xarray(event):
    mask_type = mk_type.value
    start_time, end_time = mk_date_range.value
    region_name = mk_regions.value
    shape = None
    if region_name != 'Whole World':
        if region_name == 'wkt':
            input_wkt = mk_wkt_input.value
            shape = shapely.wkt.loads(input_wkt)
        else:
            shape = gdf_region[gdf_region['name'] == region_name].geometry.values[0]

    mk_data_vars_short = [long_2_short_map[v] for v in mk_variable_mulit_choice.value]
    ds = xarray_from_simple_mask(
        variables=mk_data_vars_short,
        mask_type=mask_type,
        start_time=start_time,
        end_time=end_time,
        shape=shape,
        value_criteria=mk_value_criteria,
    )

    live_state['ds'] = ds
    xa_info.object = f'{ds}'
    xa_table.value = ds.to_dataframe().reset_index().head(10)
    mk_plot_variable.options = mk_variable_mulit_choice.value

    # update aggregation result
    agg = []
    for short in mk_data_vars_short:
        long = short_2_long_map[short]
        ds_min = ds[ds[short].argmin(...)]
        min_val = ds_min[short].values
        min_lon = ds_min['longitude'].values
        min_lat = ds_min['latitude'].values
        min_time = ds_min['time'].values
        ds_max = ds[ds[short].argmax(...)]
        max_val = ds_max[short].values
        max_lon = ds_max['longitude'].values
        max_lat = ds_max['latitude'].values
        max_time = ds_max['time'].values
        mean = ds.mean()[short].values
        agg.append((long, min_val, min_lon, min_lat, min_time, max_val, max_lon, max_lat, max_time, mean))
    df = pd.DataFrame.from_records(agg, columns=['Variable', 'Min', 'Lon of Min', 'Lat of Min', 'Time of Min', 'Max', 'Lon of Max', 'Lat of Max', 'Time of Max', 'Mean'])
    xa_agg.value = df


def refresh_map(event):
    ds = live_state['ds']
    viz_short = long_2_short_map[mk_plot_variable.value]
    gv_ds = gv.Dataset(ds, vdims=viz_short)
    image = gv_ds.to(gv.Image, ['longitude', 'latitude'])
    image.opts(cmap='coolwarm', colorbar=True, tools=['hover'])
    image = image * gf.coastline
    map_panel.object = image


mk_btn_add_value_criteria.on_click(add_value_criteria)
mk_btn_run.on_click(run_mask)
mk_btn_mask_to_xarray.on_click(mask_to_xarray)
mk_btn_refresh_map.on_click(refresh_map)