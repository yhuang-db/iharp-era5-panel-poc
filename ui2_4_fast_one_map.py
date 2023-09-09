from datetime import date, datetime

import geoviews as gv
import geoviews.feature as gf
import panel as pn
import shapely

from ui_query_method import *

live_state = {}

# sidebar components
om_agg_method = pn.widgets.Select(options=['avg', 'min', 'max'])
om_agg_variable_multi_choise = pn.widgets.MultiChoice(options=popular_variables)
om_agg_date_range = pn.widgets.DatetimeRangePicker(start=date(2022, 5, 1), end=date(2022, 5, 7), value=(datetime(2022, 5, 1, 0, 0, 0), datetime(2022, 5, 1, 23, 59, 59)))
om_agg_regions = pn.widgets.Select(options=['Whole World', 'greenland', 'iceland', 'tri', 'rec', 'wkt'])
om_agg_wkt_input = pn.widgets.TextAreaInput(value='POLYGON((-72 78,-13 81,-44 60,-72 78))')
om_agg_value_criteria = []
om_agg_criteria_variables = pn.widgets.Select(options=popular_variables)
om_agg_predicates_val = pn.widgets.Select(options=['>', '>=', '<', '<=', '=', '<>'], width=50)
om_agg_input_val = pn.widgets.TextInput(value='0', width=100)
om_agg_btn_add_value_criteria = pn.widgets.Button(name='Add', button_type='primary', width=100)
om_agg_value_criteria_str = pn.pane.Str('Variable criteria:')
om_agg_btn_run = pn.widgets.Button(name='Run', button_type='primary', width=100)
om_plot_variable = pn.widgets.Select(options=[])
om_btn_refresh_map = pn.widgets.Button(name='Refresh Map', button_type='primary', width=100)
om_agg_sidebar = pn.Column(
    '###### 1. Aggregation method',
    om_agg_method,
    '###### 2. Variables',
    om_agg_variable_multi_choise,
    '###### 3. Date range',
    om_agg_date_range,
    '###### 4. Region',
    om_agg_regions,
    om_agg_wkt_input,
    '###### 5. Variable value criteria',
    om_agg_criteria_variables,
    pn.Row(om_agg_predicates_val, om_agg_input_val, om_agg_btn_add_value_criteria),
    om_agg_value_criteria_str,
    om_agg_btn_run,
    '###### 6. Plot',
    om_plot_variable,
    om_btn_refresh_map,
)


# botton on click
def add_value_criteria(event):
    short = long_2_short_map[om_agg_criteria_variables.value]
    om_agg_value_criteria.append((short, om_agg_predicates_val.value, float(om_agg_input_val.value)))
    output = [f'{short_2_long_map[c[0]]} {c[1]} {c[2]}' for c in om_agg_value_criteria]
    om_agg_value_criteria_str.object = 'Variable criteria:'
    for num, i in enumerate(output):
        om_agg_value_criteria_str.object += f'\n {num + 1}. {i}'


def run_om_agg(event):
    mma = om_agg_method.value
    short_vars = [long_2_short_map[v] for v in om_agg_variable_multi_choise.value]
    start_time, end_time = om_agg_date_range.value
    region_name = om_agg_regions.value
    shape = None
    if region_name != 'Whole World':
        if region_name == 'wkt':
            input_wkt = om_agg_wkt_input.value
            shape = shapely.wkt.loads(input_wkt)
        else:
            shape = gdf_region[gdf_region['name'] == region_name].geometry.values[0]

    ds = groupby_pid_from_clickhouse_pid(
        variables=short_vars,
        agg=mma,
        start_time=start_time,
        end_time=end_time,
        shape=shape,
        value_criteria=om_agg_value_criteria,
    )

    live_state['ds'] = ds
    xa_info.object = f'{ds}'
    xa_table.value = ds.to_dataframe().reset_index().head(10)
    om_plot_variable.options = om_agg_variable_multi_choise.value


def refresh_map(event):
    ds = live_state['ds']
    viz_short = long_2_short_map[om_plot_variable.value]
    gv_ds = gv.Dataset(ds, vdims=f'{om_agg_method.value}_{viz_short}')
    image = gv_ds.to(gv.Image, ['longitude', 'latitude'])
    image.opts(cmap='coolwarm', colorbar=True, tools=['hover'])
    image = image * gf.coastline
    map_panel.object = image


om_agg_btn_add_value_criteria.on_click(add_value_criteria)
om_agg_btn_run.on_click(run_om_agg)
om_btn_refresh_map.on_click(refresh_map)