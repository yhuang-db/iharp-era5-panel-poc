from datetime import date, datetime

import geoviews as gv
import geoviews.feature as gf
import panel as pn
import plotly.express as px
import shapely

from ui_query_method import *

# sidebar components
ts_variable_mulit_choice = pn.widgets.MultiChoice(options=popular_variables)
ts_time_unit = pn.widgets.Select(options=['hour', 'day', 'week', 'month', 'year'])
ts_agg_method = pn.widgets.Select(options=['avg', 'min', 'max'])
ts_date_range = pn.widgets.DatetimeRangePicker(start=date(2022, 5, 1), end=date(2022, 5, 7), value=(datetime(2022, 5, 1, 0, 0, 0), datetime(2022, 5, 1, 23, 59, 59)))
ts_regions = pn.widgets.Select(options=['Whole World', 'greenland', 'iceland', 'tri', 'rec', 'wkt'])
ts_wkt_input = pn.widgets.TextAreaInput(value='POLYGON((-72 78,-13 81,-44 60,-72 78))')
ts_value_criteria = []
ts_criteria_variables = pn.widgets.Select(options=popular_variables)
ts_predicates_val = pn.widgets.Select(options=['>', '>=', '<', '<=', '=', '<>'], width=50)
ts_input_val = pn.widgets.TextInput(value='0', width=100)
ts_btn_add_value_criteria = pn.widgets.Button(name='Add', button_type='primary', width=100)
ts_value_criteria_str = pn.pane.Str('Variable criteria:')
ts_btn_run = pn.widgets.Button(name='Run', button_type='primary', width=100)
ts_sidebar = pn.Column(
    '###### 1. Variables',
    ts_variable_mulit_choice,
    '###### 2. Time unit',
    ts_time_unit,
    '###### 3. Aggregation method',
    ts_agg_method,
    '###### 4. Date range',
    ts_date_range,
    '###### 5. Region',
    ts_regions,
    ts_wkt_input,
    '###### 6. Variable value criteria',
    ts_criteria_variables,
    pn.Row(ts_predicates_val, ts_input_val, ts_btn_add_value_criteria),
    ts_value_criteria_str,
    ts_btn_run,
)

# main components
ts_plotly_plot = pn.pane.Plotly(None)


# botton on click
def add_value_criteria(event):
    short = long_2_short_map[ts_criteria_variables.value]
    ts_value_criteria.append((short, ts_predicates_val.value, float(ts_input_val.value)))
    output = [f'{short_2_long_map[c[0]]} {c[1]} {c[2]}' for c in ts_value_criteria]
    ts_value_criteria_str.object = 'Variable criteria:'
    for num, i in enumerate(output):
        ts_value_criteria_str.object += f'\n {num + 1}. {i}'


def run_ts(event):
    variables = [f'{long_2_short_map[v]}' for v in ts_variable_mulit_choice.value]
    time_unit = ts_time_unit.value
    agg_method = ts_agg_method.value
    start_time, end_time = ts_date_range.value
    region_name = ts_regions.value
    shape = None
    if region_name != 'Whole World':
        if region_name == 'wkt':
            input_wkt = ts_wkt_input.value
            shape = shapely.wkt.loads(input_wkt)
        else:
            shape = gdf_region[gdf_region['name'] == region_name].geometry.values[0]

    df = groupby_time_from_clickhouse_pid(
        variables=variables,
        time_unit=time_unit,
        agg=agg_method,
        start_time=start_time,
        end_time=end_time,
        shape=shape,
        value_criteria=ts_value_criteria,
    )

    fig = px.line(df)
    ts_plotly_plot.object = fig

    # update map
    if region_name != 'Whole World':
        gv_shape = gv.Shape(shape)
        image = (gf.ocean * gf.land * gf.coastline * gv_shape).opts(global_extent=True)
        map_panel.object = image


ts_btn_add_value_criteria.on_click(add_value_criteria)
ts_btn_run.on_click(run_ts)
