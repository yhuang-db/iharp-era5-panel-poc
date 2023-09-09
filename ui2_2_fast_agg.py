from datetime import date, datetime

import geoviews as gv
import geoviews.feature as gf
import panel as pn
import shapely

from ui_query_method import *

# sidebar_components
agg_method = pn.widgets.Select(options=['avg', 'min', 'max'])
agg_variable_select = pn.widgets.Select(options=popular_variables)
agg_date_range = pn.widgets.DatetimeRangePicker(start=date(2022, 5, 1), end=date(2022, 5, 7), value=(datetime(2022, 5, 1, 0, 0, 0), datetime(2022, 5, 1, 23, 59, 59)))
agg_regions = pn.widgets.Select(options=['Whole World', 'greenland', 'iceland', 'tri', 'rec', 'wkt'])
agg_wkt_input = pn.widgets.TextAreaInput(value='POLYGON((-72 78,-13 81,-44 60,-72 78))')
agg_value_criteria = []
agg_criteria_variables = pn.widgets.Select(options=popular_variables)
agg_predicates_val = pn.widgets.Select(options=['>', '>=', '<', '<=', '=', '<>'], width=50)
agg_input_val = pn.widgets.TextInput(value='0', width=100)
agg_btn_add_value_criteria = pn.widgets.Button(name='Add', button_type='primary', width=100)
agg_value_criteria_str = pn.pane.Str('Variable criteria:')
agg_btn_run = pn.widgets.Button(name='Run', button_type='primary', width=100)
agg_sidebar = pn.Column(
    '###### 1. Aggregation method',
    agg_method,
    '###### 2. Variables',
    agg_variable_select,
    '###### 3. Date range',
    agg_date_range,
    '###### 4. Region',
    agg_regions,
    agg_wkt_input,
    '###### 5. Variable value criteria',
    agg_criteria_variables,
    pn.Row(agg_predicates_val, agg_input_val, agg_btn_add_value_criteria),
    agg_value_criteria_str,
    agg_btn_run,
)

# main components
agg_single_value_result = pn.pane.Markdown('###### Aggregation result:')


# botton on click
def add_value_criteria(event):
    short = long_2_short_map[agg_criteria_variables.value]
    agg_value_criteria.append((short, agg_predicates_val.value, float(agg_input_val.value)))
    output = [f'{short_2_long_map[c[0]]} {c[1]} {c[2]}' for c in agg_value_criteria]
    agg_value_criteria_str.object = 'Variable criteria:'
    for num, i in enumerate(output):
        agg_value_criteria_str.object += f'\n {num + 1}. {i}'


def run_agg(event):
    mma = agg_method.value
    variable = long_2_short_map[agg_variable_select.value]
    start_time, end_time = agg_date_range.value
    region_name = agg_regions.value
    shape = None
    if region_name != 'Whole World':
        if region_name == 'wkt':
            input_wkt = agg_wkt_input.value
            shape = shapely.wkt.loads(input_wkt)
        else:
            shape = gdf_region[gdf_region['name'] == region_name].geometry.values[0]

    res = agg_from_clickhouse_pid(
        mma=mma,
        variable=variable,
        start_time=start_time,
        end_time=end_time,
        shape=shape,
        value_criteria=agg_value_criteria,
    )

    agg_single_value_result.object = f'###### {mma} {agg_variable_select.value}: {res}'

    # update map
    if region_name != 'Whole World':
        gv_shape = gv.Shape(shape)
        image = (gf.ocean * gf.land * gf.coastline * gv_shape).opts(global_extent=True)
        map_panel.object = image


agg_btn_add_value_criteria.on_click(add_value_criteria)
agg_btn_run.on_click(run_agg)