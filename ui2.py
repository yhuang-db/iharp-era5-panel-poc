import panel as pn

# from ui2_0_document import *
from ui2_1_data_access import *
from ui2_2_fast_agg import *
from ui2_3_fast_time_series import *
from ui2_4_fast_one_map import *
from ui2_5_mask import *
from ui2_6_mask_by_agg import *
from ui2_9_just_sql import *

# sidebar layout
side_taps = pn.WidgetBox(pn.Tabs(
    ('1. Data Access', da_sidebar),
    ('2. Fast Agg', agg_sidebar),
    ('3. Fast Time Series', ts_sidebar),
    ('4. Fast One Map', om_agg_sidebar),
    ('5. Mask', mk_sidebar),
    ('6. Mask by Agg', ma_sidebar),
))

doc_markdown = pn.pane.Markdown('''
#### Document
                                
**p_table** schema: (**time, pid**, longitude, latitude, variable1, variable2, variable3, ...)
, where **pid** is a unique identifier for each pixel, i.e., a (longitude, latitude) pair.
                                
**Procedure** shape_to_pid_set(shape): return a set of pid (pixels) within the shape.

0- Mask query
```sql
-- Spatio-temporal mask
SELECT time, pid
FROM p_table
WHERE
AND time in [input_time_range|time_query]
AND pid in [shape_to_pid_set(input_shape)|pixel_query]
AND variable [=|>|<] [input_value|value_query]

-- Time query            
SELECT DISTINCT time
FROM p_table
WHERE 
AND time in [input_time_range|time_query]
AND pid in [shape_to_pid_set(input_shape)|pixel_query]
AND variable [=|>|<] [input_value|value_query]
                                
-- Pixel query
SELECT DISTINCT pid
FROM p_table
WHERE 
AND time in [input_time_range|time_query]
AND pid in [shape_to_pid_set(input_shape)|pixel_query]
AND variable [=|>|<] [input_value|value_query]
                                
-- Mask by Aggregation
SELECT time
FROM p_table
WHERE pid in [shape_to_pid_set(input_shape)|pixel_query]
GROUP BY time
HAVING aggregation(variable) [=|>|<] [input_value|value_query]
                                
SELECT pid
FROM p_table
WHERE time in [input_time_range|time_query]
GROUP BY pid
HAVING aggregation(variable) [=|>|<] [input_value|value_query]
```    
                                
1- Raster query                          
```sql
SELECT time, longitude, latitude, variable1, variable2, ...
FROM p_table
WHERE 
AND time in [input_time_range|time_query]
AND pid in [shape_to_pid_set(input_shape)|pixel_query]
AND variable [=|>|<] [input_value|value_query]

                                
SELECT time, longitude, latitude, variable1, variable2, ...
FROM p_table, mask
WHERE 
AND p_table.time = mask.time
AND p_table.pid = mask.pid
```
                                
2- Value query
```sql
SELECT aggregation(variable)
FROM p_table
WHERE 
AND time in [input_time_range|time_query]
AND pid in [shape_to_pid_set(input_shape)|pixel_query]
AND variable [=|>|<] [input_value|value_query]


SELECT aggregation(variable)
FROM p_table mask
WHERE 
AND p_table.time = mask.time
AND p_table.pid = mask.pid
```
''')

# main layout
main_tabs = pn.Tabs(
    # ('0. Document', doc_markdown),
    ('1. Xarray Page', xa_page),
    ('2. Aggregation', agg_single_value_result),
    ('3. Time Series', ts_plotly_plot),
    ('5. Mask', mk_mask_table),
    ('6. Mask by Aggregation', ma_mask_table),
    ('9. SQL', sql_app),
)

# template
template = pn.template.BootstrapTemplate(
    title='IHARP ERA5 POC',
    theme_toggle=False,
    sidebar=[side_taps],
    main=[
        main_tabs,
        map_panel,
    ],
)

template.servable()