from datetime import datetime

import clickhouse_connect
import geopandas as gpd
import numpy as np
import shapely

from ui2_global_preload import *

client = clickhouse_connect.get_client(password='huan1531')


def get_pid_str(shape):
    gdf_sjoin = gdf_pid.sjoin(gpd.GeoDataFrame({'geometry': [shape]}, crs=4326), how='inner', predicate='within')
    pids_str = ", ".join(np.char.mod('%d', gdf_sjoin['pid'].values))
    return pids_str


def xarray_from_clickhouse_pid(
    variables: list[str],
    start_time: datetime = None,
    end_time: datetime = None,
    shape: shapely.Geometry = None,
    value_criteria: list[tuple[str, str, float]] = [],
):
    sql = f'SELECT time, longitude, latitude, {", ".join(variables)}'
    sql += f'\nFROM pop_pid'
    if start_time is not None and end_time is not None:
        sql += f"\nWHERE time BETWEEN '{start_time}' AND '{end_time}'"
    for variable, predicate, value in value_criteria:
        sql += f'\nAND {variable} {predicate} {value}'
    if shape is not None:
        pids_str = get_pid_str(shape)
        sql += f'\nAND pid IN ({pids_str})'
    print(sql)

    # query to df
    df = client.query_df(sql)
    print(df.head())

    # df to xarray
    df = df.set_index(['time', 'longitude', 'latitude'])
    ds = df.to_xarray()
    print(ds)
    return ds


def agg_from_clickhouse_pid(
    mma: str,
    variable: str,
    start_time: datetime = None,
    end_time: datetime = None,
    shape: shapely.Geometry = None,
    value_criteria: list[tuple[str, str, float]] = [],
):
    sql = f'SELECT {mma}({variable}) AS {mma}_{variable}'
    sql += f'\nFROM pop_pid'
    if start_time is not None and end_time is not None:
        sql += f"\nWHERE time BETWEEN '{start_time}' AND '{end_time}'"
    for variable, predicate, value in value_criteria:
        sql += f'\nAND {variable} {predicate} {value}'
    if shape is not None:
        pids_str = get_pid_str(shape)
        sql += f'\nAND pid IN ({pids_str})'
    print(sql)

    res = client.query(sql)
    agg = res.first_row[0]
    print(agg)
    return agg


def groupby_time_from_clickhouse_pid(
    variables: list[str],
    time_unit: str,
    agg: str,
    start_time: datetime = None,
    end_time: datetime = None,
    shape: shapely.Geometry = None,
    value_criteria: list[tuple[str, str, float]] = [],
):
    if time_unit not in ['year', 'month', 'week', 'day', 'hour']:
        raise ValueError('time_unit must be one of year, month, week, day, hour')
    time_trunc = f"date_trunc('{time_unit}', time) "
    agg_str = ', '.join([f'{agg}If({variable}, isFinite({variable})) AS {agg}_{short_2_long_map[variable]}' for variable in variables])
    sql = f'SELECT {time_trunc} AS {time_unit}, {agg_str}'
    sql += f'\nFROM pop_pid'
    if start_time is not None and end_time is not None:
        sql += f"\nWHERE time BETWEEN '{start_time}' AND '{end_time}'"
    for variable, predicate, value in value_criteria:
        sql += f'\nAND {variable} {predicate} {value}'
    if shape is not None:
        pids_str = get_pid_str(shape)
        sql += f'\nAND pid IN ({pids_str})'
    sql += f'\nGROUP BY {time_trunc}'
    sql += f'\nORDER BY {time_trunc}'
    print(sql)

    df = client.query_df(sql)
    df = df.set_index(time_unit)
    print(df.head())
    return df


def groupby_pid_from_clickhouse_pid(
    variables: list[str],
    agg: str,
    start_time: datetime = None,
    end_time: datetime = None,
    shape: shapely.Geometry = None,
    value_criteria: list[tuple[str, str, float]] = [],
):
    agg_str = ', '.join([f'{agg}If({variable}, isFinite({variable})) AS {agg}_{variable}' for variable in variables])
    sql = f'SELECT longitude, latitude, {agg_str}'
    sql += f'\nFROM pop_pid'
    if start_time is not None and end_time is not None:
        sql += f"\nWHERE time BETWEEN '{start_time}' AND '{end_time}'"
    for variable, predicate, value in value_criteria:
        sql += f'\nAND {variable} {predicate} {value}'
    if shape is not None:
        pids_str = get_pid_str(shape)
        sql += f'\nAND pid IN ({pids_str})'
    sql += f'\nGROUP BY longitude, latitude'
    print(sql)

    # query to df
    df = client.query_df(sql)
    print(df.head())

    # df to xarray
    df = df.set_index(['longitude', 'latitude'])
    ds = df.to_xarray()
    print(ds)
    return ds


def gen_sql_for_simple_mask(
    mask_type: str,
    start_time: datetime = None,
    end_time: datetime = None,
    shape: shapely.Geometry = None,
    value_criteria: list[tuple[str, str, float]] = [],
):
    if mask_type == 'time':
        sql = f'SELECT DISTINCT time'
    elif mask_type == 'pixel':
        sql = f'SELECT DISTINCT pid'
    else:
        sql = f'SELECT time, pid'
    sql += f'\nFROM pop_pid'
    if start_time is not None and end_time is not None:
        sql += f"\nWHERE time BETWEEN '{start_time}' AND '{end_time}'"
    for variable, predicate, value in value_criteria:
        sql += f'\nAND {variable} {predicate} {value}'
    if shape is not None:
        pids_str = get_pid_str(shape)
        sql += f'\nAND pid IN ({pids_str})'
    if mask_type == 'time':
        sql += f'\nORDER BY time'
    print(sql)
    return sql


def gen_sql_for_agg_mask(
    mask_type: str,
    start_time: datetime = None,
    end_time: datetime = None,
    shape: shapely.Geometry = None,
    agg_value_criteria: list[tuple[str, str, str, float]] = None,
):
    agg_projection = ',\n'.join([f'{agg}If({variable}, isFinite({variable})) as {agg}_{short_2_long_map[variable]}' for agg, variable, _, _ in agg_value_criteria])
    if mask_type == 'time':
        sql = f'SELECT time, {agg_projection}'
    elif mask_type == 'pixel':
        sql = f'SELECT pid, {agg_projection}'
    else:
        raise ValueError('mask_type must be one of time, pixel')
    sql += f'\nFROM pop_pid'
    if start_time is not None and end_time is not None:
        sql += f"\nWHERE time BETWEEN '{start_time}' AND '{end_time}'"
    if shape is not None:
        pids_str = get_pid_str(shape)
        sql += f'\nAND pid IN ({pids_str})'
    sql += f'\nGROUP BY {mask_type}'
    having = '\nAND '.join([f'{agg}If({variable}, isFinite({variable})) {predicate} {value}' for agg, variable, predicate, value in agg_value_criteria])
    sql += f'\nHAVING {having}'
    if mask_type == 'time':
        sql += f'\nORDER BY time'
    print(sql)
    return sql


def gen_simple_mask(
    mask_type: str,
    start_time: datetime = None,
    end_time: datetime = None,
    shape: shapely.Geometry = None,
    value_criteria: list[tuple[str, str, float]] = [],
):
    sql = gen_sql_for_simple_mask(mask_type, start_time, end_time, shape, value_criteria)
    df = client.query_df(sql)
    print(df.head())
    return df


def gen_agg_mask(
    mask_type: str,
    start_time: datetime = None,
    end_time: datetime = None,
    shape: shapely.Geometry = None,
    agg_value_criteria: list[tuple[str, str, str, float]] = None,
):
    sql = gen_sql_for_agg_mask(mask_type, start_time, end_time, shape, agg_value_criteria)
    df = client.query_df(sql)
    print(df.head())
    return df


def xarray_from_simple_mask(
    variables: list[str],
    mask_type: str,
    start_time: datetime = None,
    end_time: datetime = None,
    shape: shapely.Geometry = None,
    value_criteria: list[tuple[str, str, float]] = [],
):
    mask_sql = gen_sql_for_simple_mask(mask_type, start_time, end_time, shape, value_criteria)
    sql = f'SELECT time, longitude, latitude, {", ".join(variables)}'
    sql += f'\nFROM pop_pid p, ({mask_sql}) m'
    if mask_type == 'time':
        sql += f'\nWHERE p.time = m.time'
    elif mask_type == 'pid':
        sql += f'\nWHERE p.pid = m.pid'
    else:
        sql += f'\nWHERE p.time = m.time AND p.pid = m.pid'
    if start_time is not None and end_time is not None:
        sql += f"\nAND p.time BETWEEN '{start_time}' AND '{end_time}'"
    if shape is not None:
        pids_str = get_pid_str(shape)
        sql += f'\nAND p.pid IN ({pids_str})'
    print(sql)

    # query to df
    df = client.query_df(sql)
    print(df.head())

    # df to xarray
    df = df.set_index(['time', 'longitude', 'latitude'])
    ds = df.to_xarray()
    print(ds)
    return ds


def xarray_from_agg_mask(
    variables: list[str],
    mask_type: str,
    start_time: datetime = None,
    end_time: datetime = None,
    shape: shapely.Geometry = None,
    agg_value_criteria: list[tuple[str, str, str, float]] = None,
):
    mask_sql = gen_sql_for_agg_mask(mask_type, start_time, end_time, shape, agg_value_criteria)
    sql = f'SELECT time, longitude, latitude, {", ".join(variables)}'
    sql += f'\nFROM pop_pid p, ({mask_sql}) m'
    if mask_type == 'time':
        sql += f'\nWHERE p.time = m.time'
    elif mask_type == 'pid':
        sql += f'\nWHERE p.pid = m.pid'
    else:
        sql += f'\nWHERE p.time = m.time AND p.pid = m.pid'
    if start_time is not None and end_time is not None:
        sql += f"\nAND p.time BETWEEN '{start_time}' AND '{end_time}'"
    if shape is not None:
        pids_str = get_pid_str(shape)
        sql += f'\nAND p.pid IN ({pids_str})'
    print(sql)

    # query to df
    df = client.query_df(sql)
    print(df.head())

    # df to xarray
    df = df.set_index(['time', 'longitude', 'latitude'])
    ds = df.to_xarray()
    print(ds)
    return ds
