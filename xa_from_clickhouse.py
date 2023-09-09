from datetime import datetime

import clickhouse_connect
import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
import xarray as xr

df_pid = pd.read_csv('data/vector/pid.csv')
gdf_pid = gpd.GeoDataFrame(df_pid, geometry=gpd.points_from_xy(x=df_pid.longitude, y=df_pid.latitude, crs=4326))


def get_pid_str(shape):
    print('start sjoin')
    gdf_sjoin = gdf_pid.sjoin(gpd.GeoDataFrame({'geometry': [shape]}, crs=4326), how='inner', predicate='within')
    print('end sjoin')
    pids_str = ", ".join(np.char.mod('%d', gdf_sjoin['pid'].values))
    return pids_str


def xarray_from_clickhouse(
    variables: list[str],
    start_time: datetime = None,
    end_time: datetime = None,
    shape: shapely.Geometry = None,
    value_criteria: list[tuple[str, str, float]] = None,
):
    sql = f'SELECT time, longitude, latitude, {", ".join(variables)}'
    sql += f'\nFROM pop'
    if start_time is not None and end_time is not None:
        sql += f"\nWHERE time BETWEEN '{start_time}' AND '{end_time}'"
    if shape is not None:
        min_lon, min_lat, max_lon, max_lat = shape.bounds
        sql += f'\nAND longitude BETWEEN {min_lon} AND {max_lon}'
        sql += f'\nAND latitude BETWEEN {min_lat} AND {max_lat}'
    if value_criteria is not None:
        for variable, predicate, value in value_criteria:
            sql += f'\nAND {variable} {predicate} {value}'

    print(sql)

    # query to df
    start = datetime.now()
    df = client.query_df(sql)
    print(f'\nQuery time: {datetime.now() - start}\n')

    # df to xarray
    start = datetime.now()
    df = df.set_index(['time', 'longitude', 'latitude'])
    ds = df.to_xarray()
    print(f'\nConvert time: {datetime.now() - start}\n')
    print(ds)


def xarray_mask_by_shape(
    ds: xr.Dataset,
    shape: shapely.Geometry,
):
    # TODO:
    pass


def xarray_from_clickhouse_pid(
    variables: list[str],
    start_time: datetime = None,
    end_time: datetime = None,
    shape: shapely.Geometry = None,
    value_criteria: list[tuple[str, str, float]] = None,
):
    sql = f'SELECT time, longitude, latitude, {", ".join(variables)}'
    sql += f'\nFROM pop_pid'
    if start_time is not None and end_time is not None:
        sql += f"\nWHERE time BETWEEN '{start_time}' AND '{end_time}'"
    if value_criteria is not None:
        for variable, predicate, value in value_criteria:
            sql += f'\nAND {variable} {predicate} {value}'
    if shape is not None:
        pids_str = get_pid_str(shape)
        sql += f'\nAND pid IN ({pids_str})'
    print(sql)

    # query to df
    start = datetime.now()
    df = client.query_df(sql)
    print(f'\nQuery time: {datetime.now() - start}\n')
    print(df.head())

    # df to xarray
    start = datetime.now()
    df = df.set_index(['time', 'longitude', 'latitude'])
    ds = df.to_xarray()
    print(f'\nConvert time: {datetime.now() - start}\n')
    print(ds)
    return ds


def agg_from_clickhouse_pid(
    mma: str,
    variable: str,
    start_time: datetime = None,
    end_time: datetime = None,
    shape: shapely.Geometry = None,
    value_criteria: list[tuple[str, str, float]] = None,
):
    sql = f'SELECT {mma}({variable}) AS {mma}_{variable}'
    sql += f'\nFROM pop_pid'
    if start_time is not None and end_time is not None:
        sql += f"\nWHERE time BETWEEN '{start_time}' AND '{end_time}'"
    if value_criteria is not None:
        for variable, predicate, value in value_criteria:
            sql += f'\nAND {variable} {predicate} {value}'
    if shape is not None:
        pids_str = get_pid_str(shape)
        sql += f'\nAND pid IN ({pids_str})'

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
    value_criteria: list[tuple[str, str, float]] = None,
):
    if time_unit not in ['year', 'month', 'week', 'day', 'hour']:
        raise ValueError('time_unit must be one of year, month, week, day, hour')
    time_trunc = f"date_trunc('{time_unit}', time) "
    agg_str = ', '.join([f'{agg}({variable}) AS {agg}_{variable}' for variable in variables])
    sql = f'SELECT {time_trunc} AS {time_unit}, {agg_str}'
    sql += f'\nFROM pop_pid'
    if start_time is not None and end_time is not None:
        sql += f"\nWHERE time BETWEEN '{start_time}' AND '{end_time}'"
    if value_criteria is not None:
        for variable, predicate, value in value_criteria:
            sql += f'\nAND {variable} {predicate} {value}'
    if shape is not None:
        pids_str = get_pid_str(shape)
        sql += f'\nAND pid IN ({pids_str})'
    sql += f'\nGROUP BY {time_trunc}'
    sql += f'\nORDER BY {time_trunc}'

    print(sql)

    df = client.query_df(sql)
    print(df.head())
    return df


if __name__ == '__main__':
    client = clickhouse_connect.get_client(password='huan1531')

    variables = ['t2m', 'tp', 'u10']
    start_time = datetime(2022, 5, 1)
    end_time = datetime(2022, 5, 2, 23, 59, 59)
    gdf = gpd.read_file('data/vector/tri.geojson')
    shape = gdf.geometry[0]
    criteria = [
        ('v10', '>', 0),
    ]

    # a = agg_from_clickhouse_pid(
    #     mma='min',
    #     variable='t2m',
    #     start_time=start_time,
    #     end_time=end_time,
    #     shape=shape,
    #     value_criteria=criteria,
    # )

    # xarray_from_clickhouse_pid(
    #     variables=variables,
    #     start_time=start_time,
    #     end_time=end_time,
    #     shape=shape,
    #     value_criteria=[('t2m', '=', a)],
    # )

    groupby_time_from_clickhouse_pid(
        variables=variables,
        time_unit='day',
        agg='avg',
        start_time=start_time,
        end_time=end_time,
        shape=shape,
        # value_criteria=criteria,
    )

    client.close()