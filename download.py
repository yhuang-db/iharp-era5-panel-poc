import cdsapi
import pickle
import pandas as pd


def download(client, variable, year, month, day, f_name):
    client.retrieve(
        'reanalysis-era5-single-levels',
        {
            'product_type': 'reanalysis',
            'format': 'netcdf',
            'variable': variable,
            'year': year,
            'month': month,
            'day': day,
            'time': [str(i).zfill(2) + ':00' for i in range(0, 24)],
            'area': [90, -180, -90, 180],
        },
        f'data/download/{f_name}',
    )


if __name__ == "__main__":
    c = cdsapi.Client()

    popular_vars = pickle.load(open('data/pickle/popular_variable.pkl', 'rb'))
    start_date = '2022-01-01'
    end_date = '2022-04-30'

    # download daily
    i = 0
    for variable in popular_vars:
        i += 1
        for date in pd.date_range(start=start_date, end=end_date):
            date = date.strftime('%Y-%m-%d')
            year = date[:4]
            month = date[5:7]
            day = date[8:]
            print(f'\n{i}. Downloading {variable} for {date}')
            download(c, variable, year, month, day, f'{variable}_{date}.nc')
