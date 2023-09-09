import clickhouse_connect

import panel as pn

client = clickhouse_connect.get_client(password='huan1531')


def run_sql(event):
    sql = sql_input.value
    df = client.query_df(sql)
    sql_result.value = df


sql_input = pn.widgets.TextAreaInput(value='Input SQL here ...')
btn_sql_run = pn.widgets.Button(name='Run', button_type='primary')
btn_sql_run.on_click(run_sql)
sql_result = pn.widgets.Tabulator(None, show_index=False)

app_sql = pn.Column(
    sql_input,
    btn_sql_run,
    sql_result,
)

app_sql.servable()