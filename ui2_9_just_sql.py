import clickhouse_connect
import panel as pn

client = clickhouse_connect.get_client(password='huan1531')

# main components
sql_input = pn.widgets.TextAreaInput(value='Input SQL here ...', width=700, height=100)
btn_sql_run = pn.widgets.Button(name='Run', button_type='primary')
sql_result = pn.widgets.Tabulator(None, show_index=False)
sql_app = pn.Column(
    sql_input,
    btn_sql_run,
    sql_result,
)


# botton on click
def run_sql(event):
    sql = sql_input.value
    df = client.query_df(sql)
    sql_result.value = df


btn_sql_run.on_click(run_sql)