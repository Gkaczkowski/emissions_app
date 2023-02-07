import streamlit as st
import pandas as pd
import snowflake.connector
import plotly.graph_objects as go

st.title('Carbon Intensity Comparison')


# Initialize connection.
# Uses st.experimental_singleton to only run once.
@st.experimental_singleton
def init_connection():
    return snowflake.connector.connect(
        **st.secrets["snowflake"], client_session_keep_alive=True
    )


conn = init_connection()


@st.experimental_memo
def fetch_sql_df(sql: str) -> pd.DataFrame:
    """
    query snowflake and return the result as a dataframe
    """
    with conn.cursor() as curr:
        curr = curr.execute(sql)
        results = curr.fetchall()
        cols = [c[0].lower() for c in curr.description]
        return pd.DataFrame(results, columns=cols)


@st.cache(ttl=24 * 60 * 60)
def load_data_1():
    sql_query_1 = 'SELECT emaps_carbonintensity_timestamp,emaps_carbonintensity_zone,' \
                  'carbon_intensity_tons_per_mwh  FROM "CASESTUDY_GARETH"."average_carbon_intensity";'
    df1 = fetch_sql_df(sql_query_1)
    df1['datetime'] = df1['emaps_carbonintensity_timestamp']
    df1.set_index(['datetime'], inplace=True)
    return df1


@st.cache(ttl=24 * 60 * 60)
def load_data_2():
    sql_query_2 = 'SELECT moers_timestamp,moer_tons_per_mwh,wattime_balancing_authority ' \
                  'FROM "CASESTUDY_GARETH"."marginal_operating_emissions_rate";'
    df2 = fetch_sql_df(sql_query_2)
    df2['datetime'] = df2['moers_timestamp']
    df2.set_index(['datetime'], inplace=True)
    return df2


@st.cache(ttl=24 * 60 * 60, show_spinner=False)
def aggregate_data(df1, df2):
    df3 = pd.concat([df1, df2], copy=False).sort_values(by='datetime')
    df3.index = pd.to_datetime(df3.index, utc=True)
    df3.index = df3.index.tz_convert("US/Pacific")
    df3 = df3.fillna(method='bfill')
    return df3


@st.cache(ttl=24 * 60 * 60, show_spinner=False)
def get_aggregated_data(data, target: str):
    df = data.groupby(pd.Grouper(freq=target)).mean()
    df['delta_marginal_vs_average_tons_per_mwh'] = df['moer_tons_per_mwh'] - df['carbon_intensity_tons_per_mwh']
    return df


data_load_state = st.text('Loading data...')
df1 = load_data_1()
df2 = load_data_2()
data = aggregate_data(df1, df2)
time_data = get_aggregated_data(data, 'M')
data_load_state.text("Done!")

if st.checkbox('Show raw data'):
    st.subheader('Raw data')
    st.write(time_data)

st.header('Carbon Intensity')
st.caption('This plot compares average carbon intensity to marginal operating emissions rate')
st.caption('- it shows the trends of carbon intensity over time, and indicates a trend that although average carbon '
           'intensity is falling, marginal emissions are rising')

st.caption('-- i.e. we need to use more non-renewables to generate more power ')
option = st.selectbox(
    'What timeframe would you like to view the data on?',
    ('Week', 'Month', 'Year'), key='1')
option_map = {'Week': 'W', 'Month': 'M', 'Year': 'Y'}
time_data = get_aggregated_data(data, option_map[option])
# st.area_chart(time_data, y=['moer_tons_per_mwh', 'carbon_intensity_tons_per_mwh'])

fig = go.Figure()
fig.add_trace(go.Scatter(x=time_data.index, y=time_data['moer_tons_per_mwh'], name='moer_tons_per_mwh',
                         fill='tonexty', mode='none'))
fig.add_trace(go.Scatter(x=time_data.index, y=time_data['carbon_intensity_tons_per_mwh'],
                         name='carbon_intensity_tons_per_mwh',
                         fill='tozeroy', mode='none'))
fig.update_xaxes(
    title_text='Time Frame',
    # title_font=dict(size=16, family='Verdana', color='black'))
)
fig.update_yaxes(
    title_text="tons/mWh",
    # title_font=dict(size=30, family='Verdana', color='black'))
)
fig.update_layout(legend=dict(
    orientation="h",
    yanchor="top",
    y=-0.2,
    xanchor="left",
    x=0
))

st.plotly_chart(
    fig,
    theme="streamlit",
    use_container_width=True
)

st.header('Delta Between Marginal Operating Emissions Rate and Average Carbon Intensity')
st.caption('This plot shows the delta between marginal emissions rate and average carbon intensity over time')
st.caption('- i.e. when is the least sustainable time to use additional energy')
option_1 = st.selectbox(
    'What timeframe would you like to view the data on?',
    ('Week', 'Month', 'Year'), key='2')
time_data = get_aggregated_data(data, option_map[option_1])
st.area_chart(time_data, y=['delta_marginal_vs_average_tons_per_mwh'])
