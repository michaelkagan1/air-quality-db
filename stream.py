#DONE: separated pm2.5 and AQI explorer into expandable sections
#DONE: resolved insert updating bug
#DONE: moved aqi_df_pm25 to external func
# from connectdb import *
from connectdb import connect_db
from matplotlib import pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import pandas as pd
import streamlit as st
import datetime
from pathlib import Path

def dashboard():
    #define path
    path = Path().cwd()

    # set layout to use entire webpage instead of default column
    st.set_page_config(
        page_title="Air Quality Dashboard",
        layout='wide',
        initial_sidebar_state='expanded'
    )

    #create layout for streamlit page
    #    st.image(path/'sky.jpg')
    st.title("Air Quality in Capital Cities Around the World")
    st.markdown('###')

    # Establish connection and cursor with database as IAM user
    cnx, curs = connect_db()

    @st.cache_data      #prevents streamlit from rerunning following function more than once while data is static
    def query_to_df(query):
        return pd.read_sql_query(query, cnx)
        
    # query desired data from aqi as a pandas dataframe
    aqi_df = query_to_df(query_all_aqi())
    aqi_df2 = aqi_df.copy()

    # setup session state for expander
    if 'expander_state' not in st.session_state:
        st.session_state.expander_state = True

    # with st.expander('PM2.5 Daily and Historical Averages', expanded=st.session_state.expander_state):
    aqi_df2_latest_pm25, aqi_df_pivot = get_latest_pm25(aqi_df2)
    #apply metrics display at top of page
    maxdate = aqi_df2.datetime.max()
    top_3_metrics(maxdate, aqi_df2_latest_pm25)     
    st.markdown('')
    col1, col2 = st.columns(2, border=True)
    fig = plot_pm25_gdp(cnx)
    col1.plotly_chart(fig)
    col2.markdown('')
    col2.markdown('')
    col2.markdown("""
                #### PM2.5 refers to fine particulate matter smaller than 2.5 microns in diameter, which poses significant health risks as it can penetrate deep into the lungs and bloodstream. These solid and liquid particulates originate from vehicle exhaust, industrial emissions, and wildfires, among other sources. It is a critical air quality metric due to its association with respiratory, cardiovascular diseases, and premature death, making its monitoring essential for public health and environmental policies.
                Analysis: 
                To the left, you can see that there is an inverse relationship between GDP per capita and average PM 2.5. Overall, less developed countries have fewer systems in place and fewer resources to ensure and enforce pollution reducing measures.
                """)
    _, col, _ = st.columns([1,2,1]) 
    col.image("static/pm25info.jpg", width=800)

    st.markdown('---')

    # give option bar for countries, taken from aqi_df, in sidebar
    countries = aqi_df_pivot['country'].sort_values().unique()

    # st.sidebar.markdown('#\n#\n#\n#\n#')    #5 blank spaces
    st.sidebar.title('AQI Explorer')
    selected = st.sidebar.multiselect('Select countries', countries)
    if not selected:
        #collapse pm25 expander so user can see content
        st.session_state.expander_state = True

        st.sidebar.success('Select countries')
    st.sidebar.markdown('---')

    # filter out countries selected 
    if selected:
        #collapse pm25 expander so user can see content
        st.session_state.expander_state = False

        # filters out rows which contain the countries in the selected list. Drops any columns that don't have any valid values
        aqi_df_plot = aqi_df_pivot[aqi_df_pivot.country.isin(selected)].dropna(axis=1, how='all').sort_values(by=['country', 'datetime'])

        # select pollutant to view
        pollutants = aqi_df_plot.columns[2:]
        pollutant = st.sidebar.pills('Select a pollutant', options=pollutants, selection_mode='single')

        if pollutant:
            st.sidebar.markdown('---')
            # get measurement units, and display name for modifying xaxis label
            
            plot_aqi_explorer(curs, aqi_df_plot, pollutant)
            
        # show raw data below
        st.markdown('#####')
        st.write('##### Raw Data')
        st.write(aqi_df_plot)

def get_latest_pm25(aqi_df):

    #sort by country and date so lines don't spaghetti
    aqi_df.sort_values(by=['country', 'datetime'], inplace=True)
    aqi_df[aqi_df['avg_value'] < 0 ] = np.nan

    # aggregate any possible duplicates (of datetime, country, pollutant combination) and reset
    aqi_df = aqi_df.groupby(['datetime', 'country', 'pollutant']).mean().reset_index()

    # pivot table for graphing in plotly - each pollutant gets column
    aqi_df_pivot = aqi_df.pivot(index=['datetime', 'country'], columns='pollutant', values='avg_value').reset_index()
    aqi_df_pivot.sort_values(['country', 'datetime'], inplace=True)

    #only show latest pm25 row for each country
    maxdate = aqi_df_pivot.datetime.max()
    aqi_df_latest_pm25 = aqi_df_pivot[aqi_df_pivot.datetime == maxdate][['country', 'pm25']].dropna().sort_values(by='pm25') 

    if len(aqi_df_latest_pm25) < 6:
        mindate = maxdate - pd.Timedelta(days=1)
        aqi_df_latest_pm25 = aqi_df_pivot[(aqi_df_pivot.datetime <= maxdate) & (aqi_df_pivot.datetime >= mindate)]\
            [['country', 'pm25']].dropna().sort_values(by='pm25') 
    
    return aqi_df_latest_pm25, aqi_df_pivot
    




def top_3_metrics(date, aqi_df):      #takes dataframe with pm25 column
    #sticks top 3 and bottom 3 rows together
    top3 = pd.concat([aqi_df.head(3), aqi_df.tail(3)], axis=0)

    #today's date formatted
    date = datetime.datetime.today().strftime("%A, %B %d, %Y")
    st.markdown(f"<h3 style='text-align: center;'>{date}</h1>", unsafe_allow_html=True)
    st.markdown('---')

    left, right = st.columns(2)
    with left:
        st.markdown('##### Lowest 3 PM 2.5 (\u00b5g/m\u00b3)')
    with right:
        st.markdown('##### Highest 3 PM 2.5 (\u00b5g/m\u00b3)')

    cols = st.columns(6, border=True)
    for i, col in enumerate(cols):
        with col:
            pm25 = top3['pm25'].values[i].round(2)
            country = top3['country'].values[i]
            st.metric(label=country, value=pm25) #, border=True)
        
# gets avg pm2.5 data over time per country, with gdp per cap and region data from db
def plot_pm25_gdp(cnx):
    avg_pm25_gdp_df = pd.read_sql_query(query_avg_pm25_gdp(), cnx)
    avg_pm25_gdp_df['dummy_size'] = 1
    pm25_row = pd.read_sql_query("SELECT display_name, units FROM pollutants WHERE name = 'pm25' ", cnx)
    display_name, units = pm25_row.values[0]

    fig = px.scatter(avg_pm25_gdp_df, x='gdp_per_capita', y='avg_pm25', color='region', 
                    hover_name='country',
                    size='dummy_size',  # dummy column for size
                    size_max=11,
                    opacity=0.8,
                    title=f'Jan \'24 - Jan \'25 Average PM 2.5 vs. GDP Per Capita',
                    labels={
                        'avg_pm25': f'{display_name} ({units})', 
                        'gdp_per_capita': 'GDP Per Capita'
                        },
                    hover_data={'pollutant':False, 'dummy_size':False, 'country':False, },
               color_discrete_sequence=px.colors.qualitative.G10
                  )
    return fig

def plot_aqi_explorer(curs, aqi_df_plot, pollutant):
    curs.execute('SELECT display_name, units FROM pollutants WHERE name = %s', [pollutant,])
    display_name, units = curs.fetchall()[0]

    # apply mask to keep only rows w/ countries that have >= 1 measurement of selected pollutant
    mask = aqi_df_plot.groupby('country')[pollutant].transform(lambda x: not x.isna().all())
    aqi_df_plot = aqi_df_plot[mask]

    # establish min, max dates for slider defaults
    mindate, maxdate = aqi_df_plot.datetime.min().to_pydatetime(), aqi_df_plot.datetime.max().to_pydatetime()

    # use slider to select date range
    xrange = st.sidebar.slider("Select date range", 
                                mindate, maxdate,     # range to display on the slider
                                value=[mindate, maxdate])     # default selected range

    # find y range with selected range
    maxy = aqi_df_plot[aqi_df_plot['datetime'].between(xrange[0], xrange[1], inclusive='both')][pollutant].max()
    maxy = maxy*1.15    # add padding to upper range
    
    # add section title
    st.markdown('#####')
    st.write('##### Air Quality Explorer')
    # plotly instead of pyplot
    fig = px.line(aqi_df_plot, x='datetime', y=pollutant, color = 'country',
                range_x=xrange,
                range_y=(0,maxy),
                labels={
                    pollutant: f'{display_name} ({units})'
                })
    # add color bands to show different severity levels for PM2.5
    if pollutant == 'pm25':
        fig.add_trace(go.Scatter(x=[mindate, maxdate, maxdate, mindate], y=[0,0, 12,12], fill='toself', mode="none", fillcolor="rgba(160, 204, 93, .3)", line=dict(color='rgba(0,0,0,0)'), showlegend=False, name='Good', hoverinfo='skip'))
        fig.add_trace(go.Scatter(x=[mindate, maxdate, maxdate, mindate], y=[12,12, 35.5,35.5], fill='toself', mode="none", fillcolor="rgba(247, 207, 95, .3)", line=dict(color='rgba(0,0,0,0)'), showlegend=False, name='Moderate', hoverinfo='skip'))
        fig.add_trace(go.Scatter(x=[mindate, maxdate, maxdate, mindate], y=[35.5,35.5, 55.5,55.5], fill='toself', mode="none", fillcolor="rgba(253, 142, 82, .3)", line=dict(color='rgba(0,0,0,0)'), showlegend=False, name='Unhealthy for Sensitive Groups', hoverinfo='skip'))
        fig.add_trace(go.Scatter(x=[mindate, maxdate, maxdate, mindate], y=[55.5,55.5, 150.5,150.5], fill='toself', mode="none", fillcolor="rgba(241, 96, 96, .3)", line=dict(color='rgba(0,0,0,0)'), showlegend=False, name='Unhealthy', hoverinfo='skip'))
        fig.add_trace(go.Scatter(x=[mindate, maxdate, maxdate, mindate], y=[150.5,150.5, 250.5,250.5], fill='toself', mode="none", fillcolor="rgba(155, 115, 177, .3)", line=dict(color='rgba(0,0,0,0)'), showlegend=False, name='Very Unhealthy', hoverinfo='skip'))
        fig.add_trace(go.Scatter(x=[mindate, maxdate, maxdate, mindate], y=[250.5,250.5, 500,500], fill='toself', mode="none", fillcolor="rgba(148, 107, 121, .3)", line=dict(color='rgba(0,0,0,0)'), showlegend=False, name='Hazardous', hoverinfo='skip'))

    st.plotly_chart(fig)

def query_all_aqi():    #select all aqi data, avg by country datetime and pollutant, so one row per pollutant per country, per day
    query = """
        SELECT datetime, countries.country_name AS country, pollutants.name AS pollutant, ROUND(AVG(value), 2) AS avg_value
        FROM countries 
        JOIN locations ON countries.id = locations.country_id
        JOIN aqi ON locations.id = aqi.location_id
        JOIN pollutants on aqi.pollutant_id = pollutants.id
        GROUP BY datetime, country, pollutant
            """
    return query

def query_avg_pm25_gdp():
    query = """
        SELECT countries.country_name AS country, pollutants.name AS pollutant, ROUND(AVG(value),2) AS 'avg_pm25', gdp_per_capita, region
        FROM countries 
        JOIN locations ON countries.id = locations.country_id
        JOIN aqi ON locations.id = aqi.location_id
        JOIN pollutants on aqi.pollutant_id = pollutants.id
        WHERE pollutants.name = 'pm25'
        AND value < 300
        GROUP BY country
        HAVING avg_pm25 >0
        ORDER BY country;
            """
    return query

if __name__ == '__main__':
    dashboard()