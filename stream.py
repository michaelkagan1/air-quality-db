#TODO: look into using expander for sections
#DONE: added date selection slider, auto-scaled y-axis to fit xrange
#DONE: dropped countries from explorer that don't have selected pollutant

# from connectdb import *
from connectdb import connect_db
from matplotlib import pyplot as plt
import plotly.express as px
import numpy as np
import pandas as pd
import streamlit as st
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

    #sort by country and date so lines don't spaghetti
    aqi_df2.sort_values(by=['country', 'datetime'], inplace=True)
    aqi_df2[aqi_df2['value'] < 0 ] = np.nan

    # aggregate any possible duplicates (of datetime, country, pollutant combination) and reset
    aqi_df2 = aqi_df2.groupby(['datetime', 'country', 'pollutant']).mean().reset_index()

    # pivot table for graphing in plotly - each pollutant gets column
    aqi_df2 = aqi_df2.pivot(index=['datetime', 'country'], columns='pollutant', values='value').reset_index()
    aqi_df2.sort_values(['country', 'datetime'], inplace=True)

    #only show latest pm25 row for each country
    maxdate = aqi_df2.datetime.max()
    aqi_df2_latest_pm25 = aqi_df2[aqi_df2.datetime == maxdate][['country', 'pm25']].dropna().sort_values(by='pm25') 

    if len(aqi_df2_latest_pm25) < 6:
        mindate = maxdate - pd.Timedelta(days=1)
        aqi_df2_latest_pm25 = aqi_df2[(aqi_df2.datetime <= maxdate) & (aqi_df2.datetime >= mindate)]\
            [['country', 'pm25']].dropna().sort_values(by='pm25') 

    #apply metrics display at top of page
    top_3_metrics(maxdate, aqi_df2_latest_pm25)     
    st.markdown('')
    col1, col2 = st.columns(2, border=True)
    fig = plot_pm25_gdp(cnx)
    col1.plotly_chart(fig)
    col2.markdown('')
    col2.markdown('')
    col2.markdown("""
                #### PM2.5 refers to fine particulate matter smaller than 2.5 microns in diameter, which poses significant health risks as it can penetrate deep into the lungs and bloodstream. These solid and liquid particulates originate from vehicle exhaust, industrial emissions, and wildfires, among other sources. It is a critical air quality metric due to its association with respiratory, cardiovascular diseases, and premature death, making its monitoring essential for public health and environmental policies.
                """)
    _, col, _ = st.columns([1,4,1]) 
    col.image("static/pm25info.jpg", width=800)

    st.markdown('---')

    # give option bar for countries, taken from aqi_df, in sidebar
    countries = aqi_df2['country'].sort_values().unique()

    # st.sidebar.markdown('#\n#\n#\n#\n#')    #5 blank spaces
    selected = st.sidebar.multiselect('Select countries', countries)
    if not selected:
        st.sidebar.success('Select countries')
    st.sidebar.markdown('---')

    # filter out countries selected 
    if selected:
        # filters out rows which contain the countries in the selected list. Drops any columns that don't have any valid values
        aqi_df_plot = aqi_df2[aqi_df2.country.isin(selected)].dropna(axis=1, how='all')

        # select pollutant to view
        pollutants = aqi_df_plot.columns[2:]
        pollutant = st.sidebar.pills('Select a pollutant', options=pollutants, selection_mode='single')

        if pollutant:
            st.sidebar.markdown('---')
            # get measurement units, and display name for modifying xaxis label
            curs.execute('SELECT display_name, units FROM pollutants WHERE name = %s', [pollutant,])
            displayname, units = curs.fetchall()[0]

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
                            pollutant: f'{displayname} ({units})'
                        })

            st.plotly_chart(fig)

        #add raw data below

        st.markdown('#####')
        st.write('##### Raw Data')
        st.write(aqi_df_plot)


def top_3_metrics(date, aqi_df):      #takes dataframe with pm25 column
    #sticks top 3 and bottom 3 rows together
    top3 = pd.concat([aqi_df.head(3), aqi_df.tail(3)], axis=0)

    #today's date
    date = date.strftime("%A, %B %d, %Y")
    st.markdown(f"<h3 style='text-align: center;'>{date}</h1>", unsafe_allow_html=True)
    st.markdown('---')

    left, right = st.columns(2)
    with left:
        st.markdown('##### Lowest 3 PM 2.5 (\u00b5g/m\u00b3)')
    with right:
        st.markdown('##### Highest 3 PM 2.5 (\u00b5g/m\u00b3)')

    c1, c2, c3, c4, c5, c6 = st.columns(6, border=True)
    cols = [c1, c2, c3, c4, c5, c6]
    for i, col in enumerate(cols):
        with col:
            pm25 = top3['pm25'].values[i]
            country = top3['country'].values[i]
            st.metric(label=country, value=pm25) #, border=True)
        
# gets avg pm2.5 data over time per country, with gdp per cap and region data from db
def plot_pm25_gdp(cnx):
    avg_pm25_gdp_df = pd.read_sql_query(query_avg_pm25_gdp(), cnx)
    pm25_row = pd.read_sql_query("SELECT display_name, units FROM pollutants WHERE name = 'pm25' ", cnx)
    displayname, units = pm25_row.values[0]

    fig = px.scatter(avg_pm25_gdp_df, x='gdp_per_capita', y='avg_pm25', color='region', 
                     hover_name='country',
                     size=np.ones(len(avg_pm25_gdp_df))*5,
                     title='Average PM 2.5 Shows Inverse Relationship with GDP Per Capita',
               labels = {
                   'avg_pm25': f'{displayname} ({units})', 
                   'gdp_per_capita': 'GDP Per Capita'
               },
               color_discrete_sequence=px.colors.qualitative.G10
                  )
    return fig

def query_all_aqi():
    query = """
        SELECT datetime, countries.country_name AS country, pollutants.name AS pollutant, value
        FROM countries 
        JOIN locations ON countries.id = locations.country_id
        JOIN aqi ON locations.id = aqi.location_id
        JOIN pollutants on aqi.pollutant_id = pollutants.id
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
        GROUP BY country
        HAVING avg_pm25 >0
        ORDER BY country;
            """
    return query

if __name__ == '__main__':
    dashboard()