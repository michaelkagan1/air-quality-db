#DONE: changed values < 0 to nan


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
        page_title="Air Quality Tracker",
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
    aqi_df = query_to_df(query())
    aqi_df2 = aqi_df.copy()
 
    #sort by country and date so lines don't spaghetti
    aqi_df2.sort_values(by=['country', 'datetime'], inplace=True)
    aqi_df2[aqi_df2['value'] < 0 ] = np.nan

    # aggregate any possible duplicates (of datetime, country, pollutant combination) and reset
    aqi_df2 = aqi_df2.groupby(['datetime', 'country', 'pollutant']).mean().reset_index()

    # pivot table for graphing in plotly - each element gets column
    aqi_df2 = aqi_df2.pivot(index=['datetime', 'country'], columns='pollutant', values='value').reset_index()
    aqi_df2.sort_values(['country', 'datetime'], inplace=True)

    #only show latest pm25 row for each country
    maxdate = aqi_df2.datetime.max()
    aqi_df2_latest_pm25 = aqi_df2[aqi_df2.datetime == maxdate][['country', 'pm25']].dropna().sort_values(by='pm25') 

    #apply metrics display at top of page
    top_3_metrics(maxdate, aqi_df2_latest_pm25)     
    st.markdown('')
    col1, col2 = st.columns(2)
    col1.image("static/pm25info.jpg", width=500)
    col2.markdown("""
               #### PM2.5 refers to fine particulate matter smaller than 2.5 microns, which poses significant health risks as it can penetrate deep into the lungs and bloodstream. These solid and liquid particulates originate from vehicle exhaust, industrial emissions, and wildfires, among other sources. It is a critical air quality metric due to its association with respiratory, cardiovascular diseases, and premature death, making its monitoring essential for public health and environmental policies.
               """)

    #give option bar for countries, taken from aqi_df, in sidebar
    countries = aqi_df2['country'].sort_values().unique()
    # st.sidebar.markdown('#\n#\n#\n#\n#')    #5 blank spaces
    selected = st.sidebar.multiselect('Select countries', countries)
    st.sidebar.markdown('---')

    #filter out countries selected 
    if selected:
        aqi_df_plot = aqi_df2[aqi_df2.country.isin(selected)].dropna(axis=1, how='all')

        # select pollutant to view
        pollutants = aqi_df_plot.columns[2:]
        pollutant = st.sidebar.pills('Select a pollutant', options=pollutants, selection_mode='single')

        st.markdown('#####')
        st.write('##### Raw Data')
        st.write(aqi_df_plot)

        if pollutant:
            #get measurement units, and display name for modifying xaxis label
            curs.execute('SELECT displayName, units FROM elements WHERE name = %s', [pollutant,])
            displayname, units = curs.fetchall()[0]

            # plotly instead of pyplot
            fig = px.line(aqi_df_plot, x='datetime', y=pollutant, color = 'country',
                        labels={
                            pollutant: f'{displayname} ({units})'
                        })
            st.plotly_chart(fig, color='country')

def top_3_metrics(date, aqi_df):      #takes dataframe with pm25 column
    #sticks top 3 and bottom 3 rows together
    top3 = pd.concat([aqi_df.head(3), aqi_df.tail(3)], axis=0)

    #today's date
    date = date.strftime("%A, %B %d, %Y")
    st.markdown(f"<h3 style='text-align: center;'>{date}</h1>", unsafe_allow_html=True)
    st.markdown('######')

    left, right = st.columns(2)
    with left:
        st.markdown('##### Lowest 3 PM 2.5')
    with right:
        st.markdown('##### Highest 3 PM 2.5')

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    cols = [c1, c2, c3, c4, c5, c6]
    for i, col in enumerate(cols):
        with col:
            pm25 = top3['pm25'].values[i]
            country = top3['country'].values[i]
            st.metric(label=country, value=pm25, border=True)
        


#TODO: use pythonic library like sqlalchemy
def query():#
    #element id 2 is PM2.5
    query = """
        SELECT datetime, countries.name AS country, elements.name AS pollutant, value
        FROM countries 
        JOIN locations ON countries.id = locations.country_id
        JOIN aqi ON locations.id = aqi.location_id
        JOIN elements on aqi.element_id = elements.id
            """
    return query


if __name__ == '__main__':
    dashboard()