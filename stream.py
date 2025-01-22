from connectdb import connect_db
from matplotlib import pyplot as plt
import plotly.express as px
import numpy as np
import pandas as pd
import streamlit as st
from pathlib import Path

def main():
    #define path
    path = Path().cwd()

    #create layout for streamlit page
#    st.image(path/'sky.jpg')
    st.image('sky.jpg')
    st.title("Air Quality Index in Capital Cities Around the World")
    st.write('')

    # Establish connection and cursor with database as IAM user
    cnx, curs = connect_db()
    
    # query desired data from aqi as a pandas dataframe
    query_str = query()

    @st.cache_data
    def query_to_df(query):
        return pd.read_sql_query(query, cnx)
        
    df = query_to_df(query())
    df2 = df.copy()
    
    #display raw data on page
    st.write('Raw data')
    st.dataframe(df2)

    #give option bar for countries, taken from df
    countries = df2['country'].sort_values().unique()
    selected = st.multiselect('Select a country', countries)

    #filter out countries selected and sort by country and date so lines don't spaghetti
    df2 = df2[df2.country.isin(selected)]
    df2.sort_values(by=['country', 'datetime'], inplace=True)
    df2[df2['pm2.5'] < 0 ] = np.nan
    

    # plotly instead of pyplot
    fig = px.line(df2, x='datetime', y='pm2.5', color = 'country')

    #TODO: figure out why no lines are plotting
    st.plotly_chart(fig, color='country')


#TODO: use pythonic library like sqlalchemy
def query():
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
    main()