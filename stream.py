from connectdb import connect_db
from matplotlib import pyplot as plt
import pandas as pd
import streamlit as st

def main():

    # Establish connection and cursor with database as IAM user
    cnx, curs = connect_db()
    
    # query desired data from aqi as a pandas dataframe
    query_str = query()
    df = query_to_df(curs, query_str)

    # transform data # limit to top 5 and bottom 5 countries
    dftop5 = df.iloc[:5] 
    dfbot5 = df.iloc[-5:] 
    df2 = pd.concat([dftop5, dfbot5], axis=0)
   # import pdb; pdb.set_trace()
    
    stream(df2.drop(columns = 'no. measurements'))

    # make graphs
    plt.bar(df2['name'], df2['avg pm2.5'])
    plt.xlabel('Country')
    plt.ylabel('Average PM2.5')
    plt.tick_params(axis='x', rotation=45)
    plt.grid(axis='y', zorder=0)    # set grid to be behind bars
    plt.title('Top and Bottom 5 Avg PM 2.5 by Country (Capital City)')
    plt.tight_layout()
    plt.show()

def query():
    query = """
        SELECT name, ROUND(AVG(value),2) AS 'avg_pm2.5', COUNT(value) AS 'number measurements'
        FROM countries 
        JOIN locations ON countries.id = locations.country_id
        JOIN aqi ON locations.id = aqi.location_id
        WHERE element_id = 2
        GROUP BY country_id
        HAVING `avg_pm2.5` > 0
        ORDER BY `avg_pm2.5` DESC
            """
    return query

def query_to_df(curs, query):
    #define query
       #execute in database
    curs.execute(query)

    #retrieve results and clear cursor
    data = curs.fetchall()

    #convert data to df
    df = pd.DataFrame(data, columns=['name', 'avg pm2.5', 'no. measurements'])
    return df
    

def stream(dataframe):
    # establish streamlit app
    if st.button('click me'):
        st.bar_chart(dataframe,
                     x='name',
                     y='avg pm2.5')
                     


    # stream with streamlit


if __name__ == '__main__':
    main()