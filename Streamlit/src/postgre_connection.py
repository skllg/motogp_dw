import psycopg
import streamlit as st
import os
from dotenv import load_dotenv
import pandas as pd
load_dotenv()

def connect(): 
    db_host = os.getenv('POSTGRES_HOST')
    db_port = os.getenv('POSTGRES_PORT')
    db_name = os.getenv('POSTGRES_DB')
    db_user = os.getenv('POSTGRES_USER')
    db_password = os.getenv('POSTGRES_PASSWORD')

    conn = psycopg.connect(f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}") 
    
    return conn

def fetch_cummulative_sum_points(season, racing_class):
    conn = connect()
    cur = conn.cursor()
    # racing_class = "250cc_moto2"
    
    if (racing_class == "250cc_moto2" and season > 2009):
        racing_class="moto2"
    elif (racing_class == "250cc_moto2" and season <= 2009):
        racing_class = "250cc"

    if racing_class == "125cc_moto3" and season > 2011:
        racing_class="moto3"
    elif racing_class == "125cc_moto3" and season <= 2011:
        racing_class = "125cc"
    
    

    query = f"select distinct fr.num_round, dr.rider_full_name, SUM(dp.num_points) over (partition by fr.id_rider_fk order by fr.num_round) as cummulative_sum \
                from fact_results fr left join dim_riders dr ON dr.id_rider = fr.id_rider_fk \
                left join dim_positions dp on dp.id_position =fr.id_position_fk \
                left join dim_grand_prix dgp on dgp.id_grandprix = fr.id_grand_prix_fk\
                where fr.season = {season} and fr.racing_class = '{racing_class}'\
                group by fr.num_round, dgp.des_grandprix,dr.rider_full_name,dp.num_points, fr.id_result\
                order by fr.num_round, dr.rider_full_name"

    cur.execute(query)
    result_args = cur.fetchall()

    df_bh = pd.DataFrame(result_args,columns=['num_round', 'rider_name', 'cummulative_sum'])

    return df_bh


def fetch_season_bar_chart(season, racing_class):
    conn = connect()
    cur = conn.cursor()
    # racing_class = "250cc_moto2"
    # racing_class = str(racing_class)

    if (racing_class == "Intermediate" and season > 2009):
        racing_class="'moto2'"
    elif (racing_class == "Intermediate" and season <= 2009):
        racing_class = "'250cc'"

    if racing_class == "Lower Class" and season > 2011:
        racing_class="'moto3'"
    elif racing_class == "Lower Class" and season <= 2011:
        racing_class = "'125cc'"
    
    

    query = f"select dr.rider_full_name, sum(dp.num_points) as points  from fact_results f\
                left join dim_grand_prix dgp on f.id_grand_prix_fk = dgp.id_grandprix \
                left join dim_riders dr on  dr.id_rider = f.id_rider_fk \
                left join dim_positions dp on dp.id_position = f.id_position_fk \
                where f.racing_class = {racing_class} and f.season  = {season}\
                group by dr.rider_full_name\
                order by points desc;"

    cur.execute(query)
    result_args = cur.fetchall()

    df_bh = pd.DataFrame(result_args,columns=['rider_name', 'total_points'])

    return df_bh