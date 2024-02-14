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


def fetch_cummulative_sum_points_constructors(season, racing_class):
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
    
    

    
    query = f"WITH BestResults AS ( \
                select\
                    distinct\
                    dc.des_constructor,\
                    fr.num_round,\
                    fr.race_type,\
                    fr.season,\
                    MAX(dp.num_points) AS best_points\
                FROM\
                    fact_results fr\
                JOIN dim_positions dp ON fr.id_position_fk = dp.id_position\
                left join dim_riders dr on dr.id_rider = fr.id_rider_fk\
                left join dim_teams dt on dt.id_team = dr.id_team_fk\
                left join dim_constructors dc on dc.id_constructor = dt.id_constructor_fk\
                where dr.season={season} and dr.racing_class='{racing_class}'\
                GROUP by \
                    dc.des_constructor,\
                    fr.num_round,\
                    fr.race_type,\
                    fr.season \
                )\
                select\
                distinct\
                    dc.des_constructor,\
                    dc.season,\
                    SUM(br.best_points) AS total_points\
                FROM\
                    dim_constructors dc \
                JOIN BestResults br ON dc.des_constructor  = br.des_constructor AND dc.season = br.season\
                GROUP BY\
                    dc.id_constructor,\
                    dc.season\
                order by total_points desc"

    cur.execute(query)
    result_args = cur.fetchall()

    df_bh = pd.DataFrame(result_args,columns=['des_constructor', 'season', 'total_points'])

    return df_bh


def fetch_cummulative_sum_points_teams(season, racing_class):
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
    
    

    
    query = f" SELECT \
                    des_team,\
                    season,\
                    SUM(total_best_points) AS total_points_across_rounds\
                FROM (\
                    SELECT \
                        dt.des_team,\
                        fr.season,\
                        fr.num_round,\
                        dr.rider_full_name,\
                        fr.race_type,\
                        ROW_NUMBER() OVER (PARTITION BY dt.des_team, fr.num_round, fr.race_type ORDER BY dp.num_points DESC) AS rank,\
                        SUM(dp.num_points) AS total_best_points\
                    FROM \
                        fact_results fr\
                    JOIN \
                        dim_positions dp ON fr.id_position_fk = dp.id_position\
                    LEFT JOIN \
                        dim_riders dr ON dr.id_rider = fr.id_rider_fk\
                    LEFT JOIN \
                        dim_teams dt ON dt.id_team = dr.id_team_fk\
                    WHERE \
                        dr.season = {season} \
                        AND dr.racing_class = '{racing_class}'\
                        and fr.num_round = ANY (dr.rounds_participated)\
                        AND fr.is_wildcard = false\
                    GROUP BY\
                        dt.des_team,\
                        fr.season,\
                        fr.num_round,\
                        dr.rider_full_name,\
                        fr.race_type,\
                        dp.num_points\
                ) AS ranked_points\
                WHERE rank <= 2\
                GROUP BY \
                    des_team, \
                    season\
                order by total_points_across_rounds desc;"

    cur.execute(query)
    result_args = cur.fetchall()

    df_bh = pd.DataFrame(result_args,columns=['des_team', 'season', 'total_points'])

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
    
    

    query = f"select rider_full_name, points from (select dr.rider_full_name, sum(dp.num_points) as points  from fact_results f\
                left join dim_grand_prix dgp on f.id_grand_prix_fk = dgp.id_grandprix \
                left join dim_riders dr on  dr.id_rider = f.id_rider_fk \
                left join dim_positions dp on dp.id_position = f.id_position_fk \
                where f.racing_class = {racing_class} and f.season  = {season} \
                group by dr.rider_full_name\
                order by points desc)\
                where points > 0;"

    cur.execute(query)
    result_args = cur.fetchall()

    df_bh = pd.DataFrame(result_args,columns=['rider_name', 'total_points'])

    return df_bh

def fetch_track_location(season):
    conn = connect()
    cur = conn.cursor()
    
    
    

    query = f"select des_track, dt.longitude  ,dt.latitude from dim_grand_prix dgp\
            left join  dim_tracks dt on id_track_fk =id_track\
            where season = {season}"

    cur.execute(query)
    result_args = cur.fetchall()

    df_tracks = pd.DataFrame(result_args,columns=['des_track', 'longitude', 'latitude'])

    return df_tracks


def fetch_rider_location(season):
    conn = connect()
    cur = conn.cursor()
    
    
    

    query = f"select distinct rider_full_name, birth_latitude, birth_longitude  from dim_riders dr \
        where season = {season} and birth_latitude is not null and  birth_longitude is not null"

    cur.execute(query)
    result_args = cur.fetchall()

    df_tracks = pd.DataFrame(result_args,columns=['rider_full_name', 'longitude', 'latitude'])

    return df_tracks