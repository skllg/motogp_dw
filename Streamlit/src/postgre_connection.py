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

    # db_host = st.secrets['POSTGRES_HOST']
    # db_port = st.secrets['POSTGRES_PORT']
    # db_name = st.secrets['POSTGRES_DB']
    # db_user =st.secrets['POSTGRES_USER']
    # db_password = st.secrets['POSTGRES_PASSWORD']

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
                having SUM(total_best_points) >0 \
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

def fetch_total_num_gp(season):

    conn = connect()
    cur = conn.cursor()
    ini = season[0]
    end = season[1]

    season_proc = '('
    for x in range (ini, end+1):
        season_proc = season_proc + str(x) + ','

    season_proc = season_proc[:-1]
    season_proc = season_proc+ ')'
        

    query = f"select count(id_grandprix) as num_grand_prix from dim_grand_prix dgp \
                where season in {season_proc}"

    cur.execute(query)
    result_args = cur.fetchone()


    return result_args

def fetch_HP_races(season,racing_class):
    conn = connect()
    cur = conn.cursor()
    ini = season[0]
    end = season[1]

    season_proc = '('
    for x in range (ini, end+1):
        season_proc = season_proc + str(x) + ','

    season_proc = season_proc[:-1]
    season_proc = season_proc+ ')'

    if racing_class=='Any':
        racing_class= ('motogp', '250cc','moto2', '125cc','moto3', 'moto-e')
    
    elif (racing_class == "250cc_moto2"):
        racing_class=('moto2','250cc')
        
    elif racing_class == "125cc_moto3":
        racing_class=('moto3','125cc')
    elif racing_class == "motogp":
        racing_class=('motogp','')
    else:
        racing_class=('moto-e','')
   

    query = f"select count(distinct id_grandprix) as half_point_races from dim_grand_prix dgp \
                inner join fact_results fr on fr.id_grand_prix_fk = dgp.id_grandprix \
                left join dim_positions dp on fr.id_position_fk = dp.id_position \
                where dp.bool_half_points = true and dgp.season in {season_proc} \
                and fr.racing_class in {racing_class}"

    cur.execute(query)
    result_args = cur.fetchone()


    return result_args

def fetch_night_races(season):
    conn = connect()
    cur = conn.cursor()
    ini = season[0]
    end = season[1]

    season_proc = '('
    for x in range (ini, end+1):
        season_proc = season_proc + str(x) + ','

    season_proc = season_proc[:-1]
    season_proc = season_proc+ ')'
        

    query = f"select count(distinct id_grandprix) as night_races from dim_grand_prix dgp \
                where dgp.night_race = true and dgp.season in {season_proc}"

    cur.execute(query)
    result_args = cur.fetchone()


    return result_args

def fetch_satruday_races(season):
    conn = connect()
    cur = conn.cursor()
    ini = season[0]
    end = season[1]

    season_proc = '('
    for x in range (ini, end+1):
        season_proc = season_proc + str(x) + ','

    season_proc = season_proc[:-1]
    season_proc = season_proc+ ')'
        

    query = f"select count(distinct id_grandprix) as night_races from dim_grand_prix dgp \
  	        where dgp.saturday_race  = true and dgp.season in {season_proc}"

    cur.execute(query)
    result_args = cur.fetchone()


    return result_args
  
def fetch_num_postions(season,racing_class):
    
    conn = connect()
    cur = conn.cursor()
    
    if season=='any':
        season= (2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021,2022,2023)
    
        if racing_class=='any':
            racing_class= ("motogp", "250cc","moto2", "125cc","moto3", "moto-e")
        else:
            if (racing_class == "250cc_moto2"):
                racing_class=('moto2','250cc')
                
            if racing_class == "125cc_moto3":
                racing_class=('moto3','125cc')

    else:
        season = '('+ season +')'
        if racing_class=='any':
                racing_class= ("motogp", "250cc","moto2", "125cc","moto3", "moto-e")
        else:
            if (racing_class == "250cc_moto2" and season > 2009):
                racing_class=('moto2')
            elif (racing_class == "250cc_moto2" and season <= 2009):
                racing_class = ('250cc')
                
            elif racing_class == "125cc_moto3" and season > 2011:
                racing_class=('moto3')
            elif racing_class == "125cc_moto3" and season <= 2011:
                racing_class = ('125cc')

            elif racing_class == "motogp":
                racing_class = ('motogp')

            elif racing_class == "motoe":
                racing_class = ('motoe')

    query = f"select count(id_grandprix)"

    cur.execute(query)
    result_args = cur.fetchall()

    df_tracks = pd.DataFrame(result_args,columns=['rider_full_name', 'longitude', 'latitude'])

    return df_tracks

def fetch_top_wins(season,racing_class):

    conn = connect()
    cur = conn.cursor()
    
    ini = season[0]
    end = season[1]

    season_proc = '('
    for x in range (ini, end+1):
        season_proc = season_proc + str(x) + ','

    season_proc = season_proc[:-1]
    season_proc = season_proc+ ')'

    if racing_class=='Any':
        racing_class= ('motogp', '250cc','moto2', '125cc','moto3', 'moto-e')
    
    elif (racing_class == "250cc_moto2"):
        racing_class=('moto2','250cc')
        
    elif racing_class == "125cc_moto3":
        racing_class=('moto3','125cc')
    elif racing_class == "motogp":
        racing_class=('motogp','')
    else:
        racing_class=('moto-e','')


    query = f"select dr.rider_full_name,  count(fr.id_result) num_wins from fact_results fr \
            left join dim_riders dr on fr.id_rider_fk = dr.id_rider \
            left join dim_positions dp on dp.id_position =fr.id_position_fk \
            where dp.final_position = '1' and fr.racing_class  in {racing_class} and fr.season in {season_proc}\
            and fr.race_type='main'\
            group by dr.rider_full_name\
            order by num_wins desc\
            limit 10"
        

    cur.execute(query)
    result_args = cur.fetchall()

    df_top_wins = pd.DataFrame(result_args,columns=['rider name', 'number of wins'])

    return df_top_wins
      
def fetch_top_podiums(season,racing_class):

    conn = connect()
    cur = conn.cursor()
    
    ini = season[0]
    end = season[1]

    season_proc = '('
    for x in range (ini, end+1):
        season_proc = season_proc + str(x) + ','

    season_proc = season_proc[:-1]
    season_proc = season_proc+ ')'

    if racing_class=='Any':
        racing_class= ('motogp', '250cc','moto2', '125cc','moto3', 'moto-e')
    
    elif (racing_class == "250cc_moto2"):
        racing_class=('moto2','250cc')
        
    elif racing_class == "125cc_moto3":
        racing_class=('moto3','125cc')
    elif racing_class == "motogp":
        racing_class=('motogp','')
    else:
        racing_class=('moto-e','')


    query = f"select dr.rider_full_name,  count(fr.id_result) num_wins from fact_results fr \
            left join dim_riders dr on fr.id_rider_fk = dr.id_rider \
            left join dim_positions dp on dp.id_position =fr.id_position_fk \
            where dp.final_position in ('1','2','3') \
            and fr.racing_class  in {racing_class} and fr.season in {season_proc}\
            and fr.race_type='main'\
            group by dr.rider_full_name\
            order by num_wins desc\
            limit 10"
        

    cur.execute(query)
    result_args = cur.fetchall()

    df_top_podiums= pd.DataFrame(result_args,columns=['rider name', 'number of podiums'])

    return df_top_podiums
        
  
def fetch_top_wins_sprint(season,racing_class):

    conn = connect()
    cur = conn.cursor()
    
    ini = season[0]
    end = season[1]

    season_proc = '('
    for x in range (ini, end+1):
        season_proc = season_proc + str(x) + ','

    season_proc = season_proc[:-1]
    season_proc = season_proc+ ')'

    if racing_class=='Any':
        racing_class= ('motogp', '250cc','moto2', '125cc','moto3', 'moto-e')
    
    elif (racing_class == "250cc_moto2"):
        racing_class=('moto2','250cc')
        
    elif racing_class == "125cc_moto3":
        racing_class=('moto3','125cc')
    elif racing_class == "motogp":
        racing_class=('motogp','')
    else:
        racing_class=('moto-e','')


    query = f"select dr.rider_full_name,  count(fr.id_result) num_wins from fact_results fr \
            left join dim_riders dr on fr.id_rider_fk = dr.id_rider \
            left join dim_positions dp on dp.id_position =fr.id_position_fk \
            where dp.final_position = '1' and fr.racing_class  in {racing_class} and fr.season in {season_proc}\
            and fr.race_type='sprint'\
            group by dr.rider_full_name\
            order by num_wins desc\
            limit 10"
        

    cur.execute(query)
    result_args = cur.fetchall()

    df_top_wins = pd.DataFrame(result_args,columns=['rider name', 'number of sprint wins'])

    return df_top_wins
      
def fetch_top_podiums_sprint(season,racing_class):

    conn = connect()
    cur = conn.cursor()
    
    ini = season[0]
    end = season[1]

    season_proc = '('
    for x in range (ini, end+1):
        season_proc = season_proc + str(x) + ','

    season_proc = season_proc[:-1]
    season_proc = season_proc+ ')'

    if racing_class=='Any':
        racing_class= ('motogp', '250cc','moto2', '125cc','moto3', 'moto-e')
    
    elif (racing_class == "250cc_moto2"):
        racing_class=('moto2','250cc')
        
    elif racing_class == "125cc_moto3":
        racing_class=('moto3','125cc')
    elif racing_class == "motogp":
        racing_class=('motogp','')
    else:
        racing_class=('moto-e','')


    query = f"select dr.rider_full_name,  count(fr.id_result) num_wins from fact_results fr \
            left join dim_riders dr on fr.id_rider_fk = dr.id_rider \
            left join dim_positions dp on dp.id_position =fr.id_position_fk \
            where dp.final_position in ('1','2','3') \
            and fr.racing_class  in {racing_class} and fr.season in {season_proc}\
            and fr.race_type='sprint'\
            group by dr.rider_full_name\
            order by num_wins desc\
            limit 10"
        

    cur.execute(query)
    result_args = cur.fetchall()

    df_top_podiums= pd.DataFrame(result_args,columns=['rider name', 'number of sprint podiums'])

    return df_top_podiums
        
def fetch_top_poles(season,racing_class):

    conn = connect()
    cur = conn.cursor()
    
    ini = season[0]
    end = season[1]

    season_proc = '('
    for x in range (ini, end+1):
        season_proc = season_proc + str(x) + ','

    season_proc = season_proc[:-1]
    season_proc = season_proc+ ')'

    if racing_class=='Any':
        racing_class= ('motogp', '250cc','moto2', '125cc','moto3', 'moto-e')
    
    elif (racing_class == "250cc_moto2"):
        racing_class=('moto2','250cc')
        
    elif racing_class == "125cc_moto3":
        racing_class=('moto3','125cc')
    elif racing_class == "motogp":
        racing_class=('motogp','')
    else:
        racing_class=('moto-e','')


    query = f"select dr.rider_full_name,  count(fr.id_result) num_wins from fact_results fr \
            left join dim_riders dr on fr.id_rider_fk = dr.id_rider \
            left join dim_positions dp on dp.id_position =fr.id_position_fk \
            where fr.pole = True and fr.racing_class in {racing_class} and fr.season in {season_proc}\
            and fr.race_type='sprint'\
            group by dr.rider_full_name\
            order by num_wins desc\
            limit 10"
        

    cur.execute(query)
    result_args = cur.fetchall()

    df_top_poles = pd.DataFrame(result_args,columns=['rider name', 'number of poles'])

    return df_top_poles
      
def fetch_top_fast_laps(season,racing_class):

    conn = connect()
    cur = conn.cursor()
    
    ini = season[0]
    end = season[1]

    season_proc = '('
    for x in range (ini, end+1):
        season_proc = season_proc + str(x) + ','

    season_proc = season_proc[:-1]
    season_proc = season_proc+ ')'

    if racing_class=='Any':
        racing_class= ('motogp', '250cc','moto2', '125cc','moto3', 'moto-e')
    
    elif (racing_class == "250cc_moto2"):
        racing_class=('moto2','250cc')
        
    elif racing_class == "125cc_moto3":
        racing_class=('moto3','125cc')
    elif racing_class == "motogp":
        racing_class=('motogp','')
    else:
        racing_class=('moto-e','')


    query = f"select dr.rider_full_name,  count(fr.id_result) num_wins from fact_results fr \
            left join dim_riders dr on fr.id_rider_fk = dr.id_rider \
            left join dim_positions dp on dp.id_position =fr.id_position_fk \
            where fr.fastest_lap = True \
            and fr.racing_class  in {racing_class} and fr.season in {season_proc}\
            and fr.race_type='sprint'\
            group by dr.rider_full_name\
            order by num_wins desc\
            limit 10"
        

    cur.execute(query)
    result_args = cur.fetchall()

    top_fast_laps= pd.DataFrame(result_args,columns=['rider name', 'number of fastest laps'])

    return top_fast_laps
            
def fetch_top_percentage_points(season,racing_class):
    conn = connect()
    cur = conn.cursor()
    
    ini = season[0]
    end = season[1]

    season_proc = '('
    for x in range (ini, end+1):
        season_proc = season_proc + str(x) + ','

    season_proc = season_proc[:-1]
    season_proc = season_proc+ ')'

    if racing_class=='Any':
        racing_class= ('motogp', '250cc','moto2', '125cc','moto3', 'moto-e')
    
    elif (racing_class == "250cc_moto2"):
        racing_class=('moto2','250cc')
        
    elif racing_class == "125cc_moto3":
        racing_class=('moto3','125cc')
    elif racing_class == "motogp":
        racing_class=('motogp','')
    else:
        racing_class=('moto-e','')


    query = f"SELECT \
                    subquery.season, \
                    subquery.racing_class,\
                    subquery.rider_full_name,\
                    ROUND((subquery.points*100)/max_points.max_points, 2) as percentage_points\
                FROM (\
                    SELECT \
                        dr.rider_full_name, \
                        f.racing_class,\
                        f.season, \
                        SUM(dp.num_points) AS points,\
                        ROW_NUMBER() OVER (PARTITION BY f.season,f.racing_class ORDER BY SUM(dp.num_points) DESC) AS rn\
                    FROM \
                        fact_results f\
                        LEFT JOIN dim_grand_prix dgp ON f.id_grand_prix_fk = dgp.id_grandprix \
                        LEFT JOIN dim_riders dr ON dr.id_rider = f.id_rider_fk \
                        LEFT JOIN dim_positions dp ON dp.id_position = f.id_position_fk \
                    WHERE \
                        f.racing_class in {racing_class} and f.season in {season_proc}\
                    GROUP BY \
                        dr.rider_full_name,f.racing_class, f.season\
                ) AS subquery\
                JOIN (\
                    SELECT dgp.season,\
                        CASE \
                            WHEN dgp.season = 2023 THEN COUNT(dgp.num_round) * 36 \
                            ELSE COUNT(dgp.num_round) * 25 \
                        END AS max_points\
                    FROM \
                        dim_grand_prix dgp \
                    GROUP BY \
                        dgp.season\
                ) AS max_points ON subquery.season = max_points.season \
                WHERE \
                    rn = 1 and subquery.rider_full_name not like '%Bekker%'\
                ORDER BY \
                    percentage_points ASC\
                limit 100"
        

    cur.execute(query)
    result_args = cur.fetchall()

    df_top_podiums= pd.DataFrame(result_args,columns=[ 'season','racing class','rider name', 'percentage of total points'])

    return df_top_podiums

def fetch_top_points_carrer(season, racing_class):
    conn = connect()
    cur = conn.cursor()
    ini = season[0]
    end = season[1]

    season_proc = '('
    for x in range (ini, end+1):
        season_proc = season_proc + str(x) + ','

    season_proc = season_proc[:-1]
    season_proc = season_proc+ ')'

    if racing_class=='Any':
        racing_class= ('motogp', '250cc','moto2', '125cc','moto3', 'moto-e')
    
    elif (racing_class == "250cc_moto2"):
        racing_class=('moto2','250cc')
        
    elif racing_class == "125cc_moto3":
        racing_class=('moto3','125cc')
    elif racing_class == "motogp":
        racing_class=('motogp','')
    else:
        racing_class=('moto-e','')
    

    query = f"select rider_full_name, points from (select dr.rider_full_name, sum(dp.num_points) as points  from fact_results f\
                left join dim_grand_prix dgp on f.id_grand_prix_fk = dgp.id_grandprix \
                left join dim_riders dr on  dr.id_rider = f.id_rider_fk \
                left join dim_positions dp on dp.id_position = f.id_position_fk \
                where f.racing_class in {racing_class}\
                and dr.rider_full_name not like '%Bekker%'\
                group by dr.rider_full_name\
                order by points desc)\
                where points > 0\
                limit 20;"

    cur.execute(query)
    result_args = cur.fetchall()

    df_bh = pd.DataFrame(result_args,columns=['rider_name', 'total points carreer'])

    return df_bh


def fetch_top_points_constructor(season, racing_class):
    conn = connect()
    cur = conn.cursor()
    ini = season[0]
    end = season[1]

    season_proc = '('
    for x in range (ini, end+1):
        season_proc = season_proc + str(x) + ','

    season_proc = season_proc[:-1]
    season_proc = season_proc+ ')'

    if racing_class=='Any':
        racing_class= ('motogp', '250cc','moto2', '125cc','moto3', 'moto-e')
    
    elif (racing_class == "250cc_moto2"):
        racing_class=('moto2','250cc')
        
    elif racing_class == "125cc_moto3":
        racing_class=('moto3','125cc')
    elif racing_class == "motogp":
        racing_class=('motogp','')
    else:
        racing_class=('moto-e','')
    

    query = f"   select des_constructor, points from (select  dc.des_constructor, sum(dp.num_points) as points  from fact_results f\
                left join dim_grand_prix dgp on f.id_grand_prix_fk = dgp.id_grandprix \
                left join dim_riders dr on  dr.id_rider = f.id_rider_fk \
                left join dim_teams dt on dt.id_team = dr.id_team_fk\
                left join dim_constructors dc on dc.id_constructor = dt.id_constructor_fk\
                left join dim_positions dp on dp.id_position = f.id_position_fk \
                where f.racing_class in {racing_class} and f.season in {season_proc}\
                and f.num_round = ANY (dr.rounds_participated)\
                group by  dc.des_constructor\
                order by points desc)\
                where points > 0\
                limit 20;"

    cur.execute(query)
    result_args = cur.fetchall()

    df_bh = pd.DataFrame(result_args,columns=['constructor name', 'total points constructor'])

    return df_bh


def fetch_top_wins_constructor(season,racing_class):

    conn = connect()
    cur = conn.cursor()
    
    ini = season[0]
    end = season[1]

    season_proc = '('
    for x in range (ini, end+1):
        season_proc = season_proc + str(x) + ','

    season_proc = season_proc[:-1]
    season_proc = season_proc+ ')'

    if racing_class=='Any':
        racing_class= ('motogp', '250cc','moto2', '125cc','moto3', 'moto-e')
    
    elif (racing_class == "250cc_moto2"):
        racing_class=('moto2','250cc')
        
    elif racing_class == "125cc_moto3":
        racing_class=('moto3','125cc')
    elif racing_class == "motogp":
        racing_class=('motogp','')
    else:
        racing_class=('moto-e','')


    query = f"select dc.des_constructor,  count(fr.id_result) num_wins from fact_results fr \
            left join dim_riders dr on fr.id_rider_fk = dr.id_rider \
            left join dim_positions dp on dp.id_position =fr.id_position_fk \
            left join dim_teams dt on dt.id_team = dr.id_team_fk\
            left join dim_constructors dc on dc.id_constructor = dt.id_constructor_fk\
            where dp.final_position = '1' and fr.racing_class  in {racing_class} and fr.season in {season_proc}\
            and fr.race_type='main'\
            group by dc.des_constructor\
            order by num_wins desc\
            limit 10"
        

    cur.execute(query)
    result_args = cur.fetchall()

    df_top_wins = pd.DataFrame(result_args,columns=['constructor name', 'number of wins'])

    return df_top_wins
      
def fetch_top_podiums_constructor(season,racing_class):

    conn = connect()
    cur = conn.cursor()
    
    ini = season[0]
    end = season[1]

    season_proc = '('
    for x in range (ini, end+1):
        season_proc = season_proc + str(x) + ','

    season_proc = season_proc[:-1]
    season_proc = season_proc+ ')'

    if racing_class=='Any':
        racing_class= ('motogp', '250cc','moto2', '125cc','moto3', 'moto-e')
    
    elif (racing_class == "250cc_moto2"):
        racing_class=('moto2','250cc')
        
    elif racing_class == "125cc_moto3":
        racing_class=('moto3','125cc')
    elif racing_class == "motogp":
        racing_class=('motogp','')
    else:
        racing_class=('moto-e','')


    query = f"select dc.des_constructor,  count(fr.id_result) num_wins from fact_results fr \
            left join dim_riders dr on fr.id_rider_fk = dr.id_rider \
            left join dim_positions dp on dp.id_position =fr.id_position_fk \
            left join dim_teams dt on dt.id_team = dr.id_team_fk\
            left join dim_constructors dc on dc.id_constructor = dt.id_constructor_fk\
            where dp.final_position in ('1','2','3') and fr.racing_class  in {racing_class} and fr.season in {season_proc}\
            and fr.race_type='main'\
            group by dc.des_constructor\
            order by num_wins desc\
            limit 10"
        

    cur.execute(query)
    result_args = cur.fetchall()

    df_top_podiums= pd.DataFrame(result_args,columns=['constructor name', 'number of podiums'])

    return df_top_podiums
        
def fetch_top_percentage_wins_season(season,racing_class):

    conn = connect()
    cur = conn.cursor()
    
    ini = season[0]
    end = season[1]

    season_proc = '('
    for x in range (ini, end+1):
        season_proc = season_proc + str(x) + ','

    season_proc = season_proc[:-1]
    season_proc = season_proc+ ')'

    if racing_class=='Any':
        racing_class= ('motogp', '250cc','moto2', '125cc','moto3', 'moto-e')
    
    elif (racing_class == "250cc_moto2"):
        racing_class=('moto2','250cc')
        
    elif racing_class == "125cc_moto3":
        racing_class=('moto3','125cc')
    elif racing_class == "motogp":
        racing_class=('motogp','')
    else:
        racing_class=('moto-e','')


    query = f"SELECT \
                subquery.season, \
                subquery.racing_class,\
                subquery.rider_full_name,\
                ROUND((subquery.wins*100)/num_races.num_rounds, 2) as percentage_wins\
            FROM (\
                SELECT \
                    dr.rider_full_name, \
                    f.racing_class,\
                    f.season, \
                    count(f.id_result) AS wins,\
                    ROW_NUMBER() OVER (PARTITION BY f.season,f.racing_class ORDER BY SUM(dp.num_points) DESC) AS rn\
                FROM \
                    fact_results f\
                    LEFT JOIN dim_grand_prix dgp ON f.id_grand_prix_fk = dgp.id_grandprix \
                    LEFT JOIN dim_riders dr ON dr.id_rider = f.id_rider_fk \
                    LEFT JOIN dim_positions dp ON dp.id_position = f.id_position_fk \
                WHERE \
                    f.racing_class in {racing_class} and f.race_type='main' and f.season in {season_proc}\
                    and dp.final_position in ('1')\
                GROUP BY \
                    dr.rider_full_name,f.racing_class, f.season\
            ) AS subquery\
            JOIN (\
                SELECT dgp.season, COUNT(dgp.num_round) num_rounds\
                FROM \
                    dim_grand_prix dgp \
                GROUP BY \
                    dgp.season\
            ) AS num_races ON subquery.season = num_races.season \
            WHERE \
                rn = 1 and subquery.rider_full_name not like '%Bekker%'\
            ORDER BY percentage_wins DESC\
            limit 10;"
        

    cur.execute(query)
    result_args = cur.fetchall()

    df_top_podiums= pd.DataFrame(result_args,columns=['season', 'racing_class', 'rider name', 'percentage wins'])

    return df_top_podiums  

def fetch_top_percentage_podiums_season(season,racing_class):

    conn = connect()
    cur = conn.cursor()
    
    ini = season[0]
    end = season[1]

    season_proc = '('
    for x in range (ini, end+1):
        season_proc = season_proc + str(x) + ','

    season_proc = season_proc[:-1]
    season_proc = season_proc+ ')'

    if racing_class=='Any':
        racing_class= ('motogp', '250cc','moto2', '125cc','moto3', 'moto-e')
    
    elif (racing_class == "250cc_moto2"):
        racing_class=('moto2','250cc')
        
    elif racing_class == "125cc_moto3":
        racing_class=('moto3','125cc')
    elif racing_class == "motogp":
        racing_class=('motogp','')
    else:
        racing_class=('moto-e','')


    query = f"SELECT \
                subquery.season, \
                subquery.racing_class,\
                subquery.des_constructor,\
                ROUND((subquery.podiums * 100) / num_races.num_rounds, 2) as percentage_podiums\
            FROM (\
                SELECT \
                    dc.des_constructor, \
                    f.racing_class,\
                    f.season, \
                    COUNT(DISTINCT f.id_grand_prix_fk) as podiums\
                    FROM \
                    fact_results f\
                    LEFT JOIN dim_grand_prix dgp ON f.id_grand_prix_fk = dgp.id_grandprix \
                    LEFT JOIN dim_riders dr ON dr.id_rider = f.id_rider_fk \
                    LEFT JOIN dim_teams dt ON dt.id_team = dr.id_team_fk\
                    LEFT JOIN dim_constructors dc ON dc.id_constructor = dt.id_constructor_fk\
                    LEFT JOIN dim_positions dp ON dp.id_position = f.id_position_fk \
                WHERE \
                    f.racing_class in {racing_class} AND f.season in {season_proc}\
                    AND dp.final_position in ('1', '2', '3') AND f.race_type = 'main' \
                GROUP BY \
                    dc.des_constructor, f.racing_class, f.season\
            ) AS subquery\
            JOIN (\
                SELECT dgp.season, COUNT(dgp.num_round) num_rounds\
                FROM \
                    dim_grand_prix dgp \
                GROUP BY \
                    dgp.season\
            ) AS num_races ON subquery.season = num_races.season \
            ORDER BY \
                percentage_podiums DESC\
            --LIMIT 10;"
        

    cur.execute(query)
    result_args = cur.fetchall()

    df_top_podiums= pd.DataFrame(result_args,columns=['season', 'racing_class', 'rider name', 'percentage podiums'])

    return df_top_podiums  

def fetch_top_percentage_wins_season_constructor(season,racing_class):

    conn = connect()
    cur = conn.cursor()
    
    ini = season[0]
    end = season[1]

    season_proc = '('
    for x in range (ini, end+1):
        season_proc = season_proc + str(x) + ','

    season_proc = season_proc[:-1]
    season_proc = season_proc+ ')'

    if racing_class=='Any':
        racing_class= ('motogp', '250cc','moto2', '125cc','moto3', 'moto-e')
    
    elif (racing_class == "250cc_moto2"):
        racing_class=('moto2','250cc')
        
    elif racing_class == "125cc_moto3":
        racing_class=('moto3','125cc')
    elif racing_class == "motogp":
        racing_class=('motogp','')
    else:
        racing_class=('moto-e','')


    query = f"SELECT \
                subquery.season, \
                subquery.racing_class,\
                subquery.des_constructor,\
                ROUND((subquery.wins*100)/num_races.num_rounds, 2) as percentage_wins\
            FROM (\
                SELECT \
                    dc.des_constructor, \
                    f.racing_class,\
                    f.season, \
                    count(f.id_result) AS wins,\
                    ROW_NUMBER() OVER (PARTITION BY f.season,f.racing_class ORDER BY SUM(dp.num_points) DESC) AS rn\
                FROM \
                    fact_results f\
                    LEFT JOIN dim_grand_prix dgp ON f.id_grand_prix_fk = dgp.id_grandprix \
                    LEFT JOIN dim_riders dr ON dr.id_rider = f.id_rider_fk \
                    left join dim_teams dt on dt.id_team = dr.id_team_fk\
                    left join dim_constructors dc on dc.id_constructor = dt.id_constructor_fk\
                    LEFT JOIN dim_positions dp ON dp.id_position = f.id_position_fk \
                WHERE \
                    f.racing_class in {racing_class} and f.race_type='main' and f.season in {season_proc}\
                    and dp.final_position in ('1')\
                GROUP BY \
                    dc.des_constructor,f.racing_class, f.season\
            ) AS subquery\
            JOIN (\
                SELECT dgp.season, COUNT(dgp.num_round) num_rounds\
                FROM \
                    dim_grand_prix dgp \
                GROUP BY \
                    dgp.season\
            ) AS num_races ON subquery.season = num_races.season \
            WHERE \
                rn = 1 \
            ORDER BY percentage_wins DESC\
            limit 10;"
        

    cur.execute(query)
    result_args = cur.fetchall()

    df_top_podiums= pd.DataFrame(result_args,columns=['season', 'racing_class', 'rider name', 'percentage wins'])

    return df_top_podiums  

def fetch_top_percentage_podiums_season_constructor(season,racing_class):

    conn = connect()
    cur = conn.cursor()
    
    ini = season[0]
    end = season[1]

    season_proc = '('
    for x in range (ini, end+1):
        season_proc = season_proc + str(x) + ','

    season_proc = season_proc[:-1]
    season_proc = season_proc+ ')'

    if racing_class=='Any':
        racing_class= ('motogp', '250cc','moto2', '125cc','moto3', 'moto-e')
    
    elif (racing_class == "250cc_moto2"):
        racing_class=('moto2','250cc')
        
    elif racing_class == "125cc_moto3":
        racing_class=('moto3','125cc')
    elif racing_class == "motogp":
        racing_class=('motogp','')
    else:
        racing_class=('moto-e','')


    query = f"SELECT \
                subquery.season,\
                subquery.racing_class,\
                subquery.des_constructor,\
                ROUND((subquery.podiums * 100) / num_races.num_rounds, 2) as percentage_podiums\
            FROM (\
                SELECT \
                    dc.des_constructor, \
                    f.racing_class,\
                    f.season, \
                    COUNT(DISTINCT f.id_grand_prix_fk) as podiums\
                    FROM \
                    fact_results f\
                    LEFT JOIN dim_grand_prix dgp ON f.id_grand_prix_fk = dgp.id_grandprix \
                    LEFT JOIN dim_riders dr ON dr.id_rider = f.id_rider_fk \
                    LEFT JOIN dim_teams dt ON dt.id_team = dr.id_team_fk\
                    LEFT JOIN dim_constructors dc ON dc.id_constructor = dt.id_constructor_fk\
                    LEFT JOIN dim_positions dp ON dp.id_position = f.id_position_fk \
                WHERE \
                    f.racing_class in {racing_class} AND f.season in {season_proc}\
                    AND dp.final_position in ('1', '2', '3') AND f.race_type = 'main' \
                GROUP BY \
                    dc.des_constructor, f.racing_class, f.season\
            ) AS subquery\
            JOIN (\
                SELECT dgp.season, COUNT(dgp.num_round) num_rounds\
                FROM \
                    dim_grand_prix dgp \
                GROUP BY \
                    dgp.season\
            ) AS num_races ON subquery.season = num_races.season \
            ORDER BY \
                percentage_podiums DESC\
            --LIMIT 10;"
        

    cur.execute(query)
    result_args = cur.fetchall()

    df_top_podiums= pd.DataFrame(result_args,columns=['season', 'racing_class', 'rider name', 'percentage podiums'])

    return df_top_podiums  


def fetch_top_percentage_podiums_season_teams(season,racing_class):

    conn = connect()
    cur = conn.cursor()
    
    ini = season[0]
    end = season[1]

    season_proc = '('
    for x in range (ini, end+1):
        season_proc = season_proc + str(x) + ','

    season_proc = season_proc[:-1]
    season_proc = season_proc+ ')'

    if racing_class=='Any':
        racing_class= ('motogp', '250cc','moto2', '125cc','moto3', 'moto-e')
    
    elif (racing_class == "250cc_moto2"):
        racing_class=('moto2','250cc')
        
    elif racing_class == "125cc_moto3":
        racing_class=('moto3','125cc')
    elif racing_class == "motogp":
        racing_class=('motogp','')
    else:
        racing_class=('moto-e','')


    query = f"SELECT \
                    subquery.season, \
                    subquery.racing_class,\
                    subquery.des_team,\
                    ROUND((subquery.podiums * 100) / num_races.num_rounds, 2) as percentage_podiums\
                FROM (\
                    SELECT \
                        dt.des_team, \
                        f.racing_class,\
                        f.season, \
                        COUNT(DISTINCT f.id_grand_prix_fk) as podiums\
                        FROM \
                        fact_results f\
                        LEFT JOIN dim_grand_prix dgp ON f.id_grand_prix_fk = dgp.id_grandprix \
                        LEFT JOIN dim_riders dr ON dr.id_rider = f.id_rider_fk \
                        LEFT JOIN dim_teams dt ON dt.id_team = dr.id_team_fk\
                        LEFT JOIN dim_constructors dc ON dc.id_constructor = dt.id_constructor_fk\
                        LEFT JOIN dim_positions dp ON dp.id_position = f.id_position_fk \
                    WHERE \
                        f.racing_class in {racing_class} AND f.season in {season_proc}\
                        AND dp.final_position in ('1', '2', '3') AND f.race_type = 'main' \
                    GROUP BY \
                        dt.des_team, f.racing_class, f.season\
                ) AS subquery\
                JOIN (\
                    SELECT dgp.season, COUNT(dgp.num_round) num_rounds\
                    FROM \
                        dim_grand_prix dgp \
                    GROUP BY \
                        dgp.season\
                ) AS num_races ON subquery.season = num_races.season \
                ORDER BY \
                    percentage_podiums DESC\
                --LIMIT 10;"
        

    cur.execute(query)
    result_args = cur.fetchall()

    df_top_podiums= pd.DataFrame(result_args,columns=['season', 'racing_class', 'rider name', 'percentage podiums'])

    return df_top_podiums  



def fetch_top_different_winners(season,racing_class):

    conn = connect()
    cur = conn.cursor()
    
    ini = season[0]
    end = season[1]

    season_proc = '('
    for x in range (ini, end+1):
        season_proc = season_proc + str(x) + ','

    season_proc = season_proc[:-1]
    season_proc = season_proc+ ')'

    if racing_class=='Any':
        racing_class= ('motogp', '250cc','moto2', '125cc','moto3', 'moto-e')
    
    elif (racing_class == "250cc_moto2"):
        racing_class=('moto2','250cc')
        
    elif racing_class == "125cc_moto3":
        racing_class=('moto3','125cc')
    elif racing_class == "motogp":
        racing_class=('motogp','')
    else:
        racing_class=('moto-e','')

    query = f" select  fr.season, fr.racing_class,count(distinct fr.id_rider_fk) as numer_of_winners  from fact_results fr \
                left join dim_riders dr on fr.id_rider_fk = dr.id_rider \
                left join dim_positions dp on dp.id_position = fr.id_position_fk\
                where dp.final_position ='1' and fr.race_type = 'main'\
                and fr.season in {season_proc} and fr.racing_class in {racing_class}\
                group by  fr.season, fr.racing_class \
                order by numer_of_winners desc"
                        

    cur.execute(query)
    result_args = cur.fetchall()

    df_top_podiums= pd.DataFrame(result_args,columns=['season', 'racing_class', 'numer of diferent winners'])

    return df_top_podiums  


def fetch_top_different_podium_finishers(season,racing_class):
    conn = connect()
    cur = conn.cursor()
    
    ini = season[0]
    end = season[1]

    season_proc = '('
    for x in range (ini, end+1):
        season_proc = season_proc + str(x) + ','

    season_proc = season_proc[:-1]
    season_proc = season_proc+ ')'

    if racing_class=='Any':
        racing_class= ('motogp', '250cc','moto2', '125cc','moto3', 'moto-e')
    
    elif (racing_class == "250cc_moto2"):
        racing_class=('moto2','250cc')
        
    elif racing_class == "125cc_moto3":
        racing_class=('moto3','125cc')
    elif racing_class == "motogp":
        racing_class=('motogp','')
    else:
        racing_class=('moto-e','')

    query = f"select  fr.season, fr.racing_class,count(distinct fr.id_rider_fk) as numer_of_winners   from fact_results fr \
                left join dim_riders dr on fr.id_rider_fk = dr.id_rider \
                left join dim_positions dp2 on dp2.id_position = fr.id_position_fk\
                where dp2.final_position in ('1','2','3') and fr.race_type = 'main'\
                and fr.season in {season_proc} and fr.racing_class in {racing_class}\
                group by  fr.season, fr.racing_class \
                order by numer_of_winners desc"
                        

    cur.execute(query)
    result_args = cur.fetchall()

    df_top_podiums= pd.DataFrame(result_args,columns=['season', 'racing_class', 'number of diferent podium finishers'])

    return df_top_podiums 

def fetch_top_wins_by_track(season,racing_class):
    conn = connect()
    cur = conn.cursor()
    
    ini = season[0]
    end = season[1]

    season_proc = '('
    for x in range (ini, end+1):
        season_proc = season_proc + str(x) + ','

    season_proc = season_proc[:-1]
    season_proc = season_proc+ ')'

    if racing_class=='Any':
        racing_class= ('motogp', '250cc','moto2', '125cc','moto3', 'moto-e')
    
    elif (racing_class == "250cc_moto2"):
        racing_class=('moto2','250cc')
        
    elif racing_class == "125cc_moto3":
        racing_class=('moto3','125cc')
    elif racing_class == "motogp":
        racing_class=('motogp','')
    else:
        racing_class=('moto-e','')

    query = f" SELECT \
                dr.rider_full_name, \
                dtr.des_track,\
                count(f.id_result) as wins\
                FROM \
                fact_results f\
                LEFT JOIN dim_grand_prix dgp ON f.id_grand_prix_fk = dgp.id_grandprix \
                left join dim_tracks dtr on dtr.id_track= dgp.id_track_fk\
                LEFT JOIN dim_riders dr ON dr.id_rider = f.id_rider_fk \
                LEFT JOIN dim_teams dt ON dt.id_team = dr.id_team_fk\
                LEFT JOIN dim_constructors dc ON dc.id_constructor = dt.id_constructor_fk\
                LEFT JOIN dim_positions dp ON dp.id_position = f.id_position_fk \
            WHERE \
                dp.final_position in ('1') AND f.race_type = 'main' and dt.rider_full_name not like '%Bekker%'\
                and f.season in {season_proc} and f.racing_class in {racing_class}\
            GROUP BY \
                dr.rider_full_name,  dtr.des_track\
            having count(f.id_result) >3\
            order by wins desc"
                        

    cur.execute(query)
    result_args = cur.fetchall()

    df_top_podiums= pd.DataFrame(result_args,columns=['season', 'racing_class', 'wins by track'])

    return df_top_podiums 

def fetch_top_wins_by_track_constructor(season,racing_class):
    conn = connect()
    cur = conn.cursor()
    
    ini = season[0]
    end = season[1]

    season_proc = '('
    for x in range (ini, end+1):
        season_proc = season_proc + str(x) + ','

    season_proc = season_proc[:-1]
    season_proc = season_proc+ ')'

    if racing_class=='Any':
        racing_class= ('motogp', '250cc','moto2', '125cc','moto3', 'moto-e')
    
    elif (racing_class == "250cc_moto2"):
        racing_class=('moto2','250cc')
        
    elif racing_class == "125cc_moto3":
        racing_class=('moto3','125cc')
    elif racing_class == "motogp":
        racing_class=('motogp','')
    else:
        racing_class=('moto-e','')

    query = f"SELECT \
            dc.des_constructor , \
            dtr.des_track,\
            count(f.id_result) as wins\
            FROM \
            fact_results f\
            LEFT JOIN dim_grand_prix dgp ON f.id_grand_prix_fk = dgp.id_grandprix \
            left join dim_tracks dtr on dtr.id_track= dgp.id_track_fk\
            LEFT JOIN dim_riders dr ON dr.id_rider = f.id_rider_fk \
            LEFT JOIN dim_teams dt ON dt.id_team = dr.id_team_fk\
            LEFT JOIN dim_constructors dc ON dc.id_constructor = dt.id_constructor_fk\
            LEFT JOIN dim_positions dp ON dp.id_position = f.id_position_fk \
        WHERE \
            dp.final_position in ('1') AND f.race_type = 'main' and dt.rider_full_name not like '%Bekker%' and f.racing_class = 'motogp'\
        GROUP BY \
            dc.des_constructor ,  dtr.des_track\
        having count(f.id_result) >3\
        order by wins desc"
                        

    cur.execute(query)
    result_args = cur.fetchall()

    df_top_podiums= pd.DataFrame(result_args,columns=['season', 'racing_class', 'wins by track'])

    return df_top_podiums 


def fetch_top_percentage_points(season,racing_class):
    conn = connect()
    cur = conn.cursor()
    
    ini = season[0]
    end = season[1]

    season_proc = '('
    for x in range (ini, end+1):
        season_proc = season_proc + str(x) + ','

    season_proc = season_proc[:-1]
    season_proc = season_proc+ ')'

    if racing_class=='Any':
        racing_class= ('motogp', '250cc','moto2', '125cc','moto3', 'moto-e')
    
    elif (racing_class == "250cc_moto2"):
        racing_class=('moto2','250cc')
        
    elif racing_class == "125cc_moto3":
        racing_class=('moto3','125cc')
    elif racing_class == "motogp":
        racing_class=('motogp','')
    else:
        racing_class=('moto-e','')


    query = f"SELECT \
                    subquery.season, \
                    subquery.racing_class,\
                    subquery.rider_full_name,\
                    ROUND((subquery.points*100)/max_points.max_points, 2) as percentage_points\
                FROM (\
                    SELECT \
                        dr.rider_full_name, \
                        f.racing_class,\
                        f.season, \
                        SUM(dp.num_points) AS points,\
                        ROW_NUMBER() OVER (PARTITION BY f.season,f.racing_class ORDER BY SUM(dp.num_points) DESC) AS rn\
                    FROM \
                        fact_results f\
                        LEFT JOIN dim_grand_prix dgp ON f.id_grand_prix_fk = dgp.id_grandprix \
                        LEFT JOIN dim_riders dr ON dr.id_rider = f.id_rider_fk \
                        LEFT JOIN dim_positions dp ON dp.id_position = f.id_position_fk \
                    WHERE \
                        f.racing_class in {racing_class} and f.season in {season_proc}\
                    GROUP BY \
                        dr.rider_full_name,f.racing_class, f.season\
                ) AS subquery\
                JOIN (\
                    SELECT dgp.season,\
                        CASE \
                            WHEN dgp.season = 2023 THEN COUNT(dgp.num_round) * 36 \
                            ELSE COUNT(dgp.num_round) * 25 \
                        END AS max_points\
                    FROM \
                        dim_grand_prix dgp \
                    GROUP BY \
                        dgp.season\
                ) AS max_points ON subquery.season = max_points.season \
                WHERE \
                    rn = 1 and subquery.rider_full_name not like '%Bekker%'\
                ORDER BY \
                    percentage_points ASC\
                limit 100"
        

    cur.execute(query)
    result_args = cur.fetchall()

    df_top_podiums= pd.DataFrame(result_args,columns=[ 'season','racing class','rider name', 'percentage of total points'])

    return df_top_podiums


def fetch_top_percentage_points_carreer(season,racing_class):
    conn = connect()
    cur = conn.cursor()
    
    ini = season[0]
    end = season[1]

    season_proc = '('
    for x in range (ini, end+1):
        season_proc = season_proc + str(x) + ','

    season_proc = season_proc[:-1]
    season_proc = season_proc+ ')'

    if racing_class=='Any':
        racing_class= ('motogp', '250cc','moto2', '125cc','moto3', 'moto-e')
    
    elif (racing_class == "250cc_moto2"):
        racing_class=('moto2','250cc')
        
    elif racing_class == "125cc_moto3":
        racing_class=('moto3','125cc')
    elif racing_class == "motogp":
        racing_class=('motogp','')
    else:
        racing_class=('moto-e','')


    query = "SELECT subquery.rider_full_name,\
                ROUND((subquery.points * 100) / SUM(max_points.max_points), 2) as percentage_points\
            FROM (\
                SELECT \
                    dr.rider_full_name, \
                    SUM(dp.num_points) AS points\
                FROM \
                    fact_results f\
                    LEFT JOIN dim_riders dr ON dr.id_rider = f.id_rider_fk \
                    LEFT JOIN dim_positions dp ON dp.id_position = f.id_position_fk \
                    \
                GROUP BY \
                    dr.rider_full_name\
            ) AS subquery\
            JOIN (\
                SELECT \
                    dr.rider_full_name,\
                    CASE \
                        WHEN dgp.season > 2022 THEN COUNT(DISTINCT dgp.id_grandprix) * 36 \
                        ELSE COUNT(DISTINCT dgp.id_grandprix) * 25 \
                    END AS max_points\
                    \
                FROM \
                    fact_results f\
                    left join dim_positions dp on dp.id_position = f.id_position_fk \
                    LEFT JOIN dim_grand_prix dgp ON f.id_grand_prix_fk = dgp.id_grandprix\
                    LEFT JOIN dim_riders dr ON dr.id_rider = f.id_rider_fk \
                    \
                GROUP BY \
                    dr.rider_full_name, dgp.season\
            ) AS max_points ON subquery.rider_full_name = max_points.rider_full_name\
            WHERE \
                subquery.rider_full_name NOT LIKE '%Bekker%'\
            GROUP BY \
            subquery.rider_full_name,subquery.points\
            having  SUM(max_points.max_points) > 50\
            ORDER BY \
                percentage_points DESC\
            LIMIT 20;"
        

    cur.execute(query)
    result_args = cur.fetchall()

    df_top_podiums= pd.DataFrame(result_args,columns=[ 'rider name', 'percentage of total points in a carreer'])

    return df_top_podiums
