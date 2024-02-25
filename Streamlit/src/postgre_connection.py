import psycopg
import streamlit as st
import os
from dotenv import load_dotenv
import pandas as pd
import itertools
import pandasql as psql

load_dotenv()

def connect_csv():
    global dim_riders
    global dim_constructors
    global dim_tracks
    global dim_grand_prix
    global dim_date
    global dim_positions
    global dim_teams
    global fact_results
    global consecutive_results
    global winless_rider_results

    dim_riders= st.secrets['dim_riders'],
    dim_constructors= st.secrets['dim_constructors'],
    dim_tracks= st.secrets['dim_tracks'],
    dim_grand_prix= st.secrets['dim_grand_prix'],
    dim_date= st.secrets['dim_date'],
    dim_positions= st.secrets['dim_positions'],
    dim_teams= st.secrets['dim_teams'],
    fact_results= st.secrets['fact_results'],
    consecutive_results = st.secrets['consecutive_results'],
    winless_rider_results = st.secrets['winless_rider_results'],
    # st.write(dim_riders[0])

    # Download the CSV files from the Streamlit sharing service
    dim_riders = pd.read_csv(dim_riders[0])
    dim_constructors = pd.read_csv(dim_constructors[0])
    dim_tracks = pd.read_csv(dim_tracks[0])
    dim_grand_prix = pd.read_csv(dim_grand_prix[0])
    dim_date = pd.read_csv(dim_date[0])
    dim_positions = pd.read_csv(dim_positions[0])
    dim_teams = pd.read_csv(dim_teams[0])
    fact_results = pd.read_csv(fact_results[0])
    consecutive_results =  pd.read_csv(consecutive_results[0])
    winless_rider_results =  pd.read_csv(winless_rider_results[0])
    # st.dataframe(dim_riders)
    

    # sdf =  psql.sqldf("SELECT des_team FROM dim_riders dr left join dim_teams dt on dr.id_team_fk=dt.id_team")
    # st.dataframe(sdf)


def connect(): 
    # if st.secrets['is_deployed']:
    #     db_host = os.getenv('POSTGRES_HOST')
    #     db_port = os.getenv('POSTGRES_PORT')
    #     db_name = os.getenv('POSTGRES_DB')
    #     db_user = os.getenv('POSTGRES_USER')
    #     db_password = os.getenv('POSTGRES_PASSWORD')
        
    # else:
    db_host = st.secrets['POSTGRES_HOST']
    db_port = st.secrets['POSTGRES_PORT']
    db_name = st.secrets['POSTGRES_DB']
    db_user =st.secrets['POSTGRES_USER']
    db_password = st.secrets['POSTGRES_PASSWORD']

    conn = psycopg.connect(f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}") 
    
    return conn

def fetch_cummulative_sum_points(season, racing_class):
    
    
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

    
    
    if st.session_state.UsingCSV:
        df_bh =  psql.sqldf(query)
    else:
        conn = connect()
        cur = conn.cursor()
        cur.execute(query)
        result_args = cur.fetchall()
        df_bh = pd.DataFrame(result_args,columns=['num_round', 'rider_full_name', 'cummulative_sum'])

    return df_bh

def fetch_cummulative_sum_points_constructors(season, racing_class):
    
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
    if st.session_state.UsingCSV:
        df_bh =  psql.sqldf(query)
    else:
        conn = connect()
        cur = conn.cursor()
        cur.execute(query)
        result_args = cur.fetchall()

        df_bh = pd.DataFrame(result_args,columns=['des_constructor', 'season', 'total_points'])

    return df_bh

def fetch_cummulative_sum_points_teams(season, racing_class):
    
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
                    SUM(total_best_points) AS total_points\
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
                order by total_points desc"
    query2  = f"""
            SELECT 
                des_team,
                season,
                SUM(total_best_points) AS total_points
            FROM (
                SELECT 
                    dt.des_team,
                    fr.season,
                    fr.num_round,
                    dr.rider_full_name,
                    fr.race_type,
                    ROW_NUMBER() OVER (PARTITION BY dt.des_team, fr.num_round, fr.race_type ORDER BY dp.num_points DESC) AS rank,
                    SUM(dp.num_points) AS total_best_points
                FROM 
                    fact_results fr
                JOIN 
                    dim_positions dp ON fr.id_position_fk = dp.id_position
                LEFT JOIN 
                    dim_riders dr ON dr.id_rider = fr.id_rider_fk
                LEFT JOIN 
                    dim_teams dt ON dt.id_team = dr.id_team_fk
                WHERE 
                    dr.season = {season}
                    AND dr.racing_class = '{racing_class}'
                    AND fr.is_wildcard = 0
                GROUP BY
                    dt.des_team,
                    fr.season,
                    fr.num_round,
                    dr.rider_full_name,
                    fr.race_type,
                    dp.num_points
            ) AS ranked_points
            WHERE rank <= 2
            GROUP BY 
                des_team, 
                season
            HAVING SUM(total_best_points) > 0
            ORDER BY total_points DESC;
            """



    if  st.session_state.UsingCSV:
        df_bh = psql.sqldf(query2)
        # st.dataframe(df_bh)
    else:
        conn = connect()
        cur = conn.cursor()    
        cur.execute(query)
        result_args = cur.fetchall()
        df_bh = pd.DataFrame(result_args,columns=['des_team', 'season', 'total_points'])

    return df_bh

def fetch_season_bar_chart(season, racing_class):
    
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
    
    

    query = f"select rider_full_name as rider_name, total_points from (select dr.rider_full_name, sum(dp.num_points) as total_points  from fact_results f\
                left join dim_grand_prix dgp on f.id_grand_prix_fk = dgp.id_grandprix \
                left join dim_riders dr on  dr.id_rider = f.id_rider_fk \
                left join dim_positions dp on dp.id_position = f.id_position_fk \
                where f.racing_class = {racing_class} and f.season  = {season} \
                group by dr.rider_full_name\
                order by total_points desc)\
                where total_points > 0;"

    if st.session_state.UsingCSV:
        df_bh =  psql.sqldf(query)
    else:
        conn = connect()
        cur = conn.cursor()    
        cur.execute(query)
        result_args = cur.fetchall()

        df_bh = pd.DataFrame(result_args,columns=['rider_name', 'total_points'])

    return df_bh

def fetch_track_location(season):
    
    
    
    

    query = f"select des_track, dt.longitude  ,dt.latitude from dim_grand_prix dgp\
            left join  dim_tracks dt on id_track_fk =id_track\
            where season = {season}"
    if st.session_state.UsingCSV:
        df_tracks =  psql.sqldf(query)
    else:
        conn = connect()
        cur = conn.cursor()
        cur.execute(query)
        result_args = cur.fetchall()

        df_tracks = pd.DataFrame(result_args,columns=['des_track', 'longitude', 'latitude'])

    return df_tracks

def fetch_rider_location(season):

    
    
    

    query = f"select distinct rider_full_name, birth_latitude as longitude, birth_longitude as latitude  from dim_riders dr \
        where season = {season} and birth_latitude is not null and  birth_longitude is not null"

    if st.session_state.UsingCSV:
        df_riders =  psql.sqldf(query)
    else:
        conn = connect()
        cur = conn.cursor()
    
        cur.execute(query)
        result_args = cur.fetchall()

        df_riders = pd.DataFrame(result_args,columns=['rider_full_name', 'longitude', 'latitude'])

    return df_riders

def fetch_total_num_gp(season):

    
    ini = season[0]
    end = season[1]

    season_proc = '('
    for x in range (ini, end+1):
        season_proc = season_proc + str(x) + ','

    season_proc = season_proc[:-1]
    season_proc = season_proc+ ')'
        

    query = f"select count(id_grandprix) as num_grand_prix from dim_grand_prix dgp \
                where season in {season_proc}"

    if st.session_state.UsingCSV:
        result_args =  psql.sqldf(query)
    else:
        conn = connect()
        cur = conn.cursor()
        cur.execute(query)
        result_args = cur.fetchone()


    return result_args

def fetch_HP_races(season,racing_class):
    
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
    if st.session_state.UsingCSV:
        result_args =  psql.sqldf(query)
    else:
        conn = connect()
        cur = conn.cursor()
        cur.execute(query)
        result_args = cur.fetchone()


    return result_args

def fetch_night_races(season):
    
    ini = season[0]
    end = season[1]

    season_proc = '('
    for x in range (ini, end+1):
        season_proc = season_proc + str(x) + ','

    season_proc = season_proc[:-1]
    season_proc = season_proc+ ')'
        

    query = f"select count(distinct id_grandprix) as night_races from dim_grand_prix dgp \
                where dgp.night_race = true and dgp.season in {season_proc}"

    if st.session_state.UsingCSV:
        result_args =  psql.sqldf(query)
    else:
        conn = connect()
        cur = conn.cursor()    
        cur.execute(query)
        result_args = cur.fetchone()


    return result_args

def fetch_satruday_races(season):
    
    ini = season[0]
    end = season[1]

    season_proc = '('
    for x in range (ini, end+1):
        season_proc = season_proc + str(x) + ','

    season_proc = season_proc[:-1]
    season_proc = season_proc+ ')'
        

    query = f"select count(distinct id_grandprix) as night_races from dim_grand_prix dgp \
  	        where dgp.saturday_race  = true and dgp.season in {season_proc}"

    if st.session_state.UsingCSV:
        result_args =  psql.sqldf(query)
    else:
        conn = connect()
        cur = conn.cursor()    
        cur.execute(query)
        result_args = cur.fetchone()


    return result_args

 


def fetch_num_postions(season,racing_class):
    
    
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

    if st.session_state.UsingCSV:
        df =  psql.sqldf(query)
    else:
        conn = connect()
        cur = conn.cursor()
        
        cur.execute(query)
        result_args = cur.fetchall()

        df = pd.DataFrame(result_args,columns=['rider_full_name', 'longitude', 'latitude'])

    return df

def fetch_top_wins(season,racing_class):

    
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
        
    if st.session_state.UsingCSV:
        df_top_wins =  psql.sqldf(query)
    else:
        conn = connect()
        cur = conn.cursor()

        cur.execute(query)
        result_args = cur.fetchall()

        df_top_wins = pd.DataFrame(result_args,columns=['rider name', 'number of wins'])

    return df_top_wins
      
def fetch_top_podiums(season,racing_class):

    
    
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
        
    if st.session_state.UsingCSV:
        df_top_podiums =  psql.sqldf(query)
    else:
        conn = connect()
        cur = conn.cursor()    
        cur.execute(query)
        result_args = cur.fetchall()

        df_top_podiums= pd.DataFrame(result_args,columns=['rider name', 'number of podiums'])

    return df_top_podiums
        
  
def fetch_top_wins_sprint(season,racing_class):

    
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


    query = f"select dr.rider_full_name as 'rider name',  count(fr.id_result) as num_wins from fact_results fr \
            left join dim_riders dr on fr.id_rider_fk = dr.id_rider \
            left join dim_positions dp on dp.id_position =fr.id_position_fk \
            where dp.final_position = '1' and fr.racing_class  in {racing_class} and fr.season in {season_proc}\
            and fr.race_type='sprint'\
            group by dr.rider_full_name\
            order by num_wins desc\
            limit 10"
        
    if st.session_state.UsingCSV:
        df_top_wins =  psql.sqldf(query)
    else:
        conn = connect()
        cur = conn.cursor()
       
        cur.execute(query)
        result_args = cur.fetchall()

        df_top_wins = pd.DataFrame(result_args,columns=['rider name', 'num_wins'])

    return df_top_wins
      
def fetch_top_podiums_sprint(season,racing_class):

    
    
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


    query = f"select dr.rider_full_name,  count(fr.id_result) 'number of sprint podiums' from fact_results fr \
            left join dim_riders dr on fr.id_rider_fk = dr.id_rider \
            left join dim_positions dp on dp.id_position =fr.id_position_fk \
            where dp.final_position in ('1','2','3') \
            and fr.racing_class  in {racing_class} and fr.season in {season_proc}\
            and fr.race_type='sprint'\
            group by dr.rider_full_name\
            order by count(fr.id_result) desc\
            limit 10"
        
    if st.session_state.UsingCSV:
        df_top_podiums =  psql.sqldf(query)
    else:
        conn = connect()
        cur = conn.cursor()
        cur.execute(query)
        result_args = cur.fetchall()

        df_top_podiums= pd.DataFrame(result_args,columns=['rider name', 'number of sprint podiums'])

    return df_top_podiums
        
def fetch_top_poles(season,racing_class):

    
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


    query = f"select dr.rider_full_name,  count(fr.id_result) 'number of poles' from fact_results fr \
            left join dim_riders dr on fr.id_rider_fk = dr.id_rider \
            left join dim_positions dp on dp.id_position =fr.id_position_fk \
            where fr.pole = True and fr.racing_class in {racing_class} and fr.season in {season_proc}\
            and fr.race_type='sprint'\
            group by dr.rider_full_name\
            order by count(fr.id_result) desc\
            limit 10"
        
    if st.session_state.UsingCSV:
        df_top_poles =  psql.sqldf(query)
    else:
        conn = connect()
        cur = conn.cursor()

        cur.execute(query)
        result_args = cur.fetchall()

        df_top_poles = pd.DataFrame(result_args,columns=['rider name', 'number of poles'])

    return df_top_poles
      
def fetch_top_fast_laps(season,racing_class):

    
    
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
        
    if st.session_state.UsingCSV:
        top_fast_laps =  psql.sqldf(query)
    else:
        conn = connect()
        cur = conn.cursor()
        cur.execute(query)
        result_args = cur.fetchall()

        top_fast_laps= pd.DataFrame(result_args,columns=['rider name', 'number of fastest laps'])

    return top_fast_laps
            
def fetch_top_percentage_points(season,racing_class):
    
    
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
        
    if st.session_state.UsingCSV:
        df_top_podiums =  psql.sqldf(query)
    else:
        conn = connect()
        cur = conn.cursor()
        cur.execute(query)
        result_args = cur.fetchall()

        df_top_podiums= pd.DataFrame(result_args,columns=[ 'season','racing class','rider name', 'percentage of total points'])

    return df_top_podiums

def fetch_top_points_carrer(season, racing_class):
    
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
    if st.session_state.UsingCSV:
        df_bh =  psql.sqldf(query)
    else:
        conn = connect()
        cur = conn.cursor()    
        cur.execute(query)
        result_args = cur.fetchall()

        df_bh = pd.DataFrame(result_args,columns=['rider_name', 'total points carreer'])

    return df_bh


def fetch_top_points_constructor(season, racing_class):
    
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
            
    query2 = f"select des_constructor, points from (select  dc.des_constructor, sum(dp.num_points) as points  from fact_results f\
            left join dim_grand_prix dgp on f.id_grand_prix_fk = dgp.id_grandprix \
            left join dim_riders dr on  dr.id_rider = f.id_rider_fk \
            left join dim_teams dt on dt.id_team = dr.id_team_fk\
            left join dim_constructors dc on dc.id_constructor = dt.id_constructor_fk\
            left join dim_positions dp on dp.id_position = f.id_position_fk \
            where f.racing_class in {racing_class} and f.season in {season_proc}\
            and  ',' || dr.rounds_string || ',' LIKE '%,' || f.num_round || ',%'\
            group by  dc.des_constructor\
            order by points desc)\
            where points > 0\
            limit 20;"
    if st.session_state.UsingCSV:
        df_bh =  psql.sqldf(query2)
    else:
        conn = connect()
        cur = conn.cursor()
        cur.execute(query)
        result_args = cur.fetchall()

        df_bh = pd.DataFrame(result_args,columns=['constructor name', 'total points constructor'])

    return df_bh


def fetch_top_wins_constructor(season,racing_class):

    
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
        
    if st.session_state.UsingCSV:
        df_top_wins =  psql.sqldf(query)
    else:
        conn = connect()
        cur = conn.cursor()

        cur.execute(query)
        result_args = cur.fetchall()

        df_top_wins = pd.DataFrame(result_args,columns=['constructor name', 'number of wins'])

    return df_top_wins
      
def fetch_top_podiums_constructor(season,racing_class):

    
    
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
        
    if st.session_state.UsingCSV:
        df_top_podiums =  psql.sqldf(query)
    else:
        conn = connect()
        cur = conn.cursor()
        cur.execute(query)
        result_args = cur.fetchall()

        df_top_podiums= pd.DataFrame(result_args,columns=['constructor name', 'number of podiums'])

    return df_top_podiums
        
def fetch_top_percentage_wins_season(season,racing_class):

    
    
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
        
    if st.session_state.UsingCSV:
        df_top_podiums =  psql.sqldf(query)
    else:
        conn = connect()
        cur = conn.cursor()
        cur.execute(query)
        result_args = cur.fetchall()

        df_top_podiums= pd.DataFrame(result_args,columns=['season', 'racing_class', 'rider name', 'percentage wins'])

    return df_top_podiums  

def fetch_top_percentage_podiums_season(season,racing_class):

    
    
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
        
    if st.session_state.UsingCSV:
        df_top_podiums =  psql.sqldf(query)
    else:
        conn = connect()
        cur = conn.cursor()
        cur.execute(query)
        result_args = cur.fetchall()

        df_top_podiums= pd.DataFrame(result_args,columns=['season', 'racing_class', 'rider name', 'percentage podiums'])

    return df_top_podiums  

def fetch_top_percentage_wins_season_constructor(season,racing_class):

    
    
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
        
    if st.session_state.UsingCSV:
        df_top_podiums =  psql.sqldf(query)
    else:
        conn = connect()
        cur = conn.cursor()

        cur.execute(query)
        result_args = cur.fetchall()

        df_top_podiums= pd.DataFrame(result_args,columns=['season', 'racing_class', 'rider name', 'percentage wins'])

    return df_top_podiums  

def fetch_top_percentage_podiums_season_constructor(season,racing_class):

    
    
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
        
    if st.session_state.UsingCSV:
        df_top_podiums =  psql.sqldf(query)
    else:
        conn = connect()
        cur = conn.cursor()
        cur.execute(query)
        result_args = cur.fetchall()

        df_top_podiums= pd.DataFrame(result_args,columns=['season', 'racing_class', 'rider name', 'percentage podiums'])

    return df_top_podiums  


def fetch_top_percentage_podiums_season_teams(season,racing_class):

    
    
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
        
    if st.session_state.UsingCSV:
        df_top_podiums =  psql.sqldf(query)
    else:
        conn = connect()
        cur = conn.cursor()
        cur.execute(query)
        result_args = cur.fetchall()

        df_top_podiums= pd.DataFrame(result_args,columns=['season', 'racing_class', 'rider name', 'percentage podiums'])

    return df_top_podiums  



def fetch_top_different_winners(season,racing_class):

    
    
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
                        
    if st.session_state.UsingCSV:
        df_top_podiums =  psql.sqldf(query)
    else:
        conn = connect()
        cur = conn.cursor()
        cur.execute(query)
        result_args = cur.fetchall()

        df_top_podiums = pd.DataFrame(result_args,columns=['season', 'racing_class', 'numer of diferent winners'])

    return df_top_podiums  


def fetch_top_different_podium_finishers(season,racing_class):
    
    
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
                        
    if st.session_state.UsingCSV:
        df_top_podiums =  psql.sqldf(query)
    else:
        conn = connect()
        cur = conn.cursor()
        cur.execute(query)
        result_args = cur.fetchall()

        df_top_podiums= pd.DataFrame(result_args,columns=['season', 'racing_class', 'number of diferent podium finishers'])

    return df_top_podiums 

def fetch_top_wins_by_track(season,racing_class):
    
    
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
                        
    if st.session_state.UsingCSV:
        df_top_podiums =  psql.sqldf(query)
    else:
        conn = connect()
        cur = conn.cursor()
        cur.execute(query)
        result_args = cur.fetchall()

        df_top_podiums= pd.DataFrame(result_args,columns=['season', 'racing_class', 'wins by track'])

    return df_top_podiums 

def fetch_top_wins_by_track_constructor(season,racing_class):
    
    
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
                        
    if st.session_state.UsingCSV:
        df_top_podiums =  psql.sqldf(query)
    else:
        conn = connect()
        cur = conn.cursor()
        cur.execute(query)
        result_args = cur.fetchall()

        df_top_podiums= pd.DataFrame(result_args,columns=['season', 'racing_class', 'wins by track'])

    return df_top_podiums 


def fetch_top_percentage_points(season,racing_class):
    
    
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
        
    if st.session_state.UsingCSV:
        df_top_podiums =  psql.sqldf(query)
    else:
        conn = connect()
        cur = conn.cursor()
        cur.execute(query)
        result_args = cur.fetchall()

        df_top_podiums= pd.DataFrame(result_args,columns=[ 'season','racing class','rider name', 'percentage of total points'])

    return df_top_podiums


def fetch_top_percentage_points_carreer(season,racing_class):
    
    
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
        
    if st.session_state.UsingCSV:
        df_top_podiums =  psql.sqldf(query)
    else:
        conn = connect()
        cur = conn.cursor()
        cur.execute(query)
        result_args = cur.fetchall()

        df_top_podiums= pd.DataFrame(result_args,columns=[ 'rider name', 'percentage of total points in a carreer'])

    return df_top_podiums


def fetch_consecutive_results_aux(season,racing_class):
    
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
                rider_full_name,\
                dgp.season,\
                dgp.num_round,\
                dgp.id_grandprix,\
                fr.race_type,\
                final_position\
            FROM \
                fact_results fr\
                LEFT JOIN dim_positions dp ON dp.id_position = fr.id_position_fk\
                LEFT JOIN dim_riders dr ON fr.id_rider_fk = dr.id_rider\
                LEFT JOIN dim_grand_prix dgp ON dgp.id_grandprix = fr.id_grand_prix_fk \
            WHERE  \
                dgp.season in {season_proc} and fr.racing_class in {racing_class} and rider_full_name<>'Raúl Jara'\
                and rider_full_name<>'Mark van Kreij'\
                AND NOT EXISTS (\
                    SELECT 1\
                    FROM fact_results fr2\
                    WHERE fr.id_rider_fk = fr2.id_rider_fk\
                    AND fr.id_grand_prix_fk = fr2.id_grand_prix_fk\
                    AND fr.id_position_fk > fr2.id_position_fk\
                )\
            ORDER BY \
                rider_full_name,dgp.season, dgp.num_round"
        
    query2 = f"select * from consecutive_results where season in {season_proc} and racing_class in {racing_class} "

    if st.session_state.UsingCSV:
        df =  psql.sqldf(query2)
    else:
        conn = connect()
        cur = conn.cursor()

        cur.execute(query)        
        result_args = cur.fetchall()

        df= pd.DataFrame(result_args,columns=[ 'rider_full_name', 'season','num_round','id_grandprix','race_type','final_position'])
    
    df['rider_full_name']=df['rider_full_name'].astype(str)
    df['season']=df['season'].astype(int)
    df['num_round']=df['num_round'].astype(int)
    df['id_grandprix']=df['id_grandprix'].astype(int)
    df['race_type']=df['race_type'].astype(str)
    df['final_position']=df['final_position'].astype(str)
    return df
    
def fetch_winless_results_aux(season,racing_class):
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

    df = psql.sqldf(f"Select wrr.* from winless_rider_results wrr inner join (select dr.rider_full_name,count(id_result) from fact_results fr left join dim_riders dr on fr.id_rider_fk = dr.id_rider\
                            group by dr.rider_full_name\
                            order by count(id_result) desc\
                            limit 200) as cut_off on cut_off.rider_full_name= wrr.rider_full_name\
                            where  wrr.season in {season_proc} and  wrr.racing_class in {racing_class}")
                    

    return df
def most_consecutive_finishes(season, racing_class):
    df = fetch_consecutive_results_aux(season,racing_class)
    cursor="None"
        # Function to fetch the name related to id_grandprix from the database
    def get_name_for_id_gp(id_grandprix, cursor):
        if st.session_state.UsingCSV:
            query=f"SELECT des_grandprix || ' ' || season FROM dim_grand_prix WHERE id_grandprix = {id_grandprix}"
            name =  psql.sqldf(query)
            name=name.iloc[0,0]

        else:
            conn = connect()
            cursor = conn.cursor()
            cursor.execute(f"SELECT CONCAT(des_grandprix,' ', season) FROM dim_grand_prix WHERE id_grandprix = {id_grandprix}")
            name = cursor.fetchone()[0]
            cursor.close()
            conn.close()
        return name

    # Example DataFrame
    # df = pd.DataFrame(result_args, columns=['rider_full_name', 'season', 'num_round', 'id_grandprix', 'race_type', 'final_position'])

    # Function to find the top N longest continuous successions of numeric strings and their respective riders
    def top_n_longest_successions(series, n, cursor):
        successions = []
        current_succession = 0
        current_riders = set()  # Using a set to store unique rider names
        current_id_gp = None
        for i, value in enumerate(series):
            if value.isnumeric():
                current_succession += 1
                current_riders.add(df['rider_full_name'][i])
                if current_id_gp is None:
                    current_id_gp = df['id_grandprix'][i]
            else:
                if current_succession > 0:
                    first_id_gp = df['id_grandprix'][i - current_succession]
                    last_id_gp = df['id_grandprix'][i - 1]
                    # first_name = get_name_for_id_gp(first_id_gp, cursor)
                    # last_name = get_name_for_id_gp(last_id_gp, cursor)
                    for rider in current_riders:
                        successions.append((current_succession, rider, first_id_gp, last_id_gp))
                    current_succession = 0
                    current_riders = set()
                    current_id_gp = None
        # Check if the last sequence is included
        if current_succession > 0:
            first_id_gp = df['id_grandprix'].iloc[-current_succession]
            last_id_gp = df['id_grandprix'].iloc[-1]
            # first_name = get_name_for_id_gp(first_id_gp, cursor)
            # last_name = get_name_for_id_gp(last_id_gp, cursor)
            for rider in current_riders:
                successions.append((current_succession, rider, first_id_gp, last_id_gp))
        # Sort the successions by length in descending order
        successions.sort(reverse=True)

        successions = successions[:n].copy()
        for i, (current_succession, rider, first_id_gp, last_id_gp) in enumerate(successions):

            first_gp_name = get_name_for_id_gp(successions[i][2], cursor)
            last_gp_name = get_name_for_id_gp(successions[i][3], cursor)
            successions[i] = (current_succession, rider, first_gp_name, last_gp_name)

        return successions

    

    # Get the top 10 longest successions and their respective riders with names for the first and last id_grandprix
    top_10_successions = top_n_longest_successions(df['final_position'], 10, cursor)
    
    # Convert the list of tuples to a DataFrame
    result_df = pd.DataFrame(top_10_successions, columns=['Succession Length', 'Rider', 'First Name', 'Last Name'])

    # Close the cursor and connection
    

    # Output the DataFrame
    return result_df


def most_consecutive_podiums(season, racing_class):
    df = fetch_consecutive_results_aux(season,racing_class)
    cursor=None
        # Function to fetch the name related to id_grandprix from the database
    def get_name_for_id_gp(id_grandprix, cursor):
        if st.session_state.UsingCSV:
            query=f"SELECT des_grandprix || ' ' || season FROM dim_grand_prix WHERE id_grandprix = {id_grandprix}"
            name =  psql.sqldf(query)
            name=name.iloc[0,0]
        else:
            conn = connect()
            cursor = conn.cursor()
            cursor.execute(f"SELECT CONCAT(des_grandprix,' ', season) FROM dim_grand_prix WHERE id_grandprix = {id_grandprix}")
            name = cursor.fetchone()[0]
            cursor.close()
            conn.close()
        return name

    # Example DataFrame
    # df = pd.DataFrame(result_args, columns=['rider_full_name', 'season', 'num_round', 'id_grandprix', 'race_type', 'final_position'])

    # Function to find the top N longest continuous successions of numeric strings and their respective riders
    def top_n_longest_successions(series, n, cursor):
        successions = []
        current_succession = 0
        current_riders = set()  # Using a set to store unique rider names
        current_id_gp = None
        for i, value in enumerate(series):
            if value in ['1','2','3']:
                current_succession += 1
                current_riders.add(df['rider_full_name'][i])
                if current_id_gp is None:
                    current_id_gp = df['id_grandprix'][i]
            else:
                if current_succession > 0:
                    first_id_gp = df['id_grandprix'][i - current_succession]
                    last_id_gp = df['id_grandprix'][i - 1]
                    for rider in current_riders:
                        successions.append((current_succession, rider, first_id_gp, last_id_gp))
                    current_succession = 0
                    current_riders = set()
                    current_id_gp = None
        # Check if the last sequence is included
        if current_succession > 0:
            first_id_gp = df['id_grandprix'].iloc[-current_succession]
            last_id_gp = df['id_grandprix'].iloc[-1]
            for rider in current_riders:
                successions.append((current_succession, rider, first_id_gp, last_id_gp))
        # Sort the successions by length in descending order
        successions.sort(reverse=True)

        successions = successions[:n].copy()

        for i, (current_succession, rider, first_id_gp, last_id_gp) in enumerate(successions):
            first_gp_name = get_name_for_id_gp(successions[i][2], cursor)
            last_gp_name = get_name_for_id_gp(successions[i][3], cursor)
            successions[i] = (current_succession, rider, first_gp_name, last_gp_name)

        return successions

    

    # Get the top 10 longest successions and their respective riders with names for the first and last id_grandprix
    top_10_successions = top_n_longest_successions(df['final_position'], 10, cursor)

    # Convert the list of tuples to a DataFrame
    result_df = pd.DataFrame(top_10_successions, columns=['Succession Length', 'Rider', 'First Name', 'Last Name'])

    # Close the cursor and connection
    

    # Output the DataFrame
    return result_df

def most_consecutive_wins(season, racing_class):
    df = fetch_consecutive_results_aux(season,racing_class)
    cursor=None
        # Function to fetch the name related to id_grandprix from the database
    def get_name_for_id_gp(id_grandprix, cursor):
        if st.session_state.UsingCSV:
            query=f"SELECT des_grandprix || ' ' || season FROM dim_grand_prix WHERE id_grandprix = {id_grandprix}"
            name =  psql.sqldf(query)
            name=name.iloc[0,0]
        else:
            conn = connect()
            cursor = conn.cursor()
            cursor.execute(f"SELECT CONCAT(des_grandprix,' ', season) FROM dim_grand_prix WHERE id_grandprix = {id_grandprix}")
            name = cursor.fetchone()[0]
            cursor.close()
            conn.close()
        return name

    # Example DataFrame
    # df = pd.DataFrame(result_args, columns=['rider_full_name', 'season', 'num_round', 'id_grandprix', 'race_type', 'final_position'])

    # Function to find the top N longest continuous successions of numeric strings and their respective riders
    def top_n_longest_successions(series, n, cursor):
        successions = []
        current_succession = 0
        current_riders = set()  # Using a set to store unique rider names
        current_id_gp = None
        for i, value in enumerate(series):
            if value in ['1']:
                current_succession += 1
                current_riders.add(df['rider_full_name'][i])
                if current_id_gp is None:
                    current_id_gp = df['id_grandprix'][i]
            else:
                if current_succession > 0:
                    first_id_gp = df['id_grandprix'][i - current_succession]
                    last_id_gp = df['id_grandprix'][i - 1]
                    for rider in current_riders:
                        successions.append((current_succession, rider, first_id_gp, last_id_gp))
                    current_succession = 0
                    current_riders = set()
                    current_id_gp = None
        # Check if the last sequence is included
        if current_succession > 0:
            first_id_gp = df['id_grandprix'].iloc[-current_succession]
            last_id_gp = df['id_grandprix'].iloc[-1]
            for rider in current_riders:
                successions.append((current_succession, rider, first_id_gp, last_id_gp))
        # Sort the successions by length in descending order
        successions.sort(reverse=True)
        successions = successions[:n].copy()

        for i, (current_succession, rider, first_id_gp, last_id_gp) in enumerate(successions):
            first_gp_name = get_name_for_id_gp(successions[i][2], cursor)
            last_gp_name = get_name_for_id_gp(successions[i][3], cursor)
            successions[i] = (current_succession, rider, first_gp_name, last_gp_name)

        return successions

    

    # Get the top 10 longest successions and their respective riders with names for the first and last id_grandprix
    top_10_successions = top_n_longest_successions(df['final_position'], 10, cursor)

    # Convert the list of tuples to a DataFrame
    result_df = pd.DataFrame(top_10_successions, columns=['Succession Length', 'Rider', 'First Name', 'Last Name'])

    

    # Output the DataFrame
    return result_df


def most_consecutive_fails(season, racing_class):
    df = fetch_consecutive_results_aux(season,racing_class)
    cursor=None
        # Function to fetch the name related to id_grandprix from the database
    def get_name_for_id_gp(id_grandprix, cursor):
        if st.session_state.UsingCSV:
            query=f"SELECT des_grandprix || ' ' || season FROM dim_grand_prix WHERE id_grandprix = {id_grandprix}"
            name =  psql.sqldf(query)
            name=name.iloc[0,0]
        else:
            conn = connect()
            cursor = conn.cursor()
            cursor.execute(f"SELECT CONCAT(des_grandprix,' ', season) FROM dim_grand_prix WHERE id_grandprix = {id_grandprix}")
            name = cursor.fetchone()[0]
            cursor.close()
            conn.close()
        return name

    # Example DataFrame
    # df = pd.DataFrame(result_args, columns=['rider_full_name', 'season', 'num_round', 'id_grandprix', 'race_type', 'final_position'])

    # Function to find the top N longest continuous successions of numeric strings and their respective riders
    def top_n_longest_successions(series, n, cursor):
        successions = []
        current_succession = 0
        current_riders = set()  # Using a set to store unique rider names
        current_id_gp = None

        for i, value in enumerate(series):
            if (not str(value).isnumeric()) and value !='' :
                current_succession += 1
                current_riders.add(df['rider_full_name'][i])
                if current_id_gp is None:
                    current_id_gp = df['id_grandprix'][i]
            else:
                if current_succession > 0:
                    
                    first_id_gp = df['id_grandprix'][i - current_succession]
                    last_id_gp = df['id_grandprix'][i - 1]
                    # if first_name !='Gauloises Grand Prix České republiky 2003' and first_name != 'Gauloises Pacific Grand Prix of Motegi 2002':
                    for rider in current_riders:
                        successions.append((current_succession, rider, first_id_gp, last_id_gp))
                    current_succession = 0
                    current_riders = set()
                    current_id_gp = None
        # Check if the last sequence is included
        if current_succession > 0:
            first_id_gp = df['id_grandprix'].iloc[-current_succession]

            last_id_gp = df['id_grandprix'].iloc[-1]
            
            for rider in current_riders:
                successions.append((current_succession, rider, first_id_gp, last_id_gp))
            current_succession = 0
            current_riders = set()
            current_id_gp = None
        # Sort the successions by length in descending order
        successions.sort(reverse=True)
        successions = successions[:n].copy()

        for i, (current_succession, rider, first_id_gp, last_id_gp) in enumerate(successions):
            first_gp_name = get_name_for_id_gp(successions[i][2], cursor)
            last_gp_name = get_name_for_id_gp(successions[i][3], cursor)
            successions[i] = (current_succession, rider, first_gp_name, last_gp_name)

        return successions

    

    # Get the top 10 longest successions and their respective riders with names for the first and last id_grandprix
    top_10_successions = top_n_longest_successions(df['final_position'], 10, cursor)
    
    # Convert the list of tuples to a DataFrame
    result_df = pd.DataFrame(top_10_successions, columns=['Succession Length', 'Rider', 'First Name', 'Last Name'])

    

    # Output the DataFrame
    return result_df


def soonest_win(season, racing_class):
    df = fetch_consecutive_results_aux(season,racing_class)
    cursor=None
        # Function to fetch the name related to id_grandprix from the database
    def get_name_for_id_gp(id_grandprix, cursor):
        if st.session_state.UsingCSV:
            query=f"SELECT des_grandprix || ' ' || season FROM dim_grand_prix WHERE id_grandprix = {id_grandprix}"
            name =  psql.sqldf(query)
            name=name.iloc[0,0]
        else:
            conn = connect()
            cursor = conn.cursor()
            cursor.execute(f"SELECT CONCAT(des_grandprix,' ', season) FROM dim_grand_prix WHERE id_grandprix = {id_grandprix}")
            name = cursor.fetchone()[0]
            cursor.close()
            conn.close()
        return name

    # Example DataFrame
    # df = pd.DataFrame(result_args, columns=['rider_full_name', 'season', 'num_round', 'id_grandprix', 'race_type', 'final_position'])

    # Function to find the top N longest continuous successions of numeric strings and their respective riders
    def top_n_longest_successions(series, n, cursor):
        successions = []
        current_succession = 0
        current_riders = set()  # Using a set to store unique rider names
        current_id_gp = None

        grouped = df.groupby('rider_full_name')

        for rider, data in grouped:
            # if rider == 'Aaron Polanco':
                found=False
                final_gp = 0
                first_gp= data['id_grandprix'].iloc[0]
                first_gp_index = df[(df['rider_full_name'] == rider) & (df['id_grandprix'] == first_gp)].index[0]

                if any(data['final_position'] == '1') and first_gp>66:
                    first_position_index = data[data['final_position'] == '1'].index[0]
                    id_grandprix_first_position = data.loc[first_position_index, 'id_grandprix']

                    num_gp= first_position_index - first_gp_index

                    id_grandprix_data = {
                        'rider': rider,
                        'first_gp':  first_gp,
                        'win_gp': id_grandprix_first_position,
                        'num_gp':num_gp
                    }
                    successions.append(id_grandprix_data)
                else:
                    first_position_index='-'
                    id_grandprix_first_position = None
               
                current_riders = set()

        successions.sort( key=lambda x: x['num_gp'],reverse=False)
        successions = successions[:n].copy()

        for data in successions:
            data["first_gp"] = get_name_for_id_gp(data["first_gp"], cursor)
            data["win_gp"]= get_name_for_id_gp(data["win_gp"], cursor)

        return successions

    

    # Get the top 10 longest successions and their respective riders with names for the first and last id_grandprix
    top_10_successions = top_n_longest_successions(df['final_position'], 10, cursor)
    
    # Convert the list of tuples to a DataFrame
    
    result_df = pd.DataFrame(top_10_successions, columns=['rider', 'first_gp', 'win_gp','num_gp'])

    

    # Output the DataFrame
    return result_df


def longest_win(season, racing_class):
    df = fetch_consecutive_results_aux(season,racing_class)
    cursor=None
        # Function to fetch the name related to id_grandprix from the database
    def get_name_for_id_gp(id_grandprix, cursor):
        if st.session_state.UsingCSV:
            query=f"SELECT des_grandprix || ' ' || season FROM dim_grand_prix WHERE id_grandprix = {id_grandprix}"
            name =  psql.sqldf(query)
            name=name.iloc[0,0]
        else:
            conn = connect()
            cursor = conn.cursor()
            cursor.execute(f"SELECT CONCAT(des_grandprix,' ', season) FROM dim_grand_prix WHERE id_grandprix = {id_grandprix}")
            name = cursor.fetchone()[0]
            cursor.close()
            conn.close()
        return name

    # Example DataFrame
    # df = pd.DataFrame(result_args, columns=['rider_full_name', 'season', 'num_round', 'id_grandprix', 'race_type', 'final_position'])

    # Function to find the top N longest continuous successions of numeric strings and their respective riders
    def top_n_longest_successions(series, n, cursor):
        successions = []
        current_succession = 0
        current_riders = set()  # Using a set to store unique rider names
        current_id_gp = None

        grouped = df.groupby('rider_full_name')

        for rider, data in grouped:
            if rider != 'Randy Krummenacher':
                found=False
                final_gp = 0
                first_gp= data['id_grandprix'].iloc[0]
                first_gp_index = df[(df['rider_full_name'] == rider) & (df['id_grandprix'] == first_gp)].index[0]

                if any(data['final_position'] == '1') and first_gp>66:
                    first_position_index = data[data['final_position'] == '1'].index[0]
                    id_grandprix_first_position = data.loc[first_position_index, 'id_grandprix']

                    # num_gp = id_grandprix_first_position - first_gp
                    num_gp= first_position_index - first_gp_index
                    id_grandprix_data = {
                        'rider': rider,
                        'first_gp':  first_gp,
                        'win_gp': id_grandprix_first_position,
                        'num_gp':num_gp
                    }
                    successions.append(id_grandprix_data)
                else:
                    first_position_index='-'
                    id_grandprix_first_position = None
               
                current_riders = set()

        successions.sort( key=lambda x: x['num_gp'],reverse=True)
        successions = successions[:n].copy()

        for data in successions:
            data["first_gp"] = get_name_for_id_gp(data["first_gp"], cursor)
            data["win_gp"]= get_name_for_id_gp(data["win_gp"], cursor)

        return successions

    

    # Get the top 10 longest successions and their respective riders with names for the first and last id_grandprix
    top_10_successions = top_n_longest_successions(df['final_position'], 10, cursor)
    
    # Convert the list of tuples to a DataFrame
    
    result_df = pd.DataFrame(top_10_successions, columns=['rider', 'first_gp', 'win_gp','num_gp'])

    

    # Output the DataFrame
    return result_df

def longest_winless_streak(season, racing_class):
    # df = fetch_consecutive_results_aux(season,racing_class)
    df = fetch_winless_results_aux(season, racing_class)
    cursor=None
        # Function to fetch the name related to id_grandprix from the database
    def get_name_for_id_gp(id_grandprix, cursor):
        if st.session_state.UsingCSV:
            query=f"SELECT des_grandprix || ' ' || season FROM dim_grand_prix WHERE id_grandprix = {id_grandprix}"
            name =  psql.sqldf(query)
            name=name.iloc[0,0]
        else:
            conn = connect()
            cursor = conn.cursor()
            cursor.execute(f"SELECT CONCAT(des_grandprix,' ', season) FROM dim_grand_prix WHERE id_grandprix = {id_grandprix}")
            name = cursor.fetchone()[0]
            cursor.close()
            conn.close()
        return name

    def get_latest_id_gp(rider_name,cursor):
        if st.session_state.UsingCSV:
            query=f'SELECT id_grandprix from dim_grand_prix dgp left join fact_results fr on fr.id_grand_prix_fk = dgp.id_grandprix \
                        left join dim_riders dr on dr.id_rider = fr.id_rider_fk \
                        where dr.rider_full_name = "{rider_name}"\
                        order by id_grandprix desc limit 1'
            id_gp =  psql.sqldf(query)
            id_gp=id_gp.iloc[0,0]
        else:
            conn = connect()
            cursor = conn.cursor()
            cursor.execute(f'SELECT TOP(1) id_grandprix from dim_grand_prix dgp left join fact_results fr on fr.id_grand_prix_fk = dgp.id_grandprix \
                            left join dim_riders dr on dr.id_rider = fr.id_rider_fk \
                            where dr.rider_full_name = "{rider_name}"\
                            order by id_grandprix desc ')
            id_gp = cursor.fetchone()[0]
            cursor.close()
            conn.close()
        return id_gp
    # Example DataFrame


    # Function to find the top N longest continuous successions of numeric strings and their respective riders
    def top_n_longest_successions(series, n, cursor):
        successions = []
        current_succession = 0
        current_riders = set()  # Using a set to store unique rider names
        current_id_gp = None

        grouped = df.groupby('rider_full_name')
        winless = ['Marcel Schrötter',
                'Jakub Kornfeil',
                'Axel Pons',
                'Hafizh Syahrin',
                'Bo Bendsneyder',
                'Yonny Hernández',
                'Ratthapark Wilairot',
                'Gabriel Rodrigo',
                'Isaac Viñales',
                'Xavi Vierge']
        for rider, data in grouped:
            if rider in winless:
                first_gp= data['id_grandprix'].iloc[0]
                if any(data['final_position'] != '1') and first_gp > 1:
                    found=False
                    final_gp = 0
                    
                    first_gp_index = df[(df['rider_full_name'] == rider) & (df['id_grandprix'] == first_gp)].index[0]

                    
                    id_last_race = get_latest_id_gp(rider,cursor)
                    last_gp_index = df[(df['rider_full_name'] == rider) & (df['id_grandprix'] == id_last_race)].index[0]
                    num_gp= last_gp_index - first_gp_index +1

                    id_grandprix_data = {
                            'rider': rider,
                            'first_gp':  first_gp,
                            'last_gp': id_last_race,
                            'num_gp':num_gp
                        }
                    successions.append(id_grandprix_data)
                    current_riders = set()

        successions.sort( key=lambda x: x['num_gp'],reverse=True)
        successions = successions[:n].copy()

        for data in successions:
            data["first_gp"] = get_name_for_id_gp(data["first_gp"], cursor)
            data["last_gp"]= get_name_for_id_gp(data["last_gp"], cursor)

        return successions

    

    # Get the top 10 longest successions and their respective riders with names for the first and last id_grandprix
    top_10_successions = top_n_longest_successions(df['final_position'], 10, cursor)
    
    # Convert the list of tuples to a DataFrame
    
    result_df = pd.DataFrame(top_10_successions, columns=['rider', 'first_gp', 'last_gp','num_gp'])

    

    # Output the DataFrame
    return result_df


def youngest_winner(season, racing_class):
    df = fetch_consecutive_results_aux(season,racing_class)
    cursor=None
        # Function to fetch the name related to id_grandprix from the database
    young_riders = ['Can Öncü',
            'Scott Redding',
            'Pedro Acosta',
            'Romano Fenati',
            'Jorge Lorenzo',
            'Maverick Viñales',
            'Héctor Barberá',
            'Sergio García',
            'Jonas Folger',
            'Daniel Holgado']
    def get_name_for_id_gp(id_grandprix, cursor):
        if st.session_state.UsingCSV:
            query=f"SELECT des_grandprix || ' ' || season FROM dim_grand_prix WHERE id_grandprix = {id_grandprix}"
            name =  psql.sqldf(query)
            name=name.iloc[0,0]
        else:
            conn = connect()
            cursor = conn.cursor()
            cursor.execute(f"SELECT CONCAT(des_grandprix,' ', season) FROM dim_grand_prix WHERE id_grandprix = {id_grandprix}")
            name = cursor.fetchone()[0]
            cursor.close()
            conn.close()
        return name

    def fetch_age(id_gp,rider_name):
        if st.session_state.UsingCSV:
            query=f"SELECT dr.rider_full_name,\
                        CAST((julianday(dgp.gp_date) - julianday(dr.date_birth)) / 365 AS INT) || ' years ' ||\
                        CAST(((julianday(dgp.gp_date) - julianday(dr.date_birth)) % 365) / 30 AS INT) || ' months ' ||\
                        CAST(((julianday(dgp.gp_date) - julianday(dr.date_birth)) % 365) % 30 AS INT) || ' days' AS years_months_days,\
                        (julianday(dgp.gp_date) - julianday(dr.date_birth)) AS total_days\
                        FROM \
                         dim_grand_prix dgp \
                            left join fact_results fr on fr.id_grand_prix_fk = dgp.id_grandprix \
                            left join dim_riders dr on dr.id_rider = fr.id_rider_fk \
                            where fr.id_grand_prix_fk = {id_gp} and dr.rider_full_name = '{rider_name}'\
                            group by dr.rider_full_name,dgp.gp_date, dr.date_birth"
            name =  psql.sqldf(query)
            # st.dataframe(name)
            name['total_days'] = name['total_days'].fillna(0)
            name['total_days'] = name['total_days'].astype(int)
            name_filtered = name[name['years_months_days'].notna()]
            return name_filtered
        else:
            conn = connect()
            cursor = conn.cursor()
            cursor.execute(f"select dr.rider_full_name,\
                            EXTRACT(YEAR FROM age(dgp.gp_date, dr.date_birth)) || ' years ' ||\
                                EXTRACT(MONTH FROM age(dgp.gp_date, dr.date_birth)) || ' months ' ||\
                                EXTRACT(DAY FROM age(dgp.gp_date, dr.date_birth)) || ' days' AS years_months_days,\
                                dgp.gp_date-dr.date_birth AS total_days\
                            from dim_grand_prix dgp \
                            left join fact_results fr on fr.id_grand_prix_fk = dgp.id_grandprix \
                            left join dim_riders dr on dr.id_rider = fr.id_rider_fk \
                            where fr.id_grand_prix_fk = {id_gp} and dr.rider_full_name = '{rider_name}'\
                            group by dr.rider_full_name,dgp.gp_date, dr.date_birth")
            name = cursor.fetchone()[0]
            cursor.close()
            conn.close()
        return name

    # Function to find the top N longest continuous successions of numeric strings and their respective riders
    def top_n_longest_successions(series, n, cursor):
        successions = []
        current_succession = 0
        current_riders = set()  # Using a set to store unique rider names
        current_id_gp = None

        grouped = df.groupby('rider_full_name')

        for rider, data in grouped:
            if rider in young_riders:
                
                found=False
                final_gp = 0
                first_gp= data['id_grandprix'].iloc[0]
                first_gp_index = df[(df['rider_full_name'] == rider) & (df['id_grandprix'] == first_gp)].index[0]

                if any(data['final_position'] == '1'):
                    first_position_index = data[data['final_position'] == '1'].index[0]
                    id_grandprix_first_position = data.loc[first_position_index, 'id_grandprix']

                    age = fetch_age(id_grandprix_first_position, rider)
                    if not age.empty:
                        id_grandprix_data = {
                            'rider': rider,
                            'win_gp': id_grandprix_first_position,
                            'age':age.iloc[0, 0],
                            'days': age.iloc[0, 1]
                        }
                        successions.append(id_grandprix_data)
                else:
                    first_position_index='-'
                    id_grandprix_first_position = None
               
                current_riders = set()

        successions.sort( key=lambda x: x['days'],reverse=False)
        successions = successions[:n].copy()

        for data in successions:
            data["win_gp"]= get_name_for_id_gp(data["win_gp"], cursor)

        return successions

    

    # Get the top 10 longest successions and their respective riders with names for the first and last id_grandprix
    top_10_successions = top_n_longest_successions(df['final_position'], 10, cursor)
    
    # Convert the list of tuples to a DataFrame
    
    result_df = pd.DataFrame(top_10_successions, columns=['rider', 'win_gp', 'age','days'])

    

    # Output the DataFrame
    return result_df


def oldest_winner(season, racing_class):
    df = fetch_consecutive_results_aux(season,racing_class)
    cursor=None
        # Function to fetch the name related to id_grandprix from the database
    def get_name_for_id_gp(id_grandprix, cursor):
        if st.session_state.UsingCSV:
            query=f"SELECT des_grandprix || ' ' || season FROM dim_grand_prix WHERE id_grandprix = {id_grandprix}"
            name =  psql.sqldf(query)
            name=name.iloc[0,0]
        else:
            conn = connect()
            cursor = conn.cursor()
            cursor.execute(f"SELECT CONCAT(des_grandprix,' ', season) FROM dim_grand_prix WHERE id_grandprix = {id_grandprix}")
            name = cursor.fetchone()[0]
            cursor.close()
            conn.close()
        return name

    def fetch_age(id_gp,rider_name):
        if st.session_state.UsingCSV:
            query=f"SELECT \
                        CAST((julianday(dgp.gp_date) - julianday(dr.date_birth)) / 365 AS INT) || ' years ' ||\
                        CAST(((julianday(dgp.gp_date) - julianday(dr.date_birth)) % 365) / 30 AS INT) || ' months ' ||\
                        CAST(((julianday(dgp.gp_date) - julianday(dr.date_birth)) % 365) % 30 AS INT) || ' days' AS years_months_days,\
                        (julianday(dgp.gp_date) - julianday(dr.date_birth)) AS total_days\
                        FROM \
                         dim_grand_prix dgp \
                            left join fact_results fr on fr.id_grand_prix_fk = dgp.id_grandprix \
                            left join dim_riders dr on dr.id_rider = fr.id_rider_fk \
                            where fr.id_grand_prix_fk = {id_gp} and dr.rider_full_name = '{rider_name}'\
                            group by dr.rider_full_name,dgp.gp_date, dr.date_birth\
                            "
            name =  psql.sqldf(query)
            # st.dataframe(name)
            name['total_days'] = name['total_days'].fillna(0)
            name['total_days'] = name['total_days'].astype(int)
            name_filtered = name[name['years_months_days'].notna()]
            return name_filtered
        else:
            conn = connect()
            cursor = conn.cursor()
            cursor.execute(f"select \
                            EXTRACT(YEAR FROM age(dgp.gp_date, dr.date_birth)) || ' years ' ||\
                                EXTRACT(MONTH FROM age(dgp.gp_date, dr.date_birth)) || ' months ' ||\
                                EXTRACT(DAY FROM age(dgp.gp_date, dr.date_birth)) || ' days' AS years_months_days,\
                                dgp.gp_date-dr.date_birth AS total_days\
                            from dim_grand_prix dgp \
                            left join fact_results fr on fr.id_grand_prix_fk = dgp.id_grandprix \
                            left join dim_riders dr on dr.id_rider = fr.id_rider_fk \
                            where fr.id_grand_prix_fk = {id_gp} and dr.rider_full_name = '{rider_name}'\
                            group by dr.rider_full_name,dgp.gp_date, dr.date_birth")
            name = cursor.fetchone()[0]
            cursor.close()
            conn.close()
        return name

    # Function to find the top N longest continuous successions of numeric strings and their respective riders
    def top_n_longest_successions(series, n, cursor):
        successions = []
        current_succession = 0
        current_riders = set()  # Using a set to store unique rider names
        current_id_gp = None

        grouped = df.groupby('rider_full_name')

        old_riders = ['Troy Bayliss',
            'Johann Zarco',
            'Osamu Miyazaki',
            'Danilo Petrucci',
            'Aleix Espargaró',
            'Alex Barros',
            'Max Biaggi',
            'Cal Crutchlow',
            'Sete Gibernau',
            'Loris Capirossi']
        for rider, data in grouped:
            if rider in old_riders:
                
                found=False
                final_gp = 0
                first_gp= data['id_grandprix'].iloc[0]
                first_gp_index = df[(df['rider_full_name'] == rider) & (df['id_grandprix'] == first_gp)].index[0]

                if any(data['final_position'] == '1'):
                    first_position_index = data[data['final_position'] == '1'].index[-1]
                    id_grandprix_first_position = data.loc[first_position_index, 'id_grandprix']

                    age = fetch_age(id_grandprix_first_position, rider)
                    if not age.empty:
                        id_grandprix_data = {
                            'rider': rider,
                            'win_gp': id_grandprix_first_position,
                            'age':age.iloc[0, 0],
                            'days': age.iloc[0, 1]
                        }
                        successions.append(id_grandprix_data)
                else:
                    first_position_index='-'
                    id_grandprix_first_position = None
               
                current_riders = set()

        successions.sort( key=lambda x: x['days'],reverse=True)
        successions = successions[:n].copy()

        for data in successions:
            data["win_gp"]= get_name_for_id_gp(data["win_gp"], cursor)

        return successions

    

    # Get the top 10 longest successions and their respective riders with names for the first and last id_grandprix
    top_10_successions = top_n_longest_successions(df['final_position'], 10, cursor)
    
    # Convert the list of tuples to a DataFrame
    
    result_df = pd.DataFrame(top_10_successions, columns=['rider', 'win_gp', 'age','days'])

    

    # Output the DataFrame
    return result_df




















