select dr.rider_full_name, sum(dp.num_points) as points  from fact_results f
left join dim_grand_prix dgp on f.id_grand_prix_fk = dgp.id_grandprix 
left join dim_riders dr on  dr.id_rider = f.id_rider_fk 
left join dim_positions dp on dp.id_position = f.id_position_fk 
where f.racing_class = 'moto-e' and f.season  = 2023
group by dr.rider_full_name
order by points desc;

-------------------------------clasificacion moto2-250cc 
select dr.rider_full_name, sum(dp.num_points) as points  from fact_results f
left join dim_grand_prix dgp on f.id_grand_prix_fk = dgp.id_grandprix 
left join dim_riders dr on  dr.id_rider = f.id_rider_fk 
left join dim_positions dp on dp.id_position = f.id_position_fk 
where (f.racing_class = 'moto2' or f.racing_class = '250cc')  and f.season  = 2002
group by dr.rider_full_name
order by points desc;

--------------------------------Mayor numero de victorias por temporada
select dr.rider_full_name , dr.season, dr.racing_class , count(dp.id_position) as total_points from fact_results fr 
left join dim_positions dp on dp.id_position = fr.id_position_fk 
left join dim_riders dr on dr.id_rider = fr.id_rider_fk 
where dp.final_position = '1' and dp.race_type = 'main'
group by dr.rider_full_name, dr.season, dr.racing_class
order by total_points desc
LIMIT 1

----------------------------Mayor numero de podios por temporada
select dr.rider_full_name , dr.season, dr.racing_class , count(dp.id_position) as total_podiums from fact_results fr 
left join dim_positions dp on dp.id_position = fr.id_position_fk 
left join dim_riders dr on dr.id_rider = fr.id_rider_fk 
where dp.final_position in ('1','2','3') and dp.race_type = 'main' and dr.racing_class = 'motogp'
group by dr.rider_full_name, dr.season, dr.racing_class
order by total_podiums desc
LIMIT 1

----------------------------Mayor numero de victorias por temporada
select dr.rider_full_name , dr.season, dr.racing_class , count(dp.id_position) as total_wins from fact_results fr 
left join dim_positions dp on dp.id_position = fr.id_position_fk 
left join dim_riders dr on dr.id_rider = fr.id_rider_fk 
where dp.final_position in ('1') and dp.race_type = 'main' and (dr.racing_class = 'moto2' or dr.racing_class = '250cc')
group by dr.rider_full_name, dr.season, dr.racing_class
order by total_wins desc
LIMIT 1
----------------------------Mayor numero de podios por temporada
select dr.rider_full_name , dr.season, dr.racing_class , count(dp.id_position) as total_podiums from fact_results fr 
left join dim_positions dp on dp.id_position = fr.id_position_fk 
left join dim_riders dr on dr.id_rider = fr.id_rider_fk 
where dp.final_position in ('1','2','3') and dp.race_type = 'main' and (dr.racing_class = 'moto2' or dr.racing_class = '250cc')
group by dr.rider_full_name, dr.season, dr.racing_class
order by total_podiums desc
LIMIT 1

----------------------------Mayor numero de victorias por temporada
select dr.rider_full_name , dr.season, dr.racing_class , count(dp.id_position) as total_wins from fact_results fr 
left join dim_positions dp on dp.id_position = fr.id_position_fk 
left join dim_riders dr on dr.id_rider = fr.id_rider_fk 
where dp.final_position in ('1') and dp.race_type = 'main' and (dr.racing_class = 'moto3' or dr.racing_class = '125cc')
group by dr.rider_full_name, dr.season, dr.racing_class
order by total_wins desc
LIMIT 1

----------------------------Mayor numero de podios por temporada
select dr.rider_full_name , dr.season, dr.racing_class , count(dp.id_position) as total_podiums from fact_results fr 
left join dim_positions dp on dp.id_position = fr.id_position_fk 
left join dim_riders dr on dr.id_rider = fr.id_rider_fk 
where dp.final_position in ('1','2','3') and dp.race_type = 'main' and (dr.racing_class = 'moto3' or dr.racing_class = '125cc')
group by dr.rider_full_name, dr.season, dr.racing_class
order by total_podiums desc
LIMIT 1


----------------------------Mayor numero de victorias por temporada
select dr.rider_full_name , dr.season, dr.racing_class , count(dp.id_position) as total_wins from fact_results fr 
left join dim_positions dp on dp.id_position = fr.id_position_fk 
left join dim_riders dr on dr.id_rider = fr.id_rider_fk 
where dp.final_position in ('1') and dp.race_type = 'main' and dr.racing_class = 'moto-e'
group by dr.rider_full_name, dr.season, dr.racing_class
order by total_wins desc
LIMIT 1

----------------------------Mayor numero de podios por temporada
select dr.rider_full_name , dr.season, dr.racing_class , count(dp.id_position) as total_podiums from fact_results fr 
left join dim_positions dp on dp.id_position = fr.id_position_fk 
left join dim_riders dr on dr.id_rider = fr.id_rider_fk 
where dp.final_position in ('1','2','3') and dp.race_type = 'main' and dr.racing_class = 'moto-e'
group by dr.rider_full_name, dr.season, dr.racing_class
order by total_podiums desc
LIMIT 1


---------------------------------
select * 
from fact_results fr left join dim_positions dp on dp.id_position = fr.id_position_fk 
 where season= 2020
 
 select * from dim_positions dp 
 
 
 
 
 
 -- contar num pilotos 2023 sin datos biográficos

 select * from dim_riders dr where season = 2010 and rider_weight is null and rider_height is null and birth_location is null;
  
select racing_class, season, cast(avg(rider_height) as NUMERIC(5, 2)), Count(id_rider)  from dim_riders dr 
where racing_class= 'motogp' and id_rider in (select id_rider from dim_riders dr2 where rider_height is not null)
  group by racing_class, season
  order by season
  
  select racing_class, season, cast(avg(rider_weight) as NUMERIC(5, 2)), Count(id_rider)  from dim_riders dr 
where racing_class= 'motogp' and id_rider in (select id_rider from dim_riders dr2 where rider_weight  is not null)
  group by racing_class, season
  order by season

  
select racing_class, season, cast(max(rider_height) as NUMERIC(5, 2))  from dim_riders dr 
where racing_class= 'motogp' and id_rider in (select id_rider from dim_riders dr2 where rider_height is not null)
  group by racing_class, season
  order by season
 
  select rider_full_name from dim_riders dr where rider_height = 191
  
  
select count(id_rider) from dim_riders dr2 where rider_height is not null and season =2002 and racing_class = 'motogp'



-------------------------------------Acumulado de puntos por carrera
select fr.id_result,dgp.des_grandprix,fr.race_type, dr.rider_full_name, dp.num_points,SUM(dp.num_points) over (partition by fr.id_rider_fk order by fr.id_result) as cummulative_sum from fact_results fr left join dim_riders dr ON dr.id_rider = fr.id_rider_fk 
left join dim_positions dp on dp.id_position =fr.id_position_fk 
left join dim_grand_prix dgp on dgp.id_grandprix = fr.id_grand_prix_fk
where fr.season = 2023 and fr.racing_class = 'motogp'
group by fr.id_result, dgp.des_grandprix,dr.rider_full_name,dp.num_points
order by fr.id_result, dr.rider_full_name

select distinct fr.num_round, dr.rider_full_name, SUM(dp.num_points) over (partition by fr.id_rider_fk order by fr.num_round) as cummulative_sum 
from fact_results fr left join dim_riders dr ON dr.id_rider = fr.id_rider_fk 
left join dim_positions dp on dp.id_position =fr.id_position_fk 
left join dim_grand_prix dgp on dgp.id_grandprix = fr.id_grand_prix_fk
where fr.season = 2023 and fr.racing_class = 'motogp'
group by fr.num_round, dgp.des_grandprix,dr.rider_full_name,dp.num_points, fr.id_result
order by fr.num_round, dr.rider_full_name


----------------------------------------Loc de gps por temporada
select des_track, dt.longitude  ,dt.latitude from dim_grand_prix dgp
left join  dim_tracks dt on id_track_fk =id_track
where season = 2011

----------------------------------------Loc de pilotos por temporada
select rider_full_name, birth_latitude, birth_longitude, Count(birth_latitude)  from dim_riders dr 
where season = 2011
group by rider_full_name, birth_latitude, birth_longitude


------------
select dt.des_team, fr.num_round , dr.rider_full_name , SUM(dp.num_points) as points from dim_teams dt  
left join dim_riders dr on dr.rider_full_name  = dt.rider_full_name and dr.season = dt.season 
left join fact_results fr on fr.id_rider_fk = dr.id_rider
left join dim_positions dp on dp.id_position = fr.id_position_fk
where fr.season=2023 and fr.racing_class = 'motogp' and dt.des_team ='LCR Honda' --and fr.num_round = ANY (dr.rounds_participated)
group by dt.des_team,fr.num_round, dr.rider_full_name
order by fr.num_round asc

select dt.des_team,  SUM(dp.num_points) as points from dim_teams dt  
left join dim_riders dr on dr.rider_full_name  = dt.rider_full_name and dr.season = dt.season 
left join fact_results fr on fr.id_rider_fk = dr.id_rider
left join dim_positions dp on dp.id_position = fr.id_position_fk
where fr.season=2023 and fr.racing_class = 'motogp' --and dt.des_team ='LCR Honda' --and fr.num_round = ANY (dr.rounds_participated)
group by dt.des_team
order by points desc;

select dt.des_team,  SUM(dp.num_points) as points from dim_teams dt  
left join dim_riders dr on dr.id_team_fk  = dt.id_team  and dr.season = dt.season 
left join fact_results fr on fr.id_rider_fk = dr.id_rider
left join dim_positions dp on dp.id_position = fr.id_position_fk
where fr.season=2023 and fr.racing_class = 'motogp' --and dt.des_team ='LCR Honda' --and fr.num_round = ANY (dr.rounds_participated)
group by dt.des_team
order by points desc

select rider_full_name, dr.rounds_participated ,dt.* from dim_teams dt 
left join dim_riders dr on dr.id_team_fk = dt.id_team
where dt.des_team ='LCR Honda' and dt.season = 2023




select dt.des_team ,* from dim_riders dr left join fact_results fr on fr.id_rider_fk =dr.id_rider
left join dim_teams dt on dt.id_team =dr.id_team_fk 
where rider_full_name like '%Bradl%' and fr.season = 2023 and fr.num_round = ANY (dr.rounds_participated)

select * from dim_teams 

select * from dim_riders where rider_full_name like '%Bradl%' and season = 2023

--------------------Calsificacion COnstructores
WITH BestResults AS (
    select
 		distinct
    	dc.des_constructor,
    	fr.num_round,
        fr.race_type,
        fr.season,
        MAX(dp.num_points) AS best_points
 
    FROM
        fact_results fr
    JOIN dim_positions dp ON fr.id_position_fk = dp.id_position
    left join dim_riders dr on dr.id_rider = fr.id_rider_fk
    left join dim_teams dt on dt.id_team = dr.id_team_fk
    left join dim_constructors dc on dc.id_constructor = dt.id_constructor_fk
    where dr.season=2006 and dr.racing_class='motogp'

    GROUP by 
   		
    	dc.des_constructor,
    	fr.num_round,
        fr.race_type,
        fr.season,
        fr.id_result
     
        
)

select
distinct
    dc.des_constructor,
    dc.season,
    SUM(br.best_points) AS total_points
FROM
    dim_constructors dc 
JOIN BestResults br ON dc.des_constructor  = br.des_constructor AND dc.season = br.season

GROUP BY
    dc.id_constructor,
    dc.season
order by total_points desc



--------------------Calsificacion Equipos
WITH BestResults AS (
    select
 		distinct
    	dt.des_team,
    	fr.num_round,
        fr.season,
        dp.num_points AS best_points
 
    FROM
        fact_results fr
    JOIN dim_positions dp ON fr.id_position_fk = dp.id_position
    left join dim_riders dr on dr.id_rider = fr.id_rider_fk
    left join dim_teams dt on dt.id_team = dr.id_team_fk
    left join dim_constructors dc on dc.id_constructor = dt.id_constructor_fk
    where dr.season=2023 and dr.racing_class='motogp'

    GROUP by 
    	dt.des_team,
    	fr.num_round,
        fr.race_type,
        fr.season,
        best_points
     
        
)

select
distinct
    dt.des_team,
    dt.season,
    SUM(br.best_points) AS total_points
FROM
    dim_teams dt 
JOIN BestResults br ON dt.des_team  = br.des_team AND dt.season = br.season

GROUP BY
   dt.des_team,
    dt.season
order by total_points desc


------------------

select distinct rider_full_name,count(dr.id_rider) as counter from dim_riders dr where season=2023 and not
(date_birth is not null or birth_latitude is not null or birth_longitude is not null or loc_birth is not null or
rider_height is not null or rider_weight is not null)
group by rider_full_name
order by counter desc



select distinct rider_full_name, loc_birth ,birth_longitude ,birth_latitude  from dim_riders dr where loc_birth is not null
and birth_longitude is null and birth_latitude is null

----------------------

WITH BestResults AS ( 
                select
                    distinct
                    dc.des_constructor,
                    fr.num_round,
                    fr.race_type,
                    fr.season,
                    MAX(dp.num_points) AS best_points
                FROM
                    fact_results fr
                JOIN dim_positions dp ON fr.id_position_fk = dp.id_position
                left join dim_riders dr on dr.id_rider = fr.id_rider_fk
                left join dim_teams dt on dt.id_team = dr.id_team_fk
                left join dim_constructors dc on dc.id_constructor = dt.id_constructor_fk
                where dr.season=2023 and dr.racing_class='motogp'
                GROUP by 
                    dc.des_constructor,
                    fr.num_round,
                    fr.race_type,
                    fr.season 
                )
                select
                distinct
                    dc.des_constructor,
                    dc.season,
                    SUM(br.best_points) AS total_points
                FROM
                    dim_constructors dc 
                JOIN BestResults br ON dc.des_constructor  = br.des_constructor AND dc.season = br.season
                GROUP BY
                    dc.id_constructor,
                    dc.season
                order by total_points desc
                
                
-----------------------------
                
     SELECT 
    des_team,
    season,
    SUM(total_best_points) AS total_points_across_rounds
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
        dr.season = 2023 
        AND dr.racing_class = 'motogp'
        --AND dt.des_team LIKE '%Lenovo%'
        --AND dr.rider_full_name LIKE '%Pirro%'
        and fr.num_round = ANY (dr.rounds_participated)
        AND fr.is_wildcard = false
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
   order by total_points_across_rounds desc;