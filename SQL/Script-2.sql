select dt.des_team ,* from dim_riders dr
left join dim_teams dt on dt.id_team = dr.id_team_fk 
where dr.rider_full_name like '%Pirro%' and dr.season=2023


select dt.des_team, dr.rider_full_name  from fact_results fr left join dim_riders dr on dr.id_rider =fr.id_rider_fk 
left join dim_teams dt on dt.id_team = dr.id_team_fk
where num_round=3 and dr.season=2023 and dt.des_team like '%Aprilia%'

select distinct dt.id_team , dt.des_team ,dr.rider_full_name, dr.rounds_string  from dim_riders dr
left join dim_teams dt on dt.id_team = dr.id_team_fk 
where dt.des_team like '%Lenovo%' and dr.season=2023


select distinct dt.des_team ,dr.rider_full_name, dr.rounds_string  from dim_riders dr
left join dim_teams dt on dt.id_team = dr.id_team_fk 
where dr.rider_full_name like '%Pirro%' and dr.season=2023


select * from dim_riders dr 

UPDATE fact_results 
SET id_rider_fk = 888

truncate fact_results

select dr.rider_full_name,fr.id_rider_fk,dt.des_team  ,* from fact_results fr
left join dim_riders dr on dr.id_rider = fr.id_rider_fk 
left join dim_teams dt on dt.id_team = dr.id_team_fk 
--where id_rider_fk <> 1
 where dr.rider_full_name = 'Michele Pirro'
 
 
UPDATE fact_results fr
SET id_rider_fk =  (select id_rider from dim_riders dr where dr.season=fr.season and dr.rider_full_name = fr.rider_name_fk 
and dr.racing_class= fr.racing_class 
and rounds_participated @> ARRAY[fr.num_round])

select distinct id_result,id_rider, rider_full_name, rounds_string, fr.is_wildcard  from dim_riders dr
left join fact_results fr on fr.id_rider_fk = dr.id_rider 
where fr.season=2023 and rider_full_name = 'Lorenzo Savadori' and fr.racing_class= 'motogp' 




