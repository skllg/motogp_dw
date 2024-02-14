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
        --AND dt.des_team LIKE '%Aprilia%'
        --AND fr.is_wildcard = false
    GROUP BY
        dt.des_team,
        fr.season,
        fr.num_round,
        dr.rider_full_name,
        fr.race_type,
        dp.num_points
) AS ranked_points
WHERE 
						    rank <= 2

GROUP BY 
    des_team, 
    season
   order by total_points_across_rounds desc;
