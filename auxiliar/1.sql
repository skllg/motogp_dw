           
WITH BestResults AS ( 
    SELECT 
        des_team,
        season,
        SUM(best_points) AS total_best_points
    FROM (
        SELECT 
            dt.des_team,
            fr.season,
            fr.num_round,
            dr.rider_full_name,
            fr.race_type,
            ROW_NUMBER() OVER (PARTITION BY dt.des_team, fr.num_round,fr.race_type ORDER BY dp.num_points DESC) AS rank
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
            AND fr.is_wildcard = false
            GROUP BY
        dt.des_team,
        fr.season,
        fr.num_round,
        dr.rider_full_name,
        fr.race_type,
        dp.num_points
    ) AS ranked_points
    
)
SELECT
    distinct
    dt.des_team,
    br.num_round,
    SUM(br.total_best_points) OVER (PARTITION BY br.num_round ORDER BY br.num_round asc) AS cumulative_points
FROM
    dim_teams dt
JOIN BestResults br ON dt.des_team = br.des_team AND dt.season = br.season
ORDER BY 
    br.num_round, 
    cumulative_points DESC;