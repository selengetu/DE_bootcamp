WITH deduped AS(
    select g.game_date_est,
           g.season,
           g.home_team_id,
           g.visitor_team_id,
           gd.*,
        row_number() over (partition by gd.game_id, team_id, player_id ORDER BY g.game_date_est) as row_num
    from game_details gd JOIN games g
    ON gd.game_id = g.game_id

)
-- Now insert the deduplicated rows into the target table or perform other operations as needed
SELECT *
FROM deduped
WHERE row_num = 1;

