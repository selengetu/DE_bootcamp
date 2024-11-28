-- select
--  game_id, team_id, player_id, COUNT(1)
-- from game_details
-- group by game_id, team_id, player_id
-- HAVING COUNT(1)>1


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
select
    game_date_est,
    season,
    team_id,
    team_id = home_team_id AS dim_is_playing_at_home,
    player_id,
    player_name


from deduped
WHERE row_num = 1



