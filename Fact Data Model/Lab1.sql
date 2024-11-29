-- select
--  game_id, team_id, player_id, COUNT(1)
-- from game_details
-- group by game_id, team_id, player_id
-- HAVING COUNT(1)>1

INSERT INTO fct_game_details
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
    game_date_est as dim_game_date,
    season as dim_season,
    team_id as dim_team_id,
    player_id as dim_player_id,
    player_name as dim_player_name,
    start_position as dim_start_position,
    team_id = home_team_id AS dim_is_playing_at_home,
    coalesce(POSITION('DNP' in comment),0) > 0 as dim_did_not_play,
    coalesce(POSITION('DND' in comment),0) > 0 as dim_did_not_dress,
    coalesce(POSITION('NWT' in comment),0) > 0 as dim_did_not_with_team,
    CAST(split_part(min,':', 1) as REAL) +  CAST(split_part(min,':', 2) as REAL)/60 as m_minutes,
    fgm AS m_fgm,
    fga AS m_fga,
    fg3m AS m_fg3m,
    fg3a AS m_fg3a,
    ftm AS m_ftm,
    fta AS m_fta,
    oreb AS m_oreb,
    dreb AS m_dreb,
    reb AS m_reb,
    ast AS m_ast,
    stl AS m_stl,
    blk AS m_blk,
    "TO" as m_turnovers,
    pf as m_pf,
    pts as m_pts,
    plus_minus as m_plus_minus
from deduped
WHERE row_num = 1;

-- CREATE TABLE fct_game_details (
--     dim_game_date Date,
--     dim_season INTEGER,
--     dim_team_id INTEGER,
--     dim_player_id INTEGER,
--     dim_player_name TEXT,
--     dim_start_position TEXT,
--     dim_is_playing_at_home BOOLEAN,
--     dim_did_not_play BOOLEAN,
--     dim_did_not_dress BOOLEAN,
--     dim_not_with_team BOOLEAN,
--     m_minutes REAL,
--     m_fgm INTEGER,
--     m_fga INTEGER,
--     m_fg3m INTEGER,
--     m_fg3a INTEGER,
--     m_ftm INTEGER,
--     m_fta INTEGER,
--     m_oreb INTEGER,
--     m_dreb INTEGER,
--     m_reb INTEGER,
--     m_ast INTEGER,
--     m_stl INTEGER,
--     m_blk INTEGER,
--     m_turnovers INTEGER,
--     m_pf INTEGER,
--     m_pts INTEGER,
--     m_plus_minus INTEGER,
--     PRIMARY KEY (dim_game_date, dim_team_id, dim_player_id)
-- )

-- select * from fct_game_details gd
-- JOIN teams t
-- On t.team_id = gd.dim_team_id


select dim_player_name,
       count(1) as num_games,
       count(case when dim_not_with_team then 1 end) as bailed_num,
       CAST(count(case when dim_not_with_team then 1 end) AS REAL)/count(1) AS bailed_percent
from

             fct_game_details gd
group by 1
order by 4 desc;