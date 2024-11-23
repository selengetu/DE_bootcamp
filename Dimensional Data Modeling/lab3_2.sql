INSERT INTO vertices
select game_id as identifier,
'game'::vertex_type as type,
json_build_object(
    'pts_home', pts_home,
    'pts_away', pts_away,
    'winning_team', CASE WHEN home_team_id = 1 THEN home_team_id else visitor_team_id END
    ) as proporties
from games;


INSERT INTO vertices
WITH players_agg AS(
    select
    player_id as identifier,
    max(player_name) as player_name,
    COUNT(1) as number_of_games,
    SUM(pts) as total_points,
    ARRAY_AGG(DISTINCT team_id) as teams
from game_details
GROUP BY player_id
)
Select identifier, 'player'::vertex_type,
       json_build_object(
       'player_name', player_name,
       'number_of_games', number_of_games,
       'total_points', total_points,
       'teams', teams
       )
FROM players_agg;

INSERT INTO vertices
WITH teams_deduped AS (
select *, row_number() over (PARTITION BY teams.team_id) as row_number
         from teams
)
SELECT
    team_id as identifier,
    'team'::vertex_type AS type,
    json_build_object(
    'abbreviation', abbreviation,
    'nickname', nickname,
    'city', city,
    'arena', arena,
    'year_founded', yearfounded
    )
FROM teams_deduped
where row_number = 1;

select type, count(1) from vertices
group by 1