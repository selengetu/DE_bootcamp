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

-- INSERT INTO edges
-- select
--
--     player_id AS subject_identifier,
--     'player'::vertex_type as subject_type,
--     game_id as object_identifier,
--     'game'::vertex_type as object_type,
--     'plays_in'::edge_type AS edge_type,
--     json_build_object(
--     'start_position',start_position,
--     'pts', pts,
--     'team_id', team_id,
--     'team_abbreviation', team_abbreviation
--     ) as proporties
-- from deduped
-- where row_number =1
--
--
-- select
-- v.properties->>'player_name',
-- MAX(CAST(e.properties->>'pts' AS INTEGER))
-- from vertices v
--     JOIN edges e
-- ON e.subject_identifier = v.identifier
-- AND e.subject_type = v.type
-- GROUP BY 1
-- ORDER BY 2 DESC

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
group by 1;

INSERT INTO EDGES
WITH deduped AS(
    select *, row_number() over (PARTITION BY player_id, game_id) as row_number
FROM game_details
    ),
    filtered AS (
        select * from deduped
        where row_number =1
    ),
aggregated as(
    select
    f1.player_id as subject_player_id,

    f2.player_id as object_player_id,

    CASE WHEN f1.team_abbreviation = f2.team_abbreviation
    THEN 'shares_team'::edge_type
    ELSE 'plays_against'::edge_type
    END as edge_type,
          MAX(f1.player_name) as subject_player_name,
          MAX(f2.player_name) as object_player_name,
    COUNT(1) as num_games,
    SUM(f1.pts) as subject_points,
    SUM(f2.pts) as object_points
from filtered f1 JOIN filtered f2
ON f1.game_id = f2.game_id AND f1.player_name <> f2.player_name
WHERE f1.player_id>f2.player_id
GROUP BY f1.player_id, f2.player_id, CASE WHEN f1.team_abbreviation = f2.team_abbreviation
    THEN 'shares_team'::edge_type
    ELSE 'plays_against'::edge_type
    END)
select subject_player_id AS subject_identifier,
       'player'::vertex_type AS subject_type,
       object_player_id as object_identifier,
       'player'::vertex_type AS object_type,
       edge_type as edge_type,
       json_build_object(
       'num_games', num_games,
       'subject_points', subject_points,
       'object_points', object_points
       )
from aggregated;

select
    v.properties->>'player_name',
 CAST(v.properties->>'number_of_games' AS REAL),
CASE WHEN CAST(v.properties->>'total_points' AS REAL) = 0 THEN 1
    ELSE CAST(v.properties->>'total_points' AS REAL)
END
FROM vertices v JOIN edges e
on v.identifier = e.subject_identifier
AND v.type = e.subject_type
WHERE e.object_type = 'player'::vertex_type

