select
 game_id, team_id, player_id, COUNT(1)
from game_details
group by game_id, team_id, player_id
HAVING COUNT(1)>1