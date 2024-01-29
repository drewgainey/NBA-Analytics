select
    id as player_id,
    team_id,
    full_name,
    position,
    jersey_number
from {{source('nba-analytics-412517', 'player_statistics')}}