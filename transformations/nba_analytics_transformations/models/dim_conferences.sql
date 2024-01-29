select
    distinct conference_id, conference_name
from {{source('nba-analytics-412517', 'team_rankings')}}