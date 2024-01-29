select
    distinct division_id, division_name, conference_id
from {{source('nba-analytics-412517', 'team_rankings')}}