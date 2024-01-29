select
    team_id,
    team_name,
    team_market,
    conference_id,
    division_id
 from {{source('nba-analytics-412517', 'team_rankings')}}