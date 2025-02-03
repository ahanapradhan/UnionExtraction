```sql
SELECT m.match_id, m.year, nt1.teamname AS home_team, nt2.teamname AS away_team
FROM match m
JOIN national_team nt1 ON m.home_team_id = nt1.team_id
JOIN national_team nt2 ON m.away_team_id = nt2.team_id
WHERE m.stage = 'Quarter-finals'
ORDER BY m.year DESC;
```
