Q889 = """SELECT club_name, country, found_year FROM club WHERE found_year = (SELECT 
    MIN(found_year) FROM club) OR found_year = (SELECT MAX(found_year) FROM club);"""

Q889_text = """What are the names, the countries and the years of the first and last club ever founded ?"""

Q889_seed = """Select club_name, country, found_year 
 From club;
"""

Q259_text = """Show all national teams who have won more than 2 world cups"""
Q256 = """SELECT T2.teamname\n
FROM world_cup AS T1\n    JOIN national_team AS T2 ON T1.winner = T2.team_id\nGROUP BY T2.teamname\n HAVING COUNT(t1.year) > 2"""

Q51_text = "Give me all matches where stage is Quarter-finals including the teamnames ordered descending by year\\n",
Q51 = "SELECT m.year, nt.teamname, nt2.teamname, m.home_team_goals, m.away_team_goals\n FROM match AS m\n     JOIN national_team as nt on m.home_team_id = nt.team_id\n     JOIN national_team as nt2 on nt2.team_id = m.away_team_id\n where m.stage = 'Quarter-finals'\n order by m.year desc;",
