# Week 4 Applying Analytical Patterns
The homework this week will be using the `players`, `players_scd`, and `player_seasons` tables from week 1

- A query that does state change tracking for `players`
  - A player entering the league should be `New`
  - A player leaving the league should be `Retired`
  - A player staying in the league should be `Continued Playing`
  - A player that comes out of retirement should be `Returned from Retirement`
  - A player that stays out of the league should be `Stayed Retired`

```
CREATE TABLE player_season_accounting (
  player_name TEXT,
  first_active_season INTEGER,
  last_active_season INTEGER,
  state TEXT,
  sseason TEXT
);

INSERT INTO player_season_accounting
WITH yesterday AS (
    SELECT * FROM player_season_accounting
    WHERE year = 1995
),
     today AS (
         SELECT
            player_name,
            season
         FROM player_seasons
         WHERE season=1996
         AND player_name IS NOT NULL
     )

         SELECT COALESCE(t.player_name, y.player_name)          AS player_name,
                COALESCE(y.first_active_season, t.season)       AS first_active_season,
                COALESCE(t.season, y.last_active_season)        AS last_active_season,
                CASE
                    WHEN y.player_name IS NULL THEN 'New'
                    WHEN t.player_name IS NULL THEN 'Retired'
                    WHEN y.last_active_season = t.season - 1 THEN 'Continued Playing'
                    WHEN y.last_active_season < t.season - 1 THEN 'Returned from Retirement'
                    ELSE 'Stayed Retired'
                END           AS state,
                COALESCE(t.season, y.season + 1) AS season
         FROM today t
                  FULL OUTER JOIN yesterday y
                                  ON t.player_name = y.player_name;
```

- A query that uses `GROUPING SETS` to do efficient aggregations of `game_details` data
  - Aggregate this dataset along the following dimensions
    - player and team
      - Answer questions like who scored the most points playing for one team?
    - player and season
      - Answer questions like who scored the most points in one season?
    - team
      - Answer questions like which team has won the most games?

```
CREATE TABLE game_stats AS
WITH games_augmented AS (
  SELECT
    player_name,
    team_abbreviation,
    g.season,
    COALESCE(pts, 0) AS pts,
    CASE
      WHEN gd.team_id = g.home_team_id AND home_team_wins = 1 THEN gd.game_id
      WHEN gd.team_id = g.visitor_team_id AND home_team_wins = 0 THEN gd.game_id
    END AS game_id_won
  FROM game_details gd
  JOIN games g
    ON gd.game_id = g.game_id
  WHERE g.season IS NOT NULL
)
SELECT
  COALESCE(player_name, '(overall)') AS player_name,
  COALESCE(team_abbreviation, '(overall)') AS team_abbreviation,
  COALESCE(CAST(season AS TEXT), '(overall)') AS season,
  SUM(pts) AS total_points,
  COUNT(DISTINCT game_id_won) AS total_wins
FROM games_augmented
GROUP BY GROUPING SETS (
  (player_name, team_abbreviation),
  (player_name, season),
  (team_abbreviation)
)
ORDER BY total_points DESC;

SELECT *
FROM game_stats
WHERE player_name <> '(overall)'
AND team_abbreviation <> '(overall)'
ORDER BY total_points DESC
LIMIT 1;
-[ RECORD 1 ]-----+----------------------
player_name       | Giannis Antetokounmpo
team_abbreviation | MIL
season            | (overall)
total_points      | 15591
total_wins        | 369

SELECT *
FROM game_stats
WHERE player_name <> '(overall)'
AND season <> '(overall)'
ORDER BY total_points DESC
LIMIT 1;
-[ RECORD 1 ]-----+-------------
player_name       | James Harden
team_abbreviation | (overall)
season            | 2018
total_points      | 3247
total_wins        | 60

SELECT *
FROM game_stats
WHERE player_name = '(overall)'
AND season = '(overall)'
ORDER BY total_wins DESC
LIMIT 1;
-[ RECORD 1 ]-----+----------
player_name       | (overall)
team_abbreviation | GSW
season            | (overall)
total_points      | 78427
total_wins        | 445
```

- A query that uses window functions on `game_details` to find out the following things:
  - What is the most games a team has won in a 90 game stretch?
  - How many games in a row did LeBron James score over 10 points a game?
```
WITH games_augmented AS (
  SELECT
    gd.game_id,
    player_name,
    team_abbreviation,
    COALESCE(pts, 0) AS pts,
    CASE
      WHEN gd.team_id = g.home_team_id AND home_team_wins = 1 THEN 1
      WHEN gd.team_id = g.visitor_team_id AND home_team_wins = 0 THEN 1
      ELSE 0
    END AS win
  FROM game_details gd
  JOIN games g
    ON gd.game_id = g.game_id
  WHERE g.season IS NOT NULL
),
games_deduped AS (
  SELECT DISTINCT game_id, team_abbreviation, win
  FROM games_augmented
)
SELECT SUM(win) OVER (PARTITION BY team_abbreviation ORDER BY game_id ASC ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) AS win_cnt
FROM games_deduped
ORDER BY 1 DESC
LIMIT 1;
-[ RECORD 1 ]
win_cnt | 78

WITH games_augmented AS (
  SELECT
    gd.game_id,
    player_name,
    team_abbreviation,
    COALESCE(pts, 0) AS pts,
    CASE
      WHEN gd.team_id = g.home_team_id AND home_team_wins = 1 THEN 1
      WHEN gd.team_id = g.visitor_team_id AND home_team_wins = 0 THEN 1
      ELSE 0
    END AS win
  FROM game_details gd
  JOIN games g
    ON gd.game_id = g.game_id
  WHERE g.season IS NOT NULL
),
lebron_games AS (
  SELECT ROW_NUMBER() OVER (ORDER BY game_id ASC) AS game_num, pts
  FROM games_augmented
  WHERE player_name='LeBron James'
),
streaks AS (
  SELECT
    *,
    game_num - ROW_NUMBER() OVER (PARTITION BY (pts > 10) ORDER BY game_num) AS streak_group
  FROM lebron_games
)
SELECT
  streak_group,
  COUNT(*) AS consecutive_games
FROM streaks
WHERE pts > 10
GROUP BY streak_group
ORDER BY consecutive_games DESC
LIMIT 1;
 streak_group | consecutive_games
--------------+-------------------
           34 |               163
(1 row)
```

Please add these queries into a folder `homework/<discord-username>`
