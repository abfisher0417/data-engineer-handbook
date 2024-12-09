# Dimensional Data Modeling - Week 1

This week's assignment involves working with the `actor_films` dataset. Your task is to construct a series of SQL queries and table definitions that will allow us to model the actor_films dataset in a way that facilitates efficient analysis. This involves creating new tables, defining data types, and writing queries to populate these tables with data from the actor_films dataset

## Dataset Overview
The `actor_films` dataset contains the following fields:

- `actor`: The name of the actor.
- `actorid`: A unique identifier for each actor.
- `film`: The name of the film.
- `year`: The year the film was released.
- `votes`: The number of votes the film received.
- `rating`: The rating of the film.
- `filmid`: A unique identifier for each film.

The primary key for this dataset is (`actor_id`, `film_id`).

## Assignment Tasks

1. **DDL for `actors` table:** Create a DDL for an `actors` table with the following fields:
    - `films`: An array of `struct` with the following fields:
		- film: The name of the film.
		- votes: The number of votes the film received.
		- rating: The rating of the film.
		- filmid: A unique identifier for each film.

    - `quality_class`: This field represents an actor's performance quality, determined by the average rating of movies of their most recent year. It's categorized as follows:
		- `star`: Average rating > 8.
		- `good`: Average rating > 7 and ≤ 8.
		- `average`: Average rating > 6 and ≤ 7.
		- `bad`: Average rating ≤ 6.
    - `is_active`: A BOOLEAN field that indicates whether an actor is currently active in the film industry (i.e., making films this year).
```
CREATE TYPE film_struct AS (
	film text,
	votes integer,
	rating real,
	filmid text
);

CREATE TYPE quality_enum AS ENUM (
	'star', 'good', 'average', 'bad'
);

CREATE TABLE actors (
	actor text,
	actorid text,
	films film_struct[],
	quality_class quality_enum,
	is_active boolean,
	year integer,
	PRIMARY KEY (actorid, year)
);
```
2. **Cumulative table generation query:** Write a query that populates the `actors` table one year at a time.
```
WITH
previous AS (
	SELECT *
	FROM actors
	WHERE year='1969' -- CHANGEME
),
current AS (
	SELECT
		actor,
		actorid,
		year,
		AVG(rating) AS avg_rating,
		ARRAY_AGG(
			ROW(
				film,
				votes,
				rating,
				filmid
			)::film_struct
		) AS films
	FROM actor_films
	WHERE year='1970' -- CHANGEME
	GROUP BY 1, 2, 3
)
INSERT INTO actors
SELECT
	COALESCE(l.actor, t.actor) AS actor,
	COALESCE(l.actorid, t.actorid) AS actorid,
	COALESCE(l.films, ARRAY[]::film_struct[]) ||
		CASE WHEN t.films IS NOT NULL THEN t.films
		ELSE ARRAY[]::film_struct[] END AS films,
	CASE WHEN t.avg_rating IS NOT NULL THEN
		(CASE
			WHEN t.avg_rating > 8 THEN 'star'
			WHEN t.avg_rating > 7 THEN 'good'
			WHEN t.avg_rating > 6 THEN 'average'
			ELSE 'bad'
		END)::quality_enum
	ELSE l.quality_class END AS quality_class,
	t.year IS NOT NULL AS is_active,
	'1970' AS year -- CHANGEME
FROM previous l
FULL OUTER JOIN current t
	ON l.actorid = t.actorid;
```
3. **DDL for `actors_history_scd` table:** Create a DDL for an `actors_history_scd` table with the following features:
    - Implements type 2 dimension modeling (i.e., includes `start_date` and `end_date` fields).
    - Tracks `quality_class` and `is_active` status for each actor in the `actors` table.
```
CREATE TABLE actors_history_scd (
	actor text,
	actorid text,
	start_year integer,
	end_year integer,
	quality_class quality_enum,
	is_active boolean,
	PRIMARY KEY(actorid, start_year)
);
```
4. **Backfill query for `actors_history_scd`:** Write a "backfill" query that can populate the entire `actors_history_scd` table in a single query.
```
INSERT INTO actors_history_scd
WITH actors_with_lag AS (
	SELECT
		actor,
		actorid,
		year,
		LEAD(year, 1, '9999') OVER (PARTITION BY actorid ORDER BY year ASC) AS next_year,
		LAG(quality_class, 1) OVER (PARTITION BY actorid ORDER BY year ASC) AS previous_quality_class,
		quality_class,
		LAG(is_active) OVER (PARTITION BY actorid ORDER BY year ASC) AS previous_is_active,
		is_active
	FROM actors
),
with_change_indicator AS (
	SELECT *,
	CASE
		WHEN quality_class <> previous_quality_class THEN 1
		WHEN is_active <> previous_is_active THEN 1
		ELSE 0
	END AS change_indicator
	FROM actors_with_lag
),
with_streaks AS (
	SELECT *,
	SUM(change_indicator) OVER (PARTITION BY actorid ORDER BY year) AS streak_identifier
	FROM with_change_indicator
)
SELECT
	actor,
	actorid,
	MIN(year) AS start_year,
	MAX(next_year) AS end_year,
	quality_class,
	is_active
FROM with_streaks
GROUP BY actor, actorid, streak_identifier, quality_class, is_active;
```

5. **Incremental query for `actors_history_scd`:** Write an "incremental" query that combines the previous year's SCD data with new incoming data from the `actors` table.

```
-- The following query results can be MERGE'd into the `actors_history_scd` table. To ensure idempotency,
-- If the MERGE completes for the current year and needs to be re-run, we should restore the table to
-- the snapshot version prior to running MERGE before re-running. WHY? We close open-ended intervals if
-- there's a change.
CREATE TYPE scd_type AS (
	start_year integer,
	end_year integer,
	quality_class quality_enum,
	is_active boolean
);

WITH latest_scd_data AS (
    SELECT *
	FROM actors_history_scd
    WHERE end_year = '9999' -- We are using '9999' instead of NULL to represent the latest scd data
),
this_years_actors AS (
	SELECT
		actor,
		actorid,
		year,
		quality_class,
		is_active
	FROM actors
	WHERE year = '2024' -- CHANGEME
),
unchanged_records AS (
	SELECT
		t.actor,
		t.actorid,
		l.start_year,
		l.end_year,
		l.quality_class,
		l.is_active
	FROM this_years_actors t
	JOIN latest_scd_data l
		ON l.actorid = t.actorid
	WHERE
		t.quality_class = l.quality_class
		AND t.is_active = l.is_active
),
changed_records AS (
	SELECT
		t.actor,
		t.actorid,
		UNNEST(ARRAY[
			ROW(
				l.start_year,
				t.year,
				l.quality_class,
				l.is_active
				)::scd_type,
			ROW(
				t.year,
				9999, -- Open ended since this is the latest
				t.quality_class,
				t.is_active
				)::scd_type
		]) AS records
	FROM this_years_actors t
	LEFT JOIN latest_scd_data l
		ON l.actorid = t.actorid
	WHERE
		(t.quality_class <> l.quality_class
		OR t.is_active <> l.is_active)
),
unnested_changed_records AS (
	SELECT
		actor,
		actorid,
		(records::scd_type).start_year,
		(records::scd_type).end_year,
		(records::scd_type).quality_class,
		(records::scd_type).is_active
	FROM changed_records
),
new_records AS (
	SELECT
		t.actor,
		t.actorid,
		t.year AS start_year,
		9999 AS end_year,
		t.quality_class,
		t.is_active
	FROM this_years_actors t
	LEFT JOIN latest_scd_data l
		ON t.actorid = l.actorid
	WHERE l.actorid IS NULL
)
SELECT * FROM (
	SELECT *
	FROM unchanged_records

	UNION ALL

	SELECT *
	FROM unnested_changed_records

	UNION ALL

	SELECT *
	FROM new_records
) a;
```
