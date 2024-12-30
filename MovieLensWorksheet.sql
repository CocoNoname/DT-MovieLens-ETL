CREATE DATABASE IF NOT EXISTS SNAKE_MovieLense;
USE DATABASE SNAKE_MovieLense;

CREATE SCHEMA IF NOT EXISTS PUBLIC;
USE SCHEMA PUBLIC;

USE ROLE TRAINING_ROLE;
USE WAREHOUSE SNAKE_WH;

CREATE OR REPLACE STAGE my_stage;

CREATE TABLE users_staging (
    id INT,
    age INT,
    gender CHAR(1),
    occupation_id INT,
    zip_code VARCHAR(255)
);

CREATE TABLE tags_staging (
    id INT,
    user_id INT,
    movie_id INT,
    tags VARCHAR(4000),
    created_at DATETIME
);

CREATE TABLE ratings_staging (
    id INT,
    user_id INT,
    movie_id INT,
    rating INT,
    rated_at DATETIME
);

CREATE TABLE occupations_staging (
    id INT,
    name VARCHAR(255)
);

CREATE TABLE movies_staging (
    id INT,
    title VARCHAR,
    release_year CHAR(4)
);

CREATE TABLE genres_movies_staging (
    id INT,
    movie_id INT,
    genre_id INT
);

CREATE TABLE genres_staging (
    id INT,
    name VARCHAR(255)
);

CREATE TABLE age_group_staging (
    id INT,
    name VARCHAR(45)
);


COPY INTO age_group_staging
FROM @my_stage/age_group.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);

COPY INTO genres_staging
FROM @my_stage/genres.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);

COPY INTO genres_movies_staging
FROM @my_stage/genres_movies.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);

COPY INTO movies_staging
FROM @my_stage/movies.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);

COPY INTO occupations_staging
FROM @my_stage/occupations.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);

COPY INTO ratings_staging
FROM @my_stage/ratings.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);

COPY INTO tags_staging
FROM @my_stage/tags.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);

COPY INTO users_staging
FROM @my_stage/users.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);


-- Vytvorenie dim_users
CREATE OR REPLACE TABLE dim_users AS
SELECT DISTINCT
    u.id AS usersID,
    u.age,
    CASE 
        WHEN u.age < 18 THEN 'Pod 18'
        WHEN u.age BETWEEN 18 AND 29 THEN '18-29'
        WHEN u.age BETWEEN 30 AND 39 THEN '30-39'
        WHEN u.age BETWEEN 40 AND 49 THEN '40-49'
        WHEN u.age BETWEEN 50 AND 59 THEN '50-59'
        WHEN u.age BETWEEN 60 AND 70 THEN '60-70'
        WHEN u.age >= 71 THEN '70+'
        ELSE 'Neznáme'
    END AS age_group,
    u.gender,
    o.name AS occupation_name
FROM users_staging u
LEFT JOIN occupations_staging o ON u.occupation_id = o.id
ORDER BY u.id;

-- Vytvorenie dim_tags
CREATE OR REPLACE TABLE dim_tags AS 
SELECT DISTINCT
    t.id AS tagsID,
    t.tags,
    t.created_at
FROM tags_staging t;

-- Vytvorenie dim_date
CREATE OR REPLACE TABLE dim_date AS
SELECT
    ROW_NUMBER() OVER (ORDER BY CAST(rated_at AS DATE)) AS dateID, 
    CAST(rated_at AS DATE) AS date,                    
    DATE_PART(day, rated_at) AS day,                   
    DATE_PART(dayofweek, rated_at) + 1 AS day_of_week,    
    CASE DATE_PART(dayofweek, rated_at) + 1
        WHEN 1 THEN 'Pondelok'
        WHEN 2 THEN 'Utorok'
        WHEN 3 THEN 'Streda'
        WHEN 4 THEN 'Štvrtok'
        WHEN 5 THEN 'Piatok'
        WHEN 6 THEN 'Sobota'
        WHEN 7 THEN 'Nedeľa'
    END AS day_of_week_text,
    DATE_PART(week, rated_at) AS week,
    DATE_PART(month, rated_at) AS month,              
    CASE DATE_PART(month, rated_at)
        WHEN 1 THEN 'Január'
        WHEN 2 THEN 'Február'
        WHEN 3 THEN 'Marec'
        WHEN 4 THEN 'Apríl'
        WHEN 5 THEN 'Máj'
        WHEN 6 THEN 'Jún'
        WHEN 7 THEN 'Júl'
        WHEN 8 THEN 'August'
        WHEN 9 THEN 'September'
        WHEN 10 THEN 'Október'
        WHEN 11 THEN 'November'
        WHEN 12 THEN 'December'
    END AS month_text,
    DATE_PART(year, rated_at) AS year
FROM ratings_staging
GROUP BY CAST(rated_at AS DATE), 
         DATE_PART(day, rated_at), 
         DATE_PART(dayofweek, rated_at),
         DATE_PART(week, rated_at),
         DATE_PART(month, rated_at), 
         DATE_PART(year, rated_at); 

-- Vytvorenie dim_genres
CREATE OR REPLACE TABLE dim_genres AS
SELECT DISTINCT
    g.id AS genresID,
    g.name AS genre_name
FROM genres_staging g;

-- Vytvorenie dim_movies
CREATE OR REPLACE TABLE dim_movies AS 
SELECT DISTINCT
    m.id AS moviesID,
    m.title,
    m.release_year
FROM movies_staging m;

-- Vytvorenie fact_ratings
CREATE OR REPLACE TABLE fact_ratings AS
SELECT DISTINCT
    r.id AS ratingID, 
    r.rating, 
    r.rated_at AS timestamp, 
    m.moviesID AS movieID,
    u.usersID AS userID,
    gm.genre_id AS genreID,
    COALESCE(ta.tagsID, NULL) AS tagID,
    d.dateID AS dateID
FROM ratings_staging r
LEFT JOIN dim_movies m ON r.movie_id = m.moviesID 
LEFT JOIN dim_users u ON r.user_id = u.usersID 
LEFT JOIN dim_date d ON CAST(r.rated_at AS DATE) = d.date 
LEFT JOIN genres_movies_staging gm ON r.movie_id = gm.movie_id 
LEFT JOIN dim_tags ta ON ta.tagsID = r.id;

-- Odstránenie Staging tabuliek
DROP TABLE IF EXISTS movies_staging;
DROP TABLE IF EXISTS tags_staging;
DROP TABLE IF EXISTS occupations_staging;
DROP TABLE IF EXISTS ratings_staging;
DROP TABLE IF EXISTS users_staging;
DROP TABLE IF EXISTS genres_staging;
DROP TABLE IF EXISTS genres_movies_staging;
DROP TABLE IF EXISTS age_group_staging;
