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





