-- Top 10 filmov s najvyšším priemerným hodnotením
SELECT 
    m.title AS nazov_filmu,
    ROUND(AVG(f.rating), 2) AS priemerne_hodnotenie
FROM FACT_RATINGS f
JOIN DIM_MOVIES m ON f.movieID = m.moviesID
GROUP BY m.title
ORDER BY priemerne_hodnotenie DESC
LIMIT 10;

-- Najaktívnejší používatelia (Top 10 používateľov podľa počtu hodnotení)
SELECT 
    u.usersID AS pouzivatel_id,
    COUNT(f.ratingID) AS pocet_hodnoteni
FROM FACT_RATINGS f
JOIN DIM_USERS u ON f.userID = u.usersID
GROUP BY u.usersID
ORDER BY pocet_hodnoteni DESC
LIMIT 10;

-- Rozloženie hodnotení podľa dňa v týždni
SELECT 
    d.day_of_week_text AS den,
    COUNT(f.ratingID) AS pocet_hodnoteni
FROM FACT_RATINGS f
JOIN DIM_DATE d ON f.dateID = d.dateID
GROUP BY d.day_of_week_text
ORDER BY pocet_hodnoteni DESC;

-- Počet hodnotení pre hodiny dňa
SELECT 
    DATE_PART(hour, f.timestamp) AS hodina,
    COUNT(f.ratingID) AS pocet_hodnoteni
FROM FACT_RATINGS f
GROUP BY DATE_PART(hour, f.timestamp)
ORDER BY hodina;


-- filmových žánrov podľa počtu hodnotení
SELECT 
    g.genre_name AS zaner,
    COUNT(f.ratingID) AS pocet_hodnoteni
FROM FACT_RATINGS f
JOIN DIM_GENRES g ON f.genreID = g.genresID
GROUP BY g.genre_name
ORDER BY pocet_hodnoteni DESC
LIMIT 5;

-- Počet hodnotení podľa povolaní
SELECT 
    u.occupation_name AS povolanie,
    COUNT(f.ratingID) AS pocet_hodnoteni
FROM FACT_RATINGS f
JOIN DIM_USERS u ON f.userID = u.usersID
GROUP BY u.occupation_name
ORDER BY pocet_hodnoteni DESC;


-- Počet hodnotení podľa pohlavia
SELECT 
    u.gender AS pohlavie,
    COUNT(f.ratingID) AS pocet_hodnoteni
FROM FACT_RATINGS f
JOIN DIM_USERS u ON f.userID = u.usersID
GROUP BY u.gender;


-- hodnotení podľa vekových kategórií
SELECT 
    u.age_group AS vekova_skupina,
    COUNT(f.ratingID) AS pocet_hodnoteni
FROM FACT_RATINGS f
JOIN DIM_USERS u ON f.userID = u.usersID
GROUP BY u.age_group
ORDER BY pocet_hodnoteni DESC;