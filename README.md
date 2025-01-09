# **ETL proces datasetu MovieLens**

Tento repozitár obsahuje implementáciu ETL procesu v Snowflake pre analýzu dát z **MovieLens** datasetu. Projekt sa zameriava na preskúmanie recenzií používateľov a ich filmových preferencií na základe hodnotení filmov a demografických údajov používateľov. Výsledný dátový model umožňuje multidimenzionálnu analýzu a vizualizáciu kľúčových metrik.

---
## **1. Úvod a popis zdrojových dát**
Hlavným cieľom tohto projektu je analyzovať údaje o filmoch, používateľoch a ich hodnoteniach. Táto analýza pomáha odhaliť trendy vo filmových preferenciách, najobľúbenejšie filmy, správanie rôznych skupín používateľov a viac.

Zdrojové dáta pochádzajú z GroupLens datasetu dostupného [na tejto stránke](https://grouplens.org/datasets/movielens/).

---
Dataset obsahuje osem hlavných tabuliek:

  - `age_group` - Kategórie vekových skupín používateľov.
  - `genres` - Žánre filmov.
  - `genres_movies` - Prepojenie medzi filmami a ich žánrami.
  - `movies` - Informácie o filmoch.
  - `occupations` - Zoznam povolaní používateľov.
  - `ratings` - Hodnotenia filmov používateľmi.
  - `tags` - Zlepšenie filtrácie/vyhladávanie
  - `users` - Informácie o používateľoch.

---
### **1.1 Dátová architektúra**

### **ERD diagram**
  Zobrazuje relačný model surových dát, znázornený na **entitno-relačnom diagrame (ERD)**:

<p align="center">
  <img src=https://github.com/CocoNoname/DT-MovieLens-ETL/blob/main/ERD_Schema.png alt="ERD Schema">
  <br>
  <em>Entitno-relačná schéma pre MovieLens</em>
</p>

---
## **2 Dimenzionálny model**

Pre projekt bol navrhnutý multi-dimenzionálny model typu hviezda. Tento model umožňuje efektívnu analýzu hodnotení filmov používateľmi. Model obsahuje faktovú tabuľku **`fact_ratings`** a niekoľko dimenzionálnych tabuliek:

- **`dim_users`**: Demografické údaje o používateľoch vrátane vekových kategórií, povolaní a pohlaví.
- **`dim_tags`**: Podrobné informácie o tagoch ako popisy a dátumy vytvorenia.
- **`dim_date`**: Informácie o dátumoch hodnotení vrátane dňa, mesiaca a roku.
- **`dim_genres`**: Detailné údaje o žánroch filmov.
- **`dim_movies`**: Detailné informácie o filmoch vrátane názvov a rokov vydania.

- **`fact_ratings`**: Obsahuje kľúčové metriky a prepojenie na dimenzie.
---

Hviezdicový model zobrazuje jasné vzťahy medzi dimenziami a faktovou tabuľkou:
<p align="center">
  <img src=https://github.com/CocoNoname/DT-MovieLens-ETL/blob/main/HviezdaSchema_MovieLens.png alt="Hviezdicova Schéma">
  <br>
  <em> Schéma hviezdy pre MovieLens</em>
</p>


---
## **3. ETL proces v Snowflake**
Účelom ETL procesu je dáta pripraviť, transformovať a sprístupniť pre viacdimenzionálnu analýzu.


### **3.1 Extract (Extrahovanie dát)**

#### Vytvorenie my_stage:
```sql
CREATE OR REPLACE STAGE my_stage;
```
#### Naimportovanie dát:
```sql
COPY INTO age_group_staging
FROM @my_stage/age_group.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);
```

---
### **3.2 Transform (Transformácia dát)**
V tejto fáze boli dáta zo staging tabuliek vyčistené, transformované a obohatené.

1. **Dimenzia `dim_users`:**
- Poskytuje informácie o veku, pohlaví a zamestnaní používateľov, pričom umožňuje sledovanie historických zmien.

- `dim_users`: Rozdelenie veku používateľov do kategórií:
```sql
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
```
---
2. **Dimenzia `dim_tags`:**

- `dim_tags`: Údaje o značkách:
```sql
CREATE OR REPLACE TABLE dim_tags AS 
SELECT DISTINCT
    t.id AS tagsID,
    t.tags,
    t.created_at
FROM tags_staging t;
```
---
3. **Dimenzia `dim_date`:**
   
- Dimenzia `dim_date` je štruktúrovaná tak, aby umožňovala podrobné časové analýzy, ako sú trendy hodnotení podľa dní, mesiacov alebo rokov. Z hľadiska SCD je táto dimenzia klasifikovaná ako SCD Typ 0. To znamená, že existujúce záznamy v tejto dimenzii sú nemenné a uchovávajú statické informácie.

- `dim_date`: Časová analýza dát:
```sql
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
```
---
4. **Dimenzia `dim_genres`:**

- `dim_genres`: Časová analýza dát:
```sql
CREATE OR REPLACE TABLE dim_genres AS
SELECT DISTINCT
    g.id AS genresID,
    g.name AS genre_name
FROM genres_staging g;
```
---
5. **Dimenzia `dim_movies`:**

-Podobne `dim_movies` obsahuje údaje o filmoch, ako sú názov a rok vydania . Táto dimenzia je typu SCD Typ 0, pretože údaje o filoch sú považované za nemenné, napríklad názov filmu alebo rok vydania sa nemenia. 

- `dim_movies`: Transformácia filmových údajov:
```sql
CREATE OR REPLACE TABLE dim_movies AS 
SELECT DISTINCT
    m.id AS moviesID,
    m.title,
    m.release_year
FROM movies_staging m;
```
---
6. **Dimenzia `fact_ratings`:**

-Faktová tabuľka `fact_ratings` obsahuje záznamy o hodnoteniach a prepojenia na všetky dimenzie. Obsahuje kľúčové metriky, ako je hodnota hodnotenia a časový údaj.

- `fact_ratings`: Kombinácia hlavných metrík:
```sql
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
```

---
### **3.3 Load (Načítanie dát)**

Na záver po úspešnom nahratí údajov boli staging tabuľky odstránené, aby sa optimalizovalo využitie úložiska:
```sql
DROP TABLE IF EXISTS movies_staging;
DROP TABLE IF EXISTS tags_staging;
DROP TABLE IF EXISTS occupations_staging;
DROP TABLE IF EXISTS ratings_staging;
DROP TABLE IF EXISTS users_staging;
DROP TABLE IF EXISTS genres_staging;
DROP TABLE IF EXISTS genres_movies_staging;
DROP TABLE IF EXISTS age_group_staging;
```

---
## **4 Vizualizácia dát**

Dashboard poskytuje prehľad prostredníctvom `8 vizualizácií`, ktoré ilustrujú kľúčové metriky a trendy vo filmoch, používateľoch a ich hodnoteniach. Tieto vizualizácie odpovedajú na zásadné otázky

<p align="center">
  <img src="https://github.com/CocoNoname/DT-MovieLens-ETL/blob/main/MovieLensFiltracie.png" alt="Dashboard Filtrácie">
  <br>
  <em> Dashboard MovieLens datasetu</em>
</p>


---
### **Graf 1: Top 10 filmov s najvyšším priemerným hodnotením**
Táto vizualizácia zobrazuje desať filmov s najvyšším priemerným skóre hodnotenia. Pomáha identifikovať najlepšie hodnotené tituly podľa používateľských recenzií. Tieto údaje môžu byť cenné pri odporúčaní filmov alebo plánovaní marketingových stratégií.

```sql
SELECT 
    m.title AS nazov_filmu,
    ROUND(AVG(f.rating), 2) AS priemerne_hodnotenie
FROM FACT_RATINGS f
JOIN DIM_MOVIES m ON f.movieID = m.moviesID
GROUP BY m.title
ORDER BY priemerne_hodnotenie DESC
LIMIT 10;
```
---
### **Graf 2: Najaktívnejší používatelia (Top 10 používateľov podľa počtu hodnotení)**
Graf znázorňuje 10 najaktívnejších používateľov podľa počtu hodnotení. Ukazuje, ktorí používatelia prispeli najviac hodnoteniami. Tieto informácie môžu byť užitočné na identifikáciu používateľov s vysokou angažovanosťou a na prispôsobenie osobných odporúčaní alebo odmeňovacích programov.

```sql
SELECT 
    u.usersID AS pouzivatel_id,
    COUNT(f.ratingID) AS pocet_hodnoteni
FROM FACT_RATINGS f
JOIN DIM_USERS u ON f.userID = u.usersID
GROUP BY u.usersID
ORDER BY pocet_hodnoteni DESC
LIMIT 10;
```
---
### **Graf 3: Počet hodnotení podľa dňa v týždni**
Táto vizualizácia zobrazuje počet hodnotení pre každý deň v týždni. Pomáha identifikovať, ktoré dni sú najpopulárnejšie pre hodnotenie filmov. Tieto údaje môžu byť užitočné na plánovanie marketingových kampaní alebo analýzu používateľského správania.

```sql
SELECT 
    d.day_of_week_text AS den,
    COUNT(f.ratingID) AS pocet_hodnoteni
FROM FACT_RATINGS f
JOIN DIM_DATE d ON f.dateID = d.dateID
GROUP BY d.day_of_week_text
ORDER BY pocet_hodnoteni DESC;
```
---
### **Graf 4: Celková aktivita v priebehu dňa**
Táto vizualizácia zobrazuje počet hodnotení podľa jednotlivých hodín v priebehu dňa. Pomáha identifikovať, v ktorých hodinách sú používatelia najaktívnejší pri hodnotení filmov. Tieto údaje môžu byť užitočné pri plánovaní marketingových kampaní alebo pri analýze používateľského správania počas rôznych časových období.

```sql
SELECT 
    DATE_PART(hour, f.timestamp) AS hodina,
    COUNT(f.ratingID) AS pocet_hodnoteni
FROM FACT_RATINGS f
GROUP BY DATE_PART(hour, f.timestamp)
ORDER BY hodina;
```
---
### **Graf 5: Najviac hodnotené žánre (Top 5)**
Táto vizualizácia poskytuje prehľad o top 5 filmových žánroch podľa počtu hodnotení. Umožňuje analyzovať, ktoré žánre sú medzi používateľmi najobľúbenejšie. Tieto informácie môžu byť užitočné na odporúčanie filmov podľa obľúbených žánrov alebo na cielené marketingové kampane.

```sql
SELECT 
    g.genre_name AS zaner,
    COUNT(f.ratingID) AS pocet_hodnoteni
FROM FACT_RATINGS f
JOIN DIM_GENRES g ON f.genreID = g.genresID
GROUP BY g.genre_name
ORDER BY pocet_hodnoteni DESC
LIMIT 5;
```
---
### **Graf 6: Počet hodnotení podľa povolaní používateľov**
Graf poskytuje prehľad o počte hodnotení podľa rôznych povolaní používateľov. Ukazuje, ktoré profesijné skupiny sú najaktívnejšie pri hodnotení filmov. Tieto informácie môžu byť použité na cielenie personalizovaných odporúčaní a analýzu používateľských preferencií podľa ich povolaní.

```sql
SELECT 
    u.occupation_name AS povolanie,
    COUNT(f.ratingID) AS pocet_hodnoteni
FROM FACT_RATINGS f
JOIN DIM_USERS u ON f.userID = u.usersID
GROUP BY u.occupation_name
ORDER BY pocet_hodnoteni DESC;
```
---
### **Graf 7: Počet hodnotení podľa pohlavia používateľov**
Táto vizualizácia zobrazuje počet hodnotení podľa pohlavia používateľov. Umožňuje identifikovať, aké sú hodnotenia medzi mužmi a ženami. Tieto údaje môžu byť užitočné pri analýze demografických trendov a pri tvorbe personalizovaných odporúčaní pre jednotlivé pohlavia.

```sql
SELECT 
    u.gender AS pohlavie,
    COUNT(f.ratingID) AS pocet_hodnoteni
FROM FACT_RATINGS f
JOIN DIM_USERS u ON f.userID = u.usersID
GROUP BY u.gender;

```
---
### **Graf 8: Počet hodnotení podľa vekových skupín používateľov**
Táto vizualizácia zobrazuje počet hodnotení podľa rôznych vekových skupín používateľov. Umožňuje analyzovať, ktoré vekové skupiny sú najaktívnejšie pri hodnotení filmov. Tieto informácie môžu byť užitočné pri tvorbe personalizovaných odporúčaní a plánovaní marketingových kampaní zameraných na konkrétne vekové skupiny.
```sql
SELECT 
    u.age_group AS vekova_skupina,
    COUNT(f.ratingID) AS pocet_hodnoteni
FROM FACT_RATINGS f
JOIN DIM_USERS u ON f.userID = u.usersID
GROUP BY u.age_group
ORDER BY pocet_hodnoteni DESC;
```

Dashboard ponúka komplexný prehľad dát a odpovedá na zásadné otázky týkajúce sa filmových preferencií a správania používateľov. Vizualizácie uľahčujú interpretáciu dát a môžu byť využité na zlepšenie odporúčacích systémov, marketingových stratégií a filmových služieb.

---


**Autor:** Erik Csicsó
