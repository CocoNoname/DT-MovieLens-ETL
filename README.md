# **ETL proces datasetu MovieLens**

Tento repozitár obsahuje implementáciu ETL procesu v Snowflake pre analýzu dát z **MovieLens** datasetu. Projekt sa zameriava na preskúmanie recenzií používateľov a ich filmových preferencií na základe hodnotení filmov a demografických údajov používateľov. Výsledný dátový model umožňuje multidimenzionálnu analýzu a vizualizáciu kľúčových metrik.

---
## **1. Úvod a popis zdrojových dát**
Cieľom semestrálneho projektu je analyzovať dáta týkajúce sa filmov, používateľov a ich recenzií. Táto analýza umožňuje identifikovať trendy vo filmových preferenciách, najpopulárnejšie filmy a správanie používateľov.

Zdrojové dáta pochádzajú z GroupLens datasetu dostupného [tu](https://grouplens.org/datasets/movielens/). Dataset obsahuje osem hlavných tabuliek:
- `age_group`
- `genres`
- `genres_movies`
- `movies`
- `occupations`
- `ratings`
- `tags`
- `users`

Účelom ETL procesu bolo tieto dáta pripraviť, transformovať a sprístupniť pre viacdimenzionálnu analýzu.

---
### **1.1 Dátová architektúra**

### **ERD diagram**
Surové dáta sú usporiadané v relačnom modeli, ktorý je znázornený na **entitno-relačnom diagrame (ERD)**:

<p align="center">
  <img src=https://github.com/CocoNoname/DT-MovieLens-ETL/blob/main/ERD_Schema.png alt="ERD Schema">
  <br>
  <em>Obrázok 1 Entitno-relačná schéma MovieLens</em>
</p>

---
## **2 Dimenzionálny model**

Pri návrhu sme využili **hviezdicový model (star schema)**, ktorý nám umožňuje efektívnu analýzu dát. V centre tohto modelu je faktová tabuľka **`fact_ratings`**, ktorá je spojená s nasledujúcimi dimenziami:
- **`dim_users`**: Demografické údaje o používateľoch vrátane vekových kategórií, povolaní a pohlaví.
- **`dim_tags`**: Podrobné informácie o tagoch ako popisy a dátumy vytvorenia.
- **`dim_date`**: Informácie o dátumoch hodnotení vrátane dňa, mesiaca a roku.
- **`dim_genres`**: Detailné údaje o žánroch filmov.
- **`dim_movies`**: Detailné informácie o filmoch vrátane názvov a rokov vydania.

Štruktúra hviezdicového modelu je znázornená na diagrame nižšie. Diagram ukazuje prepojenia medzi faktovou tabuľkou a dimenziami, čo zjednodušuje pochopenie a implementáciu modelu.

<p align="center">
  <img src=https://github.com/CocoNoname/DT-MovieLens-ETL/blob/main/HviezdaSchema_MovieLens.png alt="Hviezdicova Schéma">
  <br>
  <em>Obrázok 2 Schéma hviezdy pre MovieLens</em>
</p>
