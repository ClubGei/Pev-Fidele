-- Script de cr?ation des tables PEV RDC (mod?le Power BI)
-- Dimensions et faits pour la vaccination, cibles, logistique et reporting
-- Page "Cibles": dim_geo (Region, District, AS, FOSA_FPP), dim_year, fact_targets

-- Nettoyer les tables si elles existent (ordre inverse des FK)
DROP TABLE IF EXISTS fact_reporting CASCADE;
DROP TABLE IF EXISTS dim_semaine CASCADE;
DROP TABLE IF EXISTS dim_trimestre CASCADE;
DROP TABLE IF EXISTS dim_semestre CASCADE;
DROP TABLE IF EXISTS dim_month CASCADE;
DROP TABLE IF EXISTS dim_fosa CASCADE;
DROP TABLE IF EXISTS fact_logistique CASCADE;
DROP TABLE IF EXISTS fact_vaccination CASCADE;
DROP TABLE IF EXISTS fact_targets CASCADE;
DROP TABLE IF EXISTS fact_cibles CASCADE;
DROP TABLE IF EXISTS dim_antigene CASCADE;
DROP TABLE IF EXISTS dim_periode CASCADE;
DROP TABLE IF EXISTS dim_year CASCADE;
DROP TABLE IF EXISTS dim_geo CASCADE;
DROP TABLE IF EXISTS dim_district CASCADE;

-- District = Zone de sant? (1 par zone, pour FKs vaccination/cibles)
CREATE TABLE dim_district (
    zone_sante VARCHAR(100) PRIMARY KEY
);

-- Dimension géographique (alignée page Power BI "Cibles")
-- RDC: Region = Province, District = Zone de santé, AS = Aire de santé, FOSA = Formation sanitaire, FOSA_FPP = Oui/Non
CREATE TABLE dim_geo (
    id_geo SERIAL PRIMARY KEY,
    pays VARCHAR(50) NOT NULL,
    province VARCHAR(100) NOT NULL,
    zone_sante VARCHAR(100) NOT NULL REFERENCES dim_district(zone_sante),
    aire_sante VARCHAR(100) NOT NULL,
    code_zs VARCHAR(20),
    code_as VARCHAR(20) NOT NULL UNIQUE,
    region VARCHAR(100) NOT NULL,
    district VARCHAR(100) NOT NULL,
    as_aire VARCHAR(100) NOT NULL,
    fosa VARCHAR(150) NOT NULL UNIQUE,
    est_fpp SMALLINT NOT NULL DEFAULT 1
);
CREATE INDEX idx_dim_geo_zone_sante ON dim_geo(zone_sante);
CREATE INDEX idx_dim_geo_code_as ON dim_geo(code_as);

-- Dimension FOSA d?di?e au reporting (?vite ambigu?t? Power BI : 1 seule route de filtre)
CREATE TABLE dim_fosa (
    fosa VARCHAR(150) PRIMARY KEY,
    as_aire VARCHAR(100) NOT NULL,
    district VARCHAR(100) NOT NULL,
    region VARCHAR(100) NOT NULL
);

-- Dimension mois (YearMonth = year*100+month_num, trimestre, semestre, libell?s)
CREATE TABLE dim_month (
    year_month INTEGER PRIMARY KEY,
    year INTEGER NOT NULL,
    month VARCHAR(20) NOT NULL,
    month_num INTEGER NOT NULL,
    trimestre VARCHAR(10) NOT NULL,
    semestre VARCHAR(10) NOT NULL,
    month_short VARCHAR(10) NOT NULL,
    year_month_label VARCHAR(30) NOT NULL
);

-- Dimension ann?e (avec libell? pour affichage)
CREATE TABLE dim_year (
    year INTEGER PRIMARY KEY,
    year_label VARCHAR(20) NOT NULL
);

-- Dimension semaine (semaines ISO : lundi = d?but, year_week = year*100+week_num)
CREATE TABLE dim_semaine (
    year_week INTEGER PRIMARY KEY,
    year INTEGER NOT NULL,
    week_num INTEGER NOT NULL,
    week_start_date DATE NOT NULL,
    week_end_date DATE NOT NULL,
    week_label VARCHAR(50) NOT NULL
);
CREATE INDEX idx_dim_semaine_year ON dim_semaine(year);
CREATE INDEX idx_dim_semaine_dates ON dim_semaine(week_start_date, week_end_date);

-- Dimension trimestre (T1..T4 par ann?e, year_trimestre = year*10 + trimestre_num)
CREATE TABLE dim_trimestre (
    year_trimestre INTEGER PRIMARY KEY,
    year INTEGER NOT NULL,
    trimestre VARCHAR(10) NOT NULL,
    trimestre_num INTEGER NOT NULL,
    trimestre_label VARCHAR(20) NOT NULL
);
CREATE INDEX idx_dim_trimestre_year ON dim_trimestre(year);

-- Dimension semestre (S1, S2 par ann?e, year_semestre = year*10 + semestre_num)
CREATE TABLE dim_semestre (
    year_semestre INTEGER PRIMARY KEY,
    year INTEGER NOT NULL,
    semestre VARCHAR(10) NOT NULL,
    semestre_num INTEGER NOT NULL,
    semestre_label VARCHAR(20) NOT NULL
);
CREATE INDEX idx_dim_semestre_year ON dim_semestre(year);

-- Dimension p?riode (mois/jour pour vaccination)
CREATE TABLE dim_periode (
    id_periode SERIAL PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    annee INTEGER NOT NULL,
    mois VARCHAR(20) NOT NULL,
    mois_num INTEGER NOT NULL,
    trimestre VARCHAR(10) NOT NULL,
    semaine INTEGER NOT NULL
);

-- Dimension antig?ne
CREATE TABLE dim_antigene (
    antigene VARCHAR(20) PRIMARY KEY,
    dose_num INTEGER NOT NULL,
    type_vaccin VARCHAR(50) NOT NULL,
    groupe_age VARCHAR(50)
);

-- Table de faits : cibles d?mographiques (niveau Zone de sant?, historique)
CREATE TABLE fact_cibles (
    id_cibles SERIAL PRIMARY KEY,
    annee INTEGER NOT NULL,
    zone_sante VARCHAR(100) NOT NULL,
    population_totale INTEGER NOT NULL,
    population_0_11_mois INTEGER NOT NULL,
    population_12_23_mois INTEGER NOT NULL,
    CONSTRAINT fk_cibles_zone FOREIGN KEY (zone_sante) REFERENCES dim_district(zone_sante)
);

-- Table de faits : cibles pour page Power BI "Cibles" (granularit? AS + Year)
CREATE TABLE fact_targets (
    id_targets SERIAL PRIMARY KEY,
    year INTEGER NOT NULL,
    code_as VARCHAR(20) NOT NULL,
    livebirths INTEGER NOT NULL,
    target_0_11 INTEGER NOT NULL,
    target_12_23 INTEGER NOT NULL,
    target_12_59 INTEGER NOT NULL,
    target_0_59 INTEGER NOT NULL,
    expected_pregnant_women INTEGER NOT NULL,
    CONSTRAINT fk_targets_geo FOREIGN KEY (code_as) REFERENCES dim_geo(code_as),
    CONSTRAINT fk_targets_year FOREIGN KEY (year) REFERENCES dim_year(year),
    CONSTRAINT uq_targets_as_year UNIQUE (code_as, year)
);

-- Table de faits : vaccination (Date + Code_ZS + cl�s temps pour Power BI)
CREATE TABLE fact_vaccination (
    id_vaccination SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    year_month INTEGER NOT NULL,
    year INTEGER NOT NULL,
    month_num INTEGER NOT NULL,
    code_zs VARCHAR(20) NOT NULL,
    zone_sante VARCHAR(100) NOT NULL,
    antigene VARCHAR(20) NOT NULL,
    dose_num INTEGER NOT NULL,
    enfants_vaccines INTEGER NOT NULL,
    CONSTRAINT fk_vaccination_zone FOREIGN KEY (zone_sante) REFERENCES dim_district(zone_sante),
    CONSTRAINT fk_vaccination_antigene FOREIGN KEY (antigene) REFERENCES dim_antigene(antigene)
);
CREATE INDEX idx_fact_vaccination_year_month ON fact_vaccination(year_month);
CREATE INDEX idx_fact_vaccination_code_zs ON fact_vaccination(code_zs);

-- Table de faits : logistique (stocks)
CREATE TABLE fact_logistique (
    id_logistique SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    zone_sante VARCHAR(100) NOT NULL,
    antigene VARCHAR(20) NOT NULL,
    stock_ouverture INTEGER NOT NULL,
    stock_recu INTEGER NOT NULL,
    stock_utilise INTEGER NOT NULL,
    stock_perdu INTEGER NOT NULL,
    CONSTRAINT fk_logistique_zone FOREIGN KEY (zone_sante) REFERENCES dim_district(zone_sante),
    CONSTRAINT fk_logistique_antigene FOREIGN KEY (antigene) REFERENCES dim_antigene(antigene)
);

-- Table de faits : reporting (Compl?tude et promptitude - 1 ligne = 1 FOSA / 1 mois / 1 ann?e)
-- Relations uniquement vers dim_fosa et dim_month = sch?ma ?toile, 0 ambigu?t?
CREATE TABLE fact_reporting (
    id_reporting SERIAL PRIMARY KEY,
    year_month INTEGER NOT NULL,
    year INTEGER NOT NULL,
    month VARCHAR(20) NOT NULL,
    month_num INTEGER NOT NULL,
    region VARCHAR(100) NOT NULL,
    district VARCHAR(100) NOT NULL,
    as_aire VARCHAR(100) NOT NULL,
    fosa VARCHAR(150) NOT NULL,
    rapport_attendu INTEGER NOT NULL,
    rapport_recu INTEGER NOT NULL,
    rapport_a_temps INTEGER NOT NULL,
    CONSTRAINT fk_reporting_fosa FOREIGN KEY (fosa) REFERENCES dim_fosa(fosa),
    CONSTRAINT fk_reporting_year_month FOREIGN KEY (year_month) REFERENCES dim_month(year_month)
);
CREATE INDEX idx_fact_reporting_year_month ON fact_reporting(year_month);
CREATE INDEX idx_fact_reporting_fosa ON fact_reporting(fosa);

-- Index
CREATE INDEX idx_fact_vaccination_date ON fact_vaccination(date);
CREATE INDEX idx_fact_vaccination_zone ON fact_vaccination(zone_sante);
CREATE INDEX idx_fact_vaccination_antigene ON fact_vaccination(antigene);
CREATE INDEX idx_fact_logistique_date ON fact_logistique(date);
CREATE INDEX idx_fact_cibles_annee ON fact_cibles(annee);
CREATE INDEX idx_fact_targets_year ON fact_targets(year);
CREATE INDEX idx_fact_targets_code_as ON fact_targets(code_as);
