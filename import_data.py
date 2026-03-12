#!/usr/bin/env python3
"""
Script d'importation des données PEV RDC dans PostgreSQL
Structure conforme au modèle Power BI
"""

import re
from urllib.parse import quote_plus
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from sqlalchemy import create_engine
import os
import subprocess
from datetime import datetime, date, timedelta
import sys

# Configuration de la base de données
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "pev_rdc",
    "user": "pev_admin",
    "password": "P@ssw0rdPEV2024"
}

class PEVDatabaseImporter:
    def __init__(self, db_config):
        """Initialise la connexion à la base de données"""
        self.conn = None
        self.cursor = None
        self.db_config = db_config
        self._engine = None
        self.connect()
    
    def connect(self):
        """Établit la connexion à PostgreSQL"""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.cursor = self.conn.cursor()
            print("✅ Connexion à PostgreSQL établie")
        except Exception as e:
            print(f"❌ Erreur de connexion: {e}")
            sys.exit(1)

    def _get_engine(self):
        """Moteur SQLAlchemy pour pandas (évite le warning read_sql_query)."""
        if self._engine is None:
            u = quote_plus(self.db_config["user"])
            p = quote_plus(self.db_config["password"])
            h = self.db_config["host"]
            port = self.db_config["port"]
            db = self.db_config["database"]
            self._engine = create_engine(f"postgresql://{u}:{p}@{h}:{port}/{db}")
        return self._engine

    def create_tables(self):
        """Crée les tables si elles n'existent pas"""
        try:
            with open('init-scripts/01-create-tables.sql', 'r', encoding='utf-8') as f:
                sql_script = f.read()
            # Exécuter chaque instruction SQL séparément (psycopg2 n'exécute qu'une requête à la fois)
            for raw in sql_script.split(';'):
                raw = raw.strip()
                if not raw:
                    continue
                # Supprimer toutes les lignes qui ne sont que des commentaires (PostgreSQL = "empty query")
                lines = [ln for ln in raw.split('\n') if ln.strip() and not ln.strip().startswith('--')]
                stmt = '\n'.join(lines).strip()
                if not stmt or not re.search(r'\w', stmt):
                    continue
                self.cursor.execute(stmt)
            self.conn.commit()
            print("✅ Tables créées avec succès")
        except Exception as e:
            print(f"❌ Erreur lors de la création des tables: {e}")
            self.conn.rollback()
            raise
    
    def import_dim_district(self):
        """Importe les districts (zones de santé) pour les FKs."""
        zones = [
            "Limmété", "Bandalungwa", "Lubumbashi", "Kipushi",
            "Kananga", "Dibaya", "Bunia", "Mambasa"
        ]
        for z in zones:
            self.cursor.execute(
                "INSERT INTO dim_district (zone_sante) VALUES (%s) ON CONFLICT (zone_sante) DO NOTHING",
                (z,)
            )
        self.conn.commit()
        print(f"✅ {len(zones)} districts insérés dans dim_district")

    def import_dim_geo(self):
        """Importe les données géographiques (Region, District, AS, FOSA_FPP pour page Cibles)."""
        # pays, province, zone_sante, aire_sante, code_zs, code_as, region, district, as_aire, fosa, est_fpp=1
        data = [
            ("RDC", "Kinshasa", "Limmété", "Kinkole", "KIN01", "KIN01-01", "Kinshasa", "Limmété", "Kinkole", "CS Kinkole"),
            ("RDC", "Kinshasa", "Limmété", "Kinsuka", "KIN01", "KIN01-02", "Kinshasa", "Limmété", "Kinsuka", "CS Kinsuka"),
            ("RDC", "Kinshasa", "Limmété", "Ngaliema", "KIN01", "KIN01-03", "Kinshasa", "Limmété", "Ngaliema", "CS Ngaliema"),
            ("RDC", "Kinshasa", "Bandalungwa", "Bandal,Selé", "KIN02", "KIN02-01", "Kinshasa", "Bandalungwa", "Bandal-Selé", "CS Bandal"),
            ("RDC", "Kinshasa", "Bandalungwa", "Funa", "KIN02", "KIN02-02", "Kinshasa", "Bandalungwa", "Funa", "CS Funa"),
            ("RDC", "Kinshasa", "Bandalungwa", "Kauka", "KIN02", "KIN02-03", "Kinshasa", "Bandalungwa", "Kauka", "CS Kauka"),
            ("RDC", "Haut-Katanga", "Lubumbashi", "Kampemba", "HK01", "HK01-01", "Haut-Katanga", "Lubumbashi", "Kampemba", "CS Kampemba"),
            ("RDC", "Haut-Katanga", "Lubumbashi", "Kenya", "HK01", "HK01-02", "Haut-Katanga", "Lubumbashi", "Kenya", "CS Kenya"),
            ("RDC", "Haut-Katanga", "Lubumbashi", "Rwashi", "HK01", "HK01-03", "Haut-Katanga", "Lubumbashi", "Rwashi", "CS Rwashi"),
            ("RDC", "Haut-Katanga", "Kipushi", "Kipushi Centre", "HK02", "HK02-01", "Haut-Katanga", "Kipushi", "Kipushi Centre", "CS Kipushi Centre"),
            ("RDC", "Haut-Katanga", "Kipushi", "Kasaka", "HK02", "HK02-02", "Haut-Katanga", "Kipushi", "Kasaka", "CS Kasaka"),
            ("RDC", "Haut-Katanga", "Kipushi", "Shituru", "HK02", "HK02-03", "Haut-Katanga", "Kipushi", "Shituru", "CS Shituru"),
            ("RDC", "Kasaï Central", "Kananga", "Kananga Ville", "KC01", "KC01-01", "Kasaï Central", "Kananga", "Kananga Ville", "CS Kananga Ville"),
            ("RDC", "Kasaï Central", "Kananga", "Ndolo", "KC01", "KC01-02", "Kasaï Central", "Kananga", "Ndolo", "CS Ndolo"),
            ("RDC", "Kasaï Central", "Kananga", "Lupatapata", "KC01", "KC01-03", "Kasaï Central", "Kananga", "Lupatapata", "CS Lupatapata"),
            ("RDC", "Kasaï Central", "Dibaya", "Dibaya Centre", "KC02", "KC02-01", "Kasaï Central", "Dibaya", "Dibaya Centre", "CS Dibaya Centre"),
            ("RDC", "Kasaï Central", "Dibaya", "Kalamba", "KC02", "KC02-02", "Kasaï Central", "Dibaya", "Kalamba", "CS Kalamba"),
            ("RDC", "Kasaï Central", "Dibaya", "Kazumba", "KC02", "KC02-03", "Kasaï Central", "Dibaya", "Kazumba", "CS Kazumba"),
            ("RDC", "Ituri", "Bunia", "Bunia Mairie", "IT01", "IT01-01", "Ituri", "Bunia", "Bunia Mairie", "CS Bunia Mairie"),
            ("RDC", "Ituri", "Bunia", "Soya", "IT01", "IT01-02", "Ituri", "Bunia", "Soya", "CS Soya"),
            ("RDC", "Ituri", "Bunia", "Lolwa", "IT01", "IT01-03", "Ituri", "Bunia", "Lolwa", "CS Lolwa"),
            ("RDC", "Ituri", "Mambasa", "Mambasa Centre", "IT02", "IT02-01", "Ituri", "Mambasa", "Mambasa Centre", "CS Mambasa Centre"),
            ("RDC", "Ituri", "Mambasa", "Bakwali", "IT02", "IT02-02", "Ituri", "Mambasa", "Bakwali", "CS Bakwali"),
            ("RDC", "Ituri", "Mambasa", "Bafwanka", "IT02", "IT02-03", "Ituri", "Mambasa", "Bafwanka", "CS Bafwanka"),
        ]
        query = """
        INSERT INTO dim_geo (pays, province, zone_sante, aire_sante, code_zs, code_as, region, district, as_aire, fosa, est_fpp)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1)
        ON CONFLICT (code_as) DO NOTHING
        """
        execute_batch(self.cursor, query, data)
        self.conn.commit()
        print(f"✅ {len(data)} enregistrements insérés dans dim_geo")

    def import_dim_fosa(self):
        """Remplit dim_fosa à partir de dim_geo (dimension dédiée reporting = 0 ambiguïté Power BI)."""
        self.cursor.execute("""
            INSERT INTO dim_fosa (fosa, as_aire, district, region)
            SELECT fosa, as_aire, district, region FROM dim_geo
            ON CONFLICT (fosa) DO NOTHING
        """)
        self.conn.commit()
        self.cursor.execute("SELECT COUNT(*) FROM dim_fosa")
        n = self.cursor.fetchone()[0]
        print(f"✅ {n} enregistrements insérés dans dim_fosa")

    def import_dim_month(self):
        """Remplit dim_month (YearMonth, trimestre, semestre, libellés courts)."""
        month_names = [
            "Janvier", "Février", "Mars", "Avril", "Mai", "Juin",
            "Juillet", "Août", "Septembre", "Octobre", "Novembre", "Décembre"
        ]
        month_short = [
            "Janv", "Févr", "Mars", "Avr", "Mai", "Juin",
            "Juil", "Août", "Sept", "Oct", "Nov", "Déc"
        ]
        rows = []
        for year in [2023, 2024, 2025]:
            for month_num in range(1, 13):
                year_month = year * 100 + month_num
                month_name = month_names[month_num - 1]
                trimestre = f"T{(month_num - 1) // 3 + 1}"
                semestre = "S1" if month_num <= 6 else "S2"
                short = month_short[month_num - 1]
                year_month_label = f"{short} {year}"
                rows.append((year_month, year, month_name, month_num, trimestre, semestre, short, year_month_label))
        query = """
        INSERT INTO dim_month (year_month, year, month, month_num, trimestre, semestre, month_short, year_month_label)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (year_month) DO UPDATE SET
            trimestre = EXCLUDED.trimestre, semestre = EXCLUDED.semestre,
            month_short = EXCLUDED.month_short, year_month_label = EXCLUDED.year_month_label
        """
        execute_batch(self.cursor, query, rows)
        self.conn.commit()
        print(f"✅ {len(rows)} enregistrements insérés dans dim_month")

    def import_dim_semaine(self):
        """Remplit dim_semaine avec les semaines ISO (lundi = début) pour 2023–2025."""
        month_short = ["Janv", "Févr", "Mars", "Avr", "Mai", "Juin",
                       "Juil", "Août", "Sept", "Oct", "Nov", "Déc"]
        start_d = date(2023, 1, 1)
        end_d = date(2025, 12, 31)
        weeks_seen = set()
        rows = []
        d = start_d
        while d <= end_d:
            iso_year, iso_week, _ = d.isocalendar()
            key = (iso_year, iso_week)
            if key not in weeks_seen:
                weeks_seen.add(key)
                monday = d - timedelta(days=d.weekday())
                sunday = monday + timedelta(days=6)
                year_week = iso_year * 100 + iso_week
                week_label = (
                    f"S{iso_week:02d} {iso_year} "
                    f"({monday.day:02d}-{sunday.day:02d} {month_short[monday.month - 1]})"
                )
                rows.append((year_week, iso_year, iso_week, monday, sunday, week_label))
            d += timedelta(days=1)
        query = """
        INSERT INTO dim_semaine (year_week, year, week_num, week_start_date, week_end_date, week_label)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (year_week) DO UPDATE SET
            week_start_date = EXCLUDED.week_start_date,
            week_end_date = EXCLUDED.week_end_date,
            week_label = EXCLUDED.week_label
        """
        execute_batch(self.cursor, query, rows)
        self.conn.commit()
        print(f"✅ {len(rows)} semaines ISO insérées dans dim_semaine")

    def import_dim_trimestre(self):
        """Remplit dim_trimestre (T1..T4 par année)."""
        rows = []
        for year in [2023, 2024, 2025]:
            for t in range(1, 5):
                year_trimestre = year * 10 + t
                trimestre = f"T{t}"
                trimestre_label = f"T{t} {year}"
                rows.append((year_trimestre, year, trimestre, t, trimestre_label))
        query = """
        INSERT INTO dim_trimestre (year_trimestre, year, trimestre, trimestre_num, trimestre_label)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (year_trimestre) DO UPDATE SET trimestre_label = EXCLUDED.trimestre_label
        """
        execute_batch(self.cursor, query, rows)
        self.conn.commit()
        print(f"✅ {len(rows)} enregistrements insérés dans dim_trimestre")

    def import_dim_semestre(self):
        """Remplit dim_semestre (S1, S2 par année)."""
        rows = []
        for year in [2023, 2024, 2025]:
            for s in range(1, 3):
                year_semestre = year * 10 + s
                semestre = f"S{s}"
                semestre_label = f"S{s} {year}"
                rows.append((year_semestre, year, semestre, s, semestre_label))
        query = """
        INSERT INTO dim_semestre (year_semestre, year, semestre, semestre_num, semestre_label)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (year_semestre) DO UPDATE SET semestre_label = EXCLUDED.semestre_label
        """
        execute_batch(self.cursor, query, rows)
        self.conn.commit()
        print(f"✅ {len(rows)} enregistrements insérés dans dim_semestre")
    
    def import_dim_periode(self):
        """Importe les périodes"""
        periods = []
        for year in [2023, 2024]:
            for month in range(1, 13):
                date = f"{year}-{month:02d}-01"
                month_names = [
                    "Janvier", "Février", "Mars", "Avril", "Mai", "Juin",
                    "Juliet", "Août", "Septembre", "Octobre", "Novembre", "Décembre"
                ]
                month_name = month_names[month-1]
                quarter = f"T{(month-1)//3 + 1}"
                week = ((month-1) * 4) + 1
                periods.append((date, year, month_name, month, quarter, week))
        
        query = """
        INSERT INTO dim_periode (date, annee, mois, mois_num, trimestre, semaine)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (date) DO NOTHING
        """
        
        execute_batch(self.cursor, query, periods)
        self.conn.commit()
        print(f"✅ {len(periods)} enregistrements insérés dans dim_periode")
    
    def import_dim_antigene(self):
        """Importe les antigènes"""
        antigens = [
            ("BCG", 0, "BCG", "À la naissance"),
            ("Polio0", 0, "VPO", "À la naissance"),
            ("Penta1", 1, "Penta", "0-11 mois"),
            ("Penta2", 2, "Penta", "0-11 mois"),
            ("Penta3", 3, "Penta", "0-11 mois"),
            ("RR1", 1, "Rougeole", "9-59 mois"),
            ("RR2", 2, "Rougeole", "15-59 mois"),
            ("VAR", 1, "Varicelle", "12-59 mois"),
            ("VAA", 1, "Vitamine A", "6-59 mois"),
            ("VPI1", 1, "VPI", "0-11 mois"),
            ("VPI2", 2, "VPI", "0-11 mois"),
            ("VPI3", 3, "VPI", "0-11 mois")
        ]
        
        query = """
        INSERT INTO dim_antigene (antigene, dose_num, type_vaccin, groupe_age)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (antigene) DO NOTHING
        """
        
        execute_batch(self.cursor, query, antigens)
        self.conn.commit()
        print(f"✅ {len(antigens)} enregistrements insérés dans dim_antigene")
    
    def import_fact_cibles(self):
        """Importe les cibles démographiques"""
        targets = [
            (2023, "Limmété", 350000, 8750, 8400),
            (2023, "Bandalungwa", 280000, 7000, 6720),
            (2023, "Lubumbashi", 500000, 12500, 12000),
            (2023, "Kipushi", 180000, 4500, 4320),
            (2023, "Kananga", 420000, 10500, 10080),
            (2023, "Dibaya", 150000, 3750, 3600),
            (2023, "Bunia", 320000, 8000, 7680),
            (2023, "Mambasa", 200000, 5000, 4800),
            (2024, "Limmété", 360000, 9000, 8640),
            (2024, "Bandalungwa", 285000, 7125, 6840),
            (2024, "Lubumbashi", 510000, 12750, 12240),
            (2024, "Kipushi", 185000, 4625, 4440),
            (2024, "Kananga", 430000, 10750, 10320),
            (2024, "Dibaya", 155000, 3875, 3720),
            (2024, "Bunia", 325000, 8125, 7800),
            (2024, "Mambasa", 205000, 5125, 4920)
        ]
        
        query = """
        INSERT INTO fact_cibles (annee, zone_sante, population_totale, population_0_11_mois, population_12_23_mois)
        VALUES (%s, %s, %s, %s, %s)
        """
        
        execute_batch(self.cursor, query, targets)
        self.conn.commit()
        print(f"✅ {len(targets)} enregistrements insérés dans fact_cibles")

    def import_dim_year(self):
        """Importe les années avec libellé (dim_year bien complète)."""
        for year in [2023, 2024, 2025]:
            self.cursor.execute(
                """INSERT INTO dim_year (year, year_label) VALUES (%s, %s)
                   ON CONFLICT (year) DO UPDATE SET year_label = EXCLUDED.year_label""",
                (year, f"Année {year}")
            )
        self.conn.commit()
        print("✅ dim_year insérée (year, year_label) pour 2023–2025")

    def import_fact_targets(self):
        """Importe les cibles par AS et année (page Power BI Cibles: LiveBirths, 0-11, 12-23, 12-59, 0-59, Femmes enceintes)."""
        code_as_list = [
            "KIN01-01", "KIN01-02", "KIN01-03", "KIN02-01", "KIN02-02", "KIN02-03",
            "HK01-01", "HK01-02", "HK01-03", "HK02-01", "HK02-02", "HK02-03",
            "KC01-01", "KC01-02", "KC01-03", "KC02-01", "KC02-02", "KC02-03",
            "IT01-01", "IT01-02", "IT01-03", "IT02-01", "IT02-02", "IT02-03"
        ]
        # Base par code (ordre cohérent pour reproductibilité)
        rows = []
        for code_as in code_as_list:
            for year in [2023, 2024, 2025]:
                b = 400 + hash(code_as + str(year)) % 600
                livebirths = b
                t011 = b // 40
                t1223 = int(t011 * 0.95)
                t1259 = t011 * 3 + t1223
                t059 = t011 * 4 + t1223
                pregnant = int(b * 0.06)
                rows.append((year, code_as, livebirths, t011, t1223, t1259, t059, pregnant))
        query = """
        INSERT INTO fact_targets (year, code_as, livebirths, target_0_11, target_12_23, target_12_59, target_0_59, expected_pregnant_women)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (code_as, year) DO UPDATE SET
            livebirths = EXCLUDED.livebirths,
            target_0_11 = EXCLUDED.target_0_11,
            target_12_23 = EXCLUDED.target_12_23,
            target_12_59 = EXCLUDED.target_12_59,
            target_0_59 = EXCLUDED.target_0_59,
            expected_pregnant_women = EXCLUDED.expected_pregnant_women
        """
        execute_batch(self.cursor, query, rows)
        self.conn.commit()
        print(f"✅ {len(rows)} enregistrements insérés dans fact_targets")
    
    def import_fact_vaccination(self):
        """Importe les données de vaccination (Date, Code_ZS, YearMonth, Year, MonthNum, Antigene, DoseNum, EnfantsVaccines)."""
        # Zone de santé → Code_ZS (clé géo stable, aligné dim_geo)
        zone_to_code_zs = {
            "Limmété": "KIN01", "Bandalungwa": "KIN02",
            "Lubumbashi": "HK01", "Kipushi": "HK02",
            "Kananga": "KC01", "Dibaya": "KC02",
            "Bunia": "IT01", "Mambasa": "IT02",
        }
        raw = [
            ("2023-01-01", "Limmété", "BCG", 0, 245),
            ("2023-01-01", "Limmété", "Penta1", 1, 198),
            ("2023-01-01", "Limmété", "Penta3", 3, 156),
            ("2023-01-01", "Bandalungwa", "BCG", 0, 312),
            ("2023-01-01", "Bandalungwa", "Penta1", 1, 267),
            ("2023-01-01", "Bandalungwa", "Penta3", 3, 201),
            ("2023-01-01", "Lubumbashi", "BCG", 0, 412),
            ("2023-01-01", "Lubumbashi", "Penta1", 1, 356),
            ("2023-01-01", "Lubumbashi", "Penta3", 3, 287),
            ("2023-01-01", "Kipushi", "BCG", 0, 189),
            ("2023-01-01", "Kipushi", "Penta1", 1, 156),
            ("2023-01-01", "Kipushi", "Penta3", 3, 112),
            ("2023-02-01", "Limmété", "BCG", 0, 231),
            ("2023-02-01", "Limmété", "Penta1", 1, 187),
            ("2023-02-01", "Limmété", "Penta3", 3, 142),
            ("2023-02-01", "Bandalungwa", "BCG", 0, 298),
            ("2023-02-01", "Bandalungwa", "Penta1", 1, 245),
            ("2023-02-01", "Bandalungwa", "Penta3", 3, 189),
            ("2023-03-01", "Limmété", "BCG", 0, 267),
            ("2023-03-01", "Limmété", "Penta1", 1, 213),
            ("2023-03-01", "Limmété", "Penta3", 3, 167),
            ("2023-03-01", "Bandalungwa", "BCG", 0, 345),
            ("2023-03-01", "Bandalungwa", "Penta1", 1, 289),
            ("2023-03-01", "Bandalungwa", "Penta3", 3, 221),
            ("2024-01-01", "Limmété", "BCG", 0, 267),
            ("2024-01-01", "Limmété", "Penta1", 1, 221),
            ("2024-01-01", "Limmété", "Penta3", 3, 178),
            ("2024-01-01", "Bandalungwa", "BCG", 0, 356),
            ("2024-01-01", "Bandalungwa", "Penta1", 1, 298),
            ("2024-01-01", "Bandalungwa", "Penta3", 3, 234),
            ("2024-01-01", "Bunia", "BCG", 0, 123),
            ("2024-01-01", "Bunia", "Penta1", 1, 89),
            ("2024-01-01", "Bunia", "Penta3", 3, 67),
            ("2024-01-01", "Mambasa", "BCG", 0, 78),
            ("2024-01-01", "Mambasa", "Penta1", 1, 56),
            ("2024-01-01", "Mambasa", "Penta3", 3, 34),
        ]
        vaccinations = []
        for date_s, zone_sante, antigene, dose_num, enfants in raw:
            y, m = int(date_s[:4]), int(date_s[5:7])
            year_month = y * 100 + m
            code_zs = zone_to_code_zs.get(zone_sante, zone_sante[:3].upper())
            vaccinations.append((date_s, year_month, y, m, code_zs, zone_sante, antigene, dose_num, enfants))
        query = """
        INSERT INTO fact_vaccination (date, year_month, year, month_num, code_zs, zone_sante, antigene, dose_num, enfants_vaccines)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        execute_batch(self.cursor, query, vaccinations)
        self.conn.commit()
        print(f"✅ {len(vaccinations)} enregistrements insérés dans fact_vaccination")
    
    def import_fact_logistique(self):
        """Importe les données logistiques"""
        # antigene doit référencer dim_antigene.antigene (ex: Penta1, BCG), pas le type (Penta)
        logistics = [
            ("2023-01-01", "Limmété", "Penta1", 1200, 1000, 980, 15),
            ("2023-01-01", "Limmété", "BCG", 800, 500, 520, 8),
            ("2023-01-01", "Bandalungwa", "Penta1", 1500, 1200, 1150, 25),
            ("2023-02-01", "Limmété", "Penta1", 205, 1000, 950, 12),
            ("2023-02-01", "Bandalungwa", "Penta1", 525, 1200, 1180, 18),
            ("2024-01-01", "Bunia", "Penta1", 300, 800, 750, 30),
            ("2024-01-01", "Mambasa", "Penta1", 200, 600, 520, 45)
        ]
        
        query = """
        INSERT INTO fact_logistique (date, zone_sante, antigene, stock_ouverture, stock_recu, stock_utilise, stock_perdu)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        
        execute_batch(self.cursor, query, logistics)
        self.conn.commit()
        print(f"✅ {len(logistics)} enregistrements insérés dans fact_logistique")
    
    def import_fact_reporting(self):
        """Importe les données de reporting (1 ligne = 1 FOSA / 1 mois / 1 année)."""
        month_names = [
            "Janvier", "Février", "Mars", "Avril", "Mai", "Juin",
            "Juillet", "Août", "Septembre", "Octobre", "Novembre", "Décembre"
        ]
        # (region, district, as_aire, fosa) aligné sur dim_geo
        fosa_list = [
            ("Kinshasa", "Limmété", "Kinkole", "CS Kinkole"),
            ("Kinshasa", "Limmété", "Kinsuka", "CS Kinsuka"),
            ("Kinshasa", "Limmété", "Ngaliema", "CS Ngaliema"),
            ("Kinshasa", "Bandalungwa", "Bandal-Selé", "CS Bandal"),
            ("Kinshasa", "Bandalungwa", "Funa", "CS Funa"),
            ("Kinshasa", "Bandalungwa", "Kauka", "CS Kauka"),
            ("Haut-Katanga", "Lubumbashi", "Kampemba", "CS Kampemba"),
            ("Haut-Katanga", "Lubumbashi", "Kenya", "CS Kenya"),
            ("Haut-Katanga", "Lubumbashi", "Rwashi", "CS Rwashi"),
            ("Haut-Katanga", "Kipushi", "Kipushi Centre", "CS Kipushi Centre"),
            ("Haut-Katanga", "Kipushi", "Kasaka", "CS Kasaka"),
            ("Haut-Katanga", "Kipushi", "Shituru", "CS Shituru"),
            ("Kasaï Central", "Kananga", "Kananga Ville", "CS Kananga Ville"),
            ("Kasaï Central", "Kananga", "Ndolo", "CS Ndolo"),
            ("Kasaï Central", "Kananga", "Lupatapata", "CS Lupatapata"),
            ("Kasaï Central", "Dibaya", "Dibaya Centre", "CS Dibaya Centre"),
            ("Kasaï Central", "Dibaya", "Kalamba", "CS Kalamba"),
            ("Kasaï Central", "Dibaya", "Kazumba", "CS Kazumba"),
            ("Ituri", "Bunia", "Bunia Mairie", "CS Bunia Mairie"),
            ("Ituri", "Bunia", "Soya", "CS Soya"),
            ("Ituri", "Bunia", "Lolwa", "CS Lolwa"),
            ("Ituri", "Mambasa", "Mambasa Centre", "CS Mambasa Centre"),
            ("Ituri", "Mambasa", "Bakwali", "CS Bakwali"),
            ("Ituri", "Mambasa", "Bafwanka", "CS Bafwanka"),
        ]
        reporting = []
        for year in [2023, 2024]:
            for month_num in range(1, 13):
                year_month = year * 100 + month_num
                month_name = month_names[month_num - 1]
                for region, district, as_aire, fosa in fosa_list:
                    # Rapport attendu = 1. Reçu / à temps : varier pour démo (complétude ~85–95 %, promptitude ~75–92 %)
                    h = hash(f"{fosa}{year}{month_num}") % 100
                    rapport_recu = 1 if h >= 5 else 0
                    rapport_a_temps = 1 if (rapport_recu and h >= 12) else 0
                    reporting.append((year_month, year, month_name, month_num, region, district, as_aire, fosa, 1, rapport_recu, rapport_a_temps))
        query = """
        INSERT INTO fact_reporting (year_month, year, month, month_num, region, district, as_aire, fosa, rapport_attendu, rapport_recu, rapport_a_temps)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        execute_batch(self.cursor, query, reporting)
        self.conn.commit()
        print(f"✅ {len(reporting)} enregistrements insérés dans fact_reporting")
    
    def verify_data(self):
        """Vérifie l'intégrité des données"""
        queries = [
            ("Nombre de zones de santé", "SELECT COUNT(*) FROM dim_geo"),
            ("Nombre d'aires (AS)", "SELECT COUNT(*) FROM dim_geo"),
            ("Nombre de cibles (AS x Year)", "SELECT COUNT(*) FROM fact_targets"),
            ("Nombre de périodes", "SELECT COUNT(*) FROM dim_periode"),
            ("Nombre d'antigènes", "SELECT COUNT(*) FROM dim_antigene"),
            ("Nombre de vaccinations", "SELECT COUNT(*) FROM fact_vaccination"),
            ("Couverture Penta3 2023", """
                SELECT ROUND(AVG(v.enfants_vaccines::DECIMAL / NULLIF(c.population_0_11_mois, 0)) * 100, 2)
                FROM fact_vaccination v
                JOIN fact_cibles c ON v.zone_sante = c.zone_sante AND EXTRACT(YEAR FROM v.date) = c.annee
                WHERE v.antigene = 'Penta3' AND EXTRACT(YEAR FROM v.date) = 2023
            """),
            ("Taux de reporting 2023", """
                SELECT ROUND(SUM(rapport_recu)::DECIMAL / NULLIF(SUM(rapport_attendu), 0) * 100, 2)
                FROM fact_reporting
                WHERE year = 2023
            """)
        ]
        
        print("\n📊 VÉRIFICATION DES DONNÉES:")
        print("=" * 50)
        
        for label, query in queries:
            try:
                self.cursor.execute(query)
                result = self.cursor.fetchone()[0]
                print(f"{label}: {result}")
            except Exception as e:
                print(f"{label}: Erreur - {e}")
    
    def export_to_csv(self):
        """Exporte les données en CSV pour Power BI (page Cibles: dim_geo, dim_year, fact_targets)."""
        os.makedirs("export_powerbi", exist_ok=True)
        
        # Tables avec noms standards
        tables = ["dim_district", "dim_periode", "dim_antigene",
                  "fact_cibles", "fact_logistique"]
        for table in tables:
            query = f"SELECT * FROM {table}"
            df = pd.read_sql_query(query, self._get_engine())
            df.columns = [col.replace('_', ' ').title().replace(' ', '') for col in df.columns]
            df.to_csv(f"export_powerbi/{table}.csv", index=False, encoding='utf-8-sig')
            print(f"📁 {table} exporté vers export_powerbi/{table}.csv ({len(df)} lignes)")

        # fact_vaccination : Date, YearMonth, Year, MonthNum, Code_ZS, ZoneSante, Antigene, DoseNum, EnfantsVaccines
        df_vacc = pd.read_sql_query("""
            SELECT date AS "Date", year_month AS "YearMonth", year AS "Year", month_num AS "MonthNum",
                   code_zs AS "Code_ZS", zone_sante AS "ZoneSante", antigene AS "Antigene",
                   dose_num AS "DoseNum", enfants_vaccines AS "EnfantsVaccines"
            FROM fact_vaccination
        """, self._get_engine())
        df_vacc.to_csv("export_powerbi/fact_vaccination.csv", index=False, encoding='utf-8-sig')
        print(f"📁 fact_vaccination exporté (Date, YearMonth, Code_ZS, Antigene, EnfantsVaccines…) ({len(df_vacc)} lignes)")

        # fact_reporting : colonnes pour page Power BI (YearMonth = clé vers dim_month, FOSA vers dim_fosa)
        df_reporting = pd.read_sql_query("""
            SELECT year_month AS "YearMonth", year AS "Year", month AS "Month", month_num AS "Month_num",
                   region AS "Region", district AS "District", as_aire AS "AS", fosa AS "FOSA",
                   rapport_attendu AS "Rapport_attendu", rapport_recu AS "Rapport_recu",
                   rapport_a_temps AS "Rapport_a_temps"
            FROM fact_reporting
        """, self._get_engine())
        df_reporting.to_csv("export_powerbi/fact_reporting.csv", index=False, encoding='utf-8-sig')
        print(f"📁 fact_reporting exporté (YearMonth, Year, Month, FOSA, Rapport_attendu/recu/a_temps) ({len(df_reporting)} lignes)")

        # dim_fosa : dimension dédiée reporting (slicers Region / District / AS / FOSA sans ambiguïté)
        df_fosa = pd.read_sql_query("""
            SELECT fosa AS "FOSA", as_aire AS "AS", district AS "District", region AS "Region"
            FROM dim_fosa ORDER BY region, district, as_aire
        """, self._get_engine())
        df_fosa.to_csv("export_powerbi/dim_fosa.csv", index=False, encoding='utf-8-sig')
        print(f"📁 dim_fosa exporté (FOSA, AS, District, Region) ({len(df_fosa)} lignes)")

        # dim_month : complète (trimestre, semestre, libellés)
        df_month = pd.read_sql_query("""
            SELECT year_month AS "YearMonth", year AS "Year", month AS "Month", month_num AS "Month_num",
                   trimestre AS "Trimestre", semestre AS "Semestre",
                   month_short AS "Month_short", year_month_label AS "Year_month_label"
            FROM dim_month ORDER BY year_month
        """, self._get_engine())
        df_month.to_csv("export_powerbi/dim_month.csv", index=False, encoding='utf-8-sig')
        print(f"📁 dim_month exporté (YearMonth, Year, Month, Trimestre, Semestre, libellés) ({len(df_month)} lignes)")

        # dim_semaine : semaines ISO (lundi = début)
        df_semaine = pd.read_sql_query("""
            SELECT year_week AS "Year_week", year AS "Year", week_num AS "Week_num",
                   week_start_date AS "Week_start_date", week_end_date AS "Week_end_date",
                   week_label AS "Week_label"
            FROM dim_semaine ORDER BY year_week
        """, self._get_engine())
        df_semaine.to_csv("export_powerbi/dim_semaine.csv", index=False, encoding='utf-8-sig')
        print(f"📁 dim_semaine exporté (Year_week, Year, Week_num, dates, Week_label) ({len(df_semaine)} lignes)")

        # dim_trimestre
        df_trimestre = pd.read_sql_query("""
            SELECT year_trimestre AS "Year_trimestre", year AS "Year",
                   trimestre AS "Trimestre", trimestre_num AS "Trimestre_num",
                   trimestre_label AS "Trimestre_label"
            FROM dim_trimestre ORDER BY year_trimestre
        """, self._get_engine())
        df_trimestre.to_csv("export_powerbi/dim_trimestre.csv", index=False, encoding='utf-8-sig')
        print(f"📁 dim_trimestre exporté (Year_trimestre, Year, Trimestre, Trimestre_num, Trimestre_label) ({len(df_trimestre)} lignes)")

        # dim_semestre
        df_semestre = pd.read_sql_query("""
            SELECT year_semestre AS "Year_semestre", year AS "Year",
                   semestre AS "Semestre", semestre_num AS "Semestre_num",
                   semestre_label AS "Semestre_label"
            FROM dim_semestre ORDER BY year_semestre
        """, self._get_engine())
        df_semestre.to_csv("export_powerbi/dim_semestre.csv", index=False, encoding='utf-8-sig')
        print(f"📁 dim_semestre exporté (Year_semestre, Year, Semestre, Semestre_num, Semestre_label) ({len(df_semestre)} lignes)")

        # dim_geo : colonnes pour slicers (Region, District, AS, FOSA, FOSA_FPP Oui/Non, Code_AS, Code_District)
        df_geo = pd.read_sql_query("""
            SELECT region AS "Region", district AS "District", as_aire AS "AS",
                   fosa AS "FOSA",
                   CASE WHEN est_fpp = 1 THEN 'Oui' ELSE 'Non' END AS "FOSA_FPP",
                   code_as AS "Code_AS", code_zs AS "Code_District"
            FROM dim_geo
        """, self._get_engine())
        df_geo.to_csv("export_powerbi/dim_geo.csv", index=False, encoding='utf-8-sig')
        print(f"📁 dim_geo exporté (Region, District, AS, FOSA, FOSA_FPP, Code_AS, Code_District) ({len(df_geo)} lignes)")

        # dim_year : Year + libellé
        df_year = pd.read_sql_query(
            'SELECT year AS "Year", year_label AS "Year_label" FROM dim_year ORDER BY year',
            self._get_engine()
        )
        df_year.to_csv("export_powerbi/dim_year.csv", index=False, encoding='utf-8-sig')
        print(f"📁 dim_year exporté (Year, Year_label) ({len(df_year)} lignes)")

        # fact_targets : colonnes pour matrice (LiveBirths, 0-11, 12-23, 12-59, 0-59, Femmes enceintes)
        df_targets = pd.read_sql_query("""
            SELECT year AS "Year", code_as AS "AS",
                   livebirths AS "LiveBirths", target_0_11 AS "Target_0_11",
                   target_12_23 AS "Target_12_23", target_12_59 AS "Target_12_59",
                   target_0_59 AS "Target_0_59", expected_pregnant_women AS "ExpectedPregnantWomen"
            FROM fact_targets
        """, self._get_engine())
        df_targets.to_csv("export_powerbi/fact_targets.csv", index=False, encoding='utf-8-sig')
        print(f"📁 fact_targets exporté (Year, AS, LiveBirths, Target_0_11, …) ({len(df_targets)} lignes)")
    
    def run_all(self):
        """Exécute l'ensemble du processus"""
        print("🚀 DÉBUT DE L'IMPORTATION DES DONNÉES PEV RDC")
        print("=" * 60)
        
        # Étape 1: Création des tables
        self.create_tables()
        
        # Étape 2: Import des données
        self.import_dim_district()
        self.import_dim_geo()
        self.import_dim_fosa()
        self.import_dim_periode()
        self.import_dim_year()
        self.import_dim_trimestre()
        self.import_dim_semestre()
        self.import_dim_month()
        self.import_dim_semaine()
        self.import_dim_antigene()
        self.import_fact_cibles()
        self.import_fact_targets()
        self.import_fact_vaccination()
        self.import_fact_logistique()
        self.import_fact_reporting()
        
        # Étape 3: Vérification
        self.verify_data()
        
        # Étape 4: Export pour Power BI
        self.export_to_csv()
        
        print("\n" + "=" * 60)
        print("✅ IMPORTATION TERMINÉE AVEC SUCCÈS!")
        print("\n📁 Les fichiers CSV pour Power BI sont dans le dossier 'export_powerbi/'")
        print("\n🔗 Connexions disponibles:")
        print("   - PostgreSQL: localhost:5432 (pev_rdc)")
        print("   - PgAdmin: http://localhost:5050 (admin@pev.rdc / admin123)")
    
    def close(self):
        """Ferme la connexion à la base de données"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("🔌 Connexion à la base de données fermée")

def main():
    """Fonction principale"""
    print("🔧 CONFIGURATION DE LA BASE DE DONNÉES PEV RDC")
    print("=" * 60)
    
    # Vérifier que Docker est en cours d'exécution (compatible Windows/Linux/macOS)
    try:
        result = subprocess.run(
            ["docker", "ps"],
            capture_output=True,
            timeout=10,
        )
        if result.returncode != 0:
            print("⚠️  Docker n'est pas en cours d'exécution!")
            print("   Lancez Docker Desktop puis réessayez")
            sys.exit(1)
    except (FileNotFoundError, subprocess.TimeoutExpired):
        print("⚠️  Docker n'est pas en cours d'exécution!")
        print("   Lancez Docker Desktop puis réessayez")
        sys.exit(1)
    
    # Créer l'arborescence des dossiers (ne pas écraser 01-create-tables.sql)
    os.makedirs("init-scripts", exist_ok=True)
    
    # Lancer Docker Compose
    print("\n🐳 Lancement de Docker Compose...")
    os.system("docker-compose down")  # Nettoyer les anciennes instances
    os.system("docker-compose up -d")
    
    # Attendre que PostgreSQL soit prêt
    print("\n⏳ Attente du démarrage de PostgreSQL (15 secondes)...")
    import time
    time.sleep(15)
    
    # Importer les données
    try:
        importer = PEVDatabaseImporter(DB_CONFIG)
        importer.run_all()
        importer.close()
    except Exception as e:
        print(f"❌ Erreur critique: {e}")
        sys.exit(1)
    
    print("\n🎯 PROCHAINES ÉTAPES:")
    print("1. Connectez Power BI à PostgreSQL:")
    print("   - Serveur: localhost")
    print("   - Base de données: pev_rdc")
    print("   - Utilisateur: pev_admin")
    print("   - Mot de passe: P@ssw0rdPEV2024")
    print("\n2. OU importez les fichiers CSV du dossier 'export_powerbi/'")
    print("\n3. Configurez les relations comme décrit dans le modèle")

if __name__ == "__main__":
    main()