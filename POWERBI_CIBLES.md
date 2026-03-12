# Page Power BI « Cibles » – Guide

## 1. Modèle de données

### Dimensions
- **dim_geo** : Region, District, AS, FOSA, FOSA_FPP (Oui/Non ou 1/0), Code_AS, Code_District  
  (RDC : Region = Province, District = Zone de santé, AS = Aire de santé, FOSA = Formation sanitaire, FOSA_FPP = indicateur Oui/Non)
- **dim_year** : Year

### Fait
- **fact_targets** : granularité **AS + Year**  
  Colonnes : Year, AS (Code_AS), LiveBirths, Target_0_11, Target_12_23, Target_12_59, Target_0_59, ExpectedPregnantWomen

### Relations
- `dim_geo[Code_AS]` → `fact_targets[AS]` (1 → *)
- `dim_year[Year]` → `fact_targets[Year]` (1 → *)

Filtrage : dimension → fait.

---

## 2. Mesures DAX (pour le tableau)

```dax
LiveBirths =
SUM ( fact_targets[LiveBirths] )

0-11 mois =
SUM ( fact_targets[Target_0_11] )

12-23 mois =
SUM ( fact_targets[Target_12_23] )

12-59 mois =
SUM ( fact_targets[Target_12_59] )

0-59 mois =
SUM ( fact_targets[Target_0_59] )

Femmes enceintes attendues =
SUM ( fact_targets[ExpectedPregnantWomen] )
```

---

## 3. Mise en page de la page « Cibles »

### A) Slicers (5 filtres en haut)
1. **Region** → `dim_geo[Region]`
2. **Districts** → `dim_geo[District]`
3. **AS** → `dim_geo[AS]`
4. **Years** → `dim_year[Year]` (sélection unique : ON)
5. **FOSA_FPP** → `dim_geo[FOSA_FPP]` (Oui/Non)

Réglages : style **Dropdown** ; « Sélection unique » ON pour Years, OFF pour les autres.

### B) Matrice principale
- **Lignes** : `dim_geo[Region]` (optionnel : ajouter District puis AS en drilldown)
- **Valeurs** :
  - [LiveBirths]
  - [0-11 mois]
  - [12-23 mois]
  - [12-59 mois]
  - [0-59 mois]
  - [Femmes enceintes attendues]

Format : totaux et sous-totaux ON, grille légère, en-têtes en gras.

### C) Bloc « Consignes » (en bas)
- Forme (rectangle, bordure grise) + zone de texte :

**Consignes :**
- Assurez-vous que les cibles sont les bonnes
- Assurez-vous que toutes les AS s’y trouvent

*Make sure the targets are correct*  
*Make sure all Health area are there*

---

## 4. Fichiers CSV exportés

- `export_powerbi/dim_geo.csv` : Region, District, AS, FOSA, FOSA_FPP (Oui/Non), Code_AS, Code_District  
- `export_powerbi/dim_year.csv` : Year  
- `export_powerbi/fact_targets.csv` : Year, AS, LiveBirths, Target_0_11, Target_12_23, Target_12_59, Target_0_59, ExpectedPregnantWomen  

Dans Power BI : charger ces 3 fichiers, puis créer la relation `dim_geo[Code_AS]` ↔ `fact_targets[AS]` et `dim_year[Year]` ↔ `fact_targets[Year]`.
