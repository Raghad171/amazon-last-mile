# Amazon Last Mile Power BI Dashboard

Canvas: 16:9, 1280 × 720. Use the supplied dark theme and one background PNG per page.

## Setup

1. In Power BI Desktop, choose **View > Themes > Browse for themes** and select `Amazon_Last_Mile_Theme.json`.
2. Set each page to **16:9**.
3. Open **Format page > Canvas background > Image**, select the matching `background-*.png`, choose **Fit**, and set transparency to 0%.
4. Place visuals inside the outlined panels. Turn each visual's background and border off so the page background supplies the cards.
5. Use **Import** mode for the four Snowflake views unless live querying is a stated requirement.

## Page 1: Executive Overview

Primary source: `VW_DAILY_EXECUTIVE`

| Area | Visual | Suggested fields |
|---|---|---|
| KPI row | Cards | `TOTAL_ROUTES`, `TOTAL_PACKAGES`, `DELIVERED_PACKAGES`, `DELIVERY_SUCCESS_RATE`, `AVG_CAPACITY_UTILIZATION_PCT` |
| Delivery trend | Line chart | Date on X, `DELIVERY_SUCCESS_RATE` on Y |
| Route score mix | 100% stacked bar | `HIGH_SCORE_ROUTES`, `MEDIUM_SCORE_ROUTES`, `LOW_SCORE_ROUTES` |
| Package volume | Column chart | Date on X, `TOTAL_PACKAGES` on Y |
| Summary | Cards or multi-row card | `ATTEMPTED_PACKAGES`, `TOTAL_STOPS`, `AVG_PACKAGES_PER_DROPOFF` |

## Page 2: Station Performance

Use `VW_STATION_SUMMARY` for overall ranking and `VW_STATION_PERFORMANCE` for the date trend.

| Area | Visual | Suggested fields |
|---|---|---|
| Station ranking | Horizontal bar | Station on Y, packages or routes on X; tooltip with success rate |
| Capacity vs success | Scatter | Capacity on X, success rate on Y, station as Details, packages as Size |
| Station trend | Line chart | Date on X, success rate on Y, station in Legend |
| Selected station | Cards | Routes, packages, success rate and capacity |

## Page 3: Route Details

Primary source: `VW_ROUTE_PERFORMANCE`

| Area | Visual | Suggested fields |
|---|---|---|
| Route distribution | Column chart | Route ID on X, packages on Y; capacity in tooltip |
| Exception breakdown | Bar chart | Create categories for low capacity, attempts, low score and unusually high stops |
| Route table | Table | Route ID, date, station, stops, packages, success rate, capacity and score |

Add slicers for date, station and route score. Apply conditional formatting to score and low success values.

## Page 4: Data Quality

Power BI needs a small quality-results table for this page. Create it from the outputs of the Silver and Gold quality checks, or enter a temporary table while designing.

Recommended columns: `RUN_TIMESTAMP`, `LAYER`, `CHECK_NAME`, `STATUS`, `SOURCE_VALUE`, `TARGET_VALUE`, `DURATION_SECONDS`.

Use cards for overall status, null keys, duplicates and orphan records. Use a table for individual checks and cards for source-to-target reconciliation.

## Modeling rule

The views have different grains. Do not join them directly only because they share date or station fields. Prefer separate page-specific fact tables, shared Date and Station dimensions, and one-way relationships from dimensions to facts. This prevents duplicated totals.

## Validation totals

After loading the data, confirm these project totals before formatting the report:

- Routes: 6,112
- Packages: 1,457,175
- Delivered packages: 1,446,129
- Attempted packages: 11,014

If a column alias differs in Snowflake, use the matching business field from the view.
