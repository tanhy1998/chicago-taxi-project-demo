# Chicago Taxi Trips - Data Analytics Engineering Pipeline

Tech stack : BigQuery + Dataform + Data Studio (Looker).
Data Studio (Looker): https://datastudio.google.com/reporting/972a4ccf-c369-4d12-bee1-40a34ebce62a
Pipeline: fully automated via a Dataform release configuration + daily scheduled workflow.

---

## Architecture overview

Medallion architure layering with different types of SQL materialisations in dataform:

| Layer  | Models                                              | Type        | Purpose                                                                                                                                                                                                                                                                                                                      |
| ------ | --------------------------------------------------- | ----------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Bronze | `src_taxi_trips`                                  | declaration | To avoid replicate a full 75GB Chicago taxi dataset just to fulfill the Medallion architure.                                                                                                                                                                                                                                 |
| Silver | `stg_taxi_trips`                                  | view        | To perform standardisation transformation such as Dedupe, data type, normalisation, quality flag for a cleansed table to be used for gold/reporting layer                                                                                                                                                                    |
| Silver | `dim_date`, `dim_company`, `dim_payment_type` | table       | -A dimension calendar table spans from 2013-2023 with US holiday logic<br />- A dimension company table to allow quick analysis to identify involved company without scanning the whole big datasets.<br /> - A 11 rows dimension payment table to know the unique payment_type and whether is it tips reliably payment type |
| Silver | `fct_trips`                                       | incremental | Partitioned by`trip_date` in year (for demo purpose, real production should be in day for better performance), clustered by `taxi_id`, MERGE on `unique_key`to achieve idempotent                                                                                                                                     |
| Silver | `int_shifts`                                      | table       | An intermediate table to calculate the shift for Q2                                                                                                                                                                                                                                                                          |
| Gold   | `rpt_*`                                           | table       | Tables feed into data studio only whereby One thin model per question                                                                                                                                                                                                                                                        |

## Assumptions & data quality

1. **"Last 3 months" = last 3 months of available data**, tied to `MAX(trip_date)` , as this dataset was only up to 2023, so using CURRENT_DATE-relative windows is not suitable here.
2. **Keep-and-flag quality policy**: rows failing rules get `is_valid_trip = FALSE` in staging (auditable) and are excluded from the fact. Rules:
   * dedupe on`unique_key`.
   * trip end timestamp ≥ trip start timestamp,
   * trip duration ≤ 24h,
   * distance ≤ 500 mi,
   * fare ≤ $5,000,
   * no negative money fields,
3. **Testing**: Dataform assertions on every table (`uniqueKey`, `nonNull`, `rowConditions`) .Critical assertions prevent downstream builds via `dependOnDependencyAssertions` to ensure a failure in the fact stops reporting rather than silently double ingesting into the reporting layer;

---

## Q1 Top 100 taxis by tips (last 3 months)

**Question understanding:**
"Tips earned" = recorded tips summed per `taxi_id` over the last available 3 months of data, ranked with `RANK()` so ties share a position.

**Table:** Gold/Report Layer -  `rpt_top_tip_earners`.

**Assumptions**

- The calculation window was based on the `MAX(trip_date) with interval of 3 months`.
- A secondary metric, `card_tip_rate` (tips ÷ fares over card trips only), is provided as a like-for-like comparison.

**Finding.** `Top-100 taxis ID earned $400k in recorded tips for the time range window of 1st Oct - 31st Dec of 2023`

---

## Q2 Top 100 taxis regularly working long shifts

**Question understanding:**

This question looks for taxi IDs that often work long hours, with:

* short rest time between trips (less than 8 hours)
* long continuous working periods
* repeated long shifts over time

Few defined rules:

| Term           | Definition                                                                              | Explanation                                                        |
| -------------- | --------------------------------------------------------------------------------------- | ------------------------------------------------------------------ |
| Shift          | Trips where the gap between one drop-off and next pick-up is**less than 8 hours** | 8 hours is treated as a normal rest/sleep break                    |
| Shift duration | Time from first trip start to last trip end in a shift                                  | Shows total working time                                           |
| Long shift     | A shift between**12 to 30 hours**                                                 | 12+ hours means long work but 30+ hours is abnormal (special case) |
| "Regularly"    | At least**25% of shifts are long**, at least **20 shifts total** , fleet shift less than **25**    | Avoids small sample bias                                           |
| Ranking        | Total hours spent in long shifts                                                        | Focus on total workload, not just count                            |

**Method:**

We group trips into shifts using time gaps (LAG):

* If the gap between trips is  **8 hours or more** , we start a new shift
* Otherwise, trips are grouped into the same shift
* Then we calculate shift start, end, and total duration

After that:

* We label shifts as normal, long, or continuous operation
* We filter taxis that meet the “regular long shift” rule
* We rank them by total hours spent in long shifts

**Special case - continuous operation:**

Some taxis show shifts longer than 300 hours. This is not one driver working non-stop.

It could be:

* the taxi is used by multiple drivers
* the vehicle is running almost all the time

To avoid data noise:

* mark shifts **longer than 30 hours as continuous operation**
* remove them from the overworker ranking
* still keep them for reference

**Limitations:**

* `taxi_id` is a vehicle, not a driver
* timestamps are rounded to 15 minutes, so durations are not exact
* the rules (8h / 12h / 30h) are assumptions, but results are stable
* we cannot see if the driver actually rested inside the vehicle activity window

**Finding:** `About 1.9 millions trips hour in long shifts for the top 100, longest single-driver shift 977 h.(This is the one make me have more thoughts)"

---

## Q3 Do public holidays significantly affect trips?

**Question understanding:**
This question compares taxi trip counts on US federal holidays against normal days, to see whether holidays cause trips to increase or decrease.

**Methodology:**

Two main factors need to be controlled so the comparison is fair:

1. **Weekday effect**

   Taxi demand follows a weekly pattern. For example, weekdays are usually busier because of work, while weekends are different in behaviour. Since holidays could be fall on specific days of the week (e.g. New Year’s Day might be on a Monday), comparing them with a simple overall average would be misleading.
2. **Time / seasonal effect**

   Trip volume also changes over time. For example, earlier years (like 2020) may have lower demand compared to later years due to external events like COVID-19.

To reduce these effects, the baseline is calculated as the **average trips for the same weekday within ±4 weeks, excluding other holidays**. This is implemented using the following window function:

`AVG(IF(is_holiday, NULL, n_trips)) OVER (PARTITION BY day_of_week ORDER BY date_day ROWS BETWEEN 4 PRECEDING AND 4 FOLLOWING)`

- `PARTITION BY day_of_week` compares only the same weekday to remove the weekday effect.
- The ±4-week window compares nearby dates to reduce the impact of  `Time / seasonal effect` trends.
- Holiday records are excluded from the average so that one holiday does not affect the baseline of another holiday.

This works because `dim_date` is a full calendar table, so every day exists even if trip count is 0.

**Tables:**

- `dim_date`- Used to generate holiday flags based on US federal holiday rules
- `rpt_holiday_impact` - Final reporting table used to measure holiday vs normal-day difference

**Assumptions:**

- Only US federal holidays are included because the dataset is from Chicago.

**Finding:**

`Overall, holidays tend to slightly reduce taxi trips compared to normal days in the 10 years. However, the impact is not consistent across all years. In some years (e.g. 2013 and 2017 New Year’s Day), trips increased by around 20%, suggesting possible behavioural or external demand changes in those periods. In later years, holiday demand tends to drop more consistently`

---

## Bonus : Two actionable insights

**1. Demand pattern by hour and day (`rpt_demand_by_hour_dow`).**

This analysis looks at how taxi demand changes by  **hour of day and day of week** , based on trip counts and revenue over the last 12 months.

**Business value:**

This helps identify when demand is high or low, so drivers can be better planned. For example, driver shifts can be aligned with peak hours to reduce waiting time and increase completed trips. The revenue layer also shows which busy hours are actually more profitable, not just busy.

**Finding:**
Peak demand occurs around working hours (8am–6pm), with highest revenue concentrated on weekday evenings.”>

**2. Pickup-area economics (`rpt_revenue_by_pickup_area`).**

This analysis compares different pickup areas based on how much revenue they generate, using:

* fare per mile
* tip rate (card payments only)

**Business value:**

This helps identify high-value locations where drivers can position themselves to earn more. For example, airports or busy commercial districts usually generate higher fares and better tips, making them more attractive for driver allocation strategies.

**Finding:**
Higher-value pickup areas are generally associated with urban commercial zones and locations with airport connectivity. For example, Community Area 62 (West Elsdon) has the highest average fare per mile, benefits from Midway Airport-related trips while
Community Area 28 (Near West Side) shows stronger earning potential.

---

## Running the pipeline

1. Set `defaultProject` in `workflow_settings.yaml` (location must stay `US` because the dataset is in the US multi-region).
2. Link this repo to a Dataform repository and run all actions once to build the full pipeline.
3. After that, the pipeline runs automatically:

* Triggered from the `main` branch
* Scheduled daily
* Only processes new data (incremental runs with MERGE)

1. Looker Studio connects only to the `reporting` dataset, using the owner’s credentials, and the dashboard is shared via public link.

## Cost & performance notes

* Source data is not copied; the staging layer is a view that is only read when needed.
* The fact table is partitioned and clustered, and uses a 90-day `updatePartitionFilter` so each run only processes recent data.
* All reporting queries filter by `trip_date`, so only relevant partitions are scanned.
* Gold tables are small, so dashboard queries are very cheap and fast.
