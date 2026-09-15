# Common grouping-variable column names by modeling frame

These are BlueLabs's most frequently used demographic/behavioral
breakdowns, by modeling frame family. When a user says they're using the
political, commercial, or L2-commercial modeling frame for their base
table, offer these as a starting point instead of asking them to type out
every column name from scratch — confirm the frame, then confirm (or let
them override) each mapping you're about to use, rather than assuming
silently. These are common defaults, not guarantees: always let the user
add, drop, or rename any breakdown, and never treat this list as
exhaustive — a job may need columns not listed here.

The order below (age, female, ethnicity, education modeled, education
area, income, urbanicity, region, party, is married, is homeowner, has
child) is also the conventional row order BlueLabs universe tabs use —
offer breakdowns in this order when a user hasn't specified their own
order, but still confirm with them per the "Grouping variables" interview
step (order controls output row order, and that's their call, not a
default to silently apply).

## Political modeling frame (`pol`)

| Breakdown        | Column               |
|-------------------|-----------------------|
| Age                | `age_bucket_full`     |
| Female             | `tsmart_female`        |
| Ethnicity          | `combined_ethnicity_4way` |
| Education Modeled  | `education_modeled`    |
| Education Area     | `education_area_type`  |
| Income             | `income_bucket_full`   |
| Urbanicity         | `reg_nyt_urbanicity`   |
| Region             | `reg_region`           |
| Party              | `combined_party`       |
| Is Married         | `is_married`           |
| Is Homeowner       | `is_home_owner`        |
| Has Child          | `has_child`            |

## Commercial modeling frame (`comm`)

| Breakdown        | Column                        |
|-------------------|---------------------------------|
| Age                | `comm_age_bucket_full`          |
| Female             | `comm_tsmart_female`             |
| Ethnicity          | `combined_ethnicity_4way`        |
| Education Modeled  | `comm_model_education_3way`       |
| Education Area     | `education_area_type`             |
| Income             | `income_bucket_full`              |
| Urbanicity         | `tsmart_nyt_urbanicity`           |
| Region             | `tsmart_region`                    |
| Party              | `comm_combined_party` (confirm with the user — not always used for `comm`) |
| Is Married         | `is_married`                       |
| Is Homeowner       | `is_home_owner`                    |

Has Child isn't used for the commercial frame.

## L2 commercial modeling frame (`l2_comm`)

| Breakdown        | Column                     |
|-------------------|------------------------------|
| Age                | `age_bucket`                 |
| Female             | `comm_gender_female`          |
| Ethnicity          | `combined_ethnicity_4way`     |
| Education Modeled  | `comm_model_education_3way`    |
| Education Area     | `education_area_type`          |
| Income             | `income_bucket_full`           |
| Urbanicity         | `nyt_urbanicity`               |
| Region             | `natl_region`                  |
| Party              | `comm_combined_party` (confirm with the user — not always used for `l2_comm`) |
| Is Married         | `is_married`                   |
| Is Homeowner       | `is_home_owner`                |

Has Child isn't used for the L2 commercial frame.

## Using this during the interview

1. Ask which modeling frame family the base table is (political,
   commercial, L2 commercial, or none of these/something bespoke).
2. If one of the three above, offer the matching table's breakdowns as
   defaults for any grouping variable the user names in general terms
   ("age", "education") rather than an exact column.
3. Still ask for the grouping variables the user wants (this file doesn't
   replace that question) — it only saves them from having to know or
   type the exact column name for the handful of breakdowns listed here.
4. If the user names a breakdown not in this table, or is using a
   modeling frame not listed, ask for the column name directly as usual.
5. If a user's own project has previously used a different column name
   for one of these breakdowns (e.g. a project-specific override), trust
   what they tell you over this table.
