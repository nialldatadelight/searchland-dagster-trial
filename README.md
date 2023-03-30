# Searchland Dagster Trial - Listings Ingest

This repo contains two jobs:
    1. `listings.get_listings.get_listings` retrieves listings from a `start_url` resource parameter and stores them in a BigQuery table, parameterised as `bq_table_name`.
    2. `listings.join_listings.join_listings` (WIP) runs a job to join the listings within each table in the `listings` dataset. This is ran automatically after listings ingest.

To run:
``` bash
export BIGQUERY_PROJECT_ID=[GCP project ID]
export BIGQUERY_SERVICE_ACCOUNT_CREDENTIALS=`cat [service account JSON location]`
dagster dev -m listings
```

Example config for `get_listings`:
``` yaml
resources:
  start_url:
    config: https://webscraper.io/test-sites/e-commerce/allinone/phones

  bq_table_name:
    config: ecom
```

## To-dos
    - [ ] `join_listings` job, including DBT setup.

## `get_listings` explanation

`get_listings` first retrieves all relevant pages that are linked from the start_url. The HTML for each page is stored in an asset and used in parallel parsing jobs next. This pattern ensures parsing logic can be updated without multiple requests to the website, reducing the risk of blocks, improving development speed (as devs only need to alter and re-run the parsing jobs) and improving tracability (as the raw HTML is stored for later analysis).
