# Searchland Dagster Trial - Listings Ingest

This repo contains two jobs:
1. `listings.get_listings.get_listings` retrieves listings from a `start_url` resource parameter and stores them in a BigQuery table, parameterised as `bq_table_name`.

2. `listings.join_listings.join_listings` (WIP) runs a job to join the listings within each table in the `listings` dataset. This is ran automatically after listings ingest.


## Process for the `get_listings` job

1. Retrieve all relevant pages that are linked from the start_url. The HTML for each page is stored in an asset.
2. Parse listings data via parallel parsing jobs which use the stored HTML.
3. Store listings in BigQuery via an IOManager.

This pattern ensures parsing logic can be updated without multiple requests to the website, to:
- reduce the risk of blocks;
- improve development speed (as devs only need to alter and re-run the parsing jobs);
- improve tracability (as the raw HTML is stored for later analysis).

## Running the DAGs

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
