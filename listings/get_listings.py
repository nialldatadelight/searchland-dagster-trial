import os
import re
import urllib.parse
from datetime import datetime, timezone

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dagster import (
    AssetKey,
    AssetMaterialization,
    DynamicOut,
    DynamicOutput,
    OpExecutionContext,
    asset,
    get_dagster_logger,
    job,
    make_values_resource,
    op,
)
from dagster.core.events import DagsterEventType
from dagster.core.storage.event_log.base import EventRecordsFilter

from .io_managers import bigquery_pandas_io_manager

# TODO: Parameterise
BASE_PATH = os.getcwd()


def clean_url(url):
    return re.sub(r"[^A-Za-z0-9_]", "_", url)


def html_asset_key(url: str):
    return f"html_{url}"


def store_html(html: str, url: str):
    dirname = os.path.join(BASE_PATH, "media")
    os.makedirs(dirname, exist_ok=True)

    filename = os.path.join(dirname, f"{clean_url(url)}.html")

    with open(filename, "w") as f:
        f.write(html)
    return filename


def get_html(context: OpExecutionContext, url: str):
    resp = requests.get(url)
    filename = store_html(resp.text, url)
    context.log_event(
        AssetMaterialization(
            asset_key=AssetKey(html_asset_key(url)),
            description="Persisted html to local filesystem storage",
            metadata={
                "filename": filename,
            },
        )
    )
    return resp.text


def get_linked_urls(html: str) -> list[str]:
    log = get_dagster_logger()
    soup = BeautifulSoup(html, "lxml")

    # Get links
    categories = soup.find_all("a", class_=["category-link", "subcategory-link"])
    urls = []
    for category in categories:
        # TODO: Parameterise hostname
        urls.append(urllib.parse.urljoin("https://webscraper.io", category["href"]))

    log.info("Found %s urls", len(urls))

    return list(set(urls))


@op(required_resource_keys={"start_url"}, out=DynamicOut(str))
def get_all_urls(context: OpExecutionContext) -> list[str]:
    log = get_dagster_logger()

    to_parse_urls = [context.resources.start_url]

    # map for O(1) lookup
    parsed_urls = {}
    while True:
        """
        For each
        """
        if not len(to_parse_urls):
            break

        for _url in to_parse_urls:
            yield DynamicOutput(_url, mapping_key=clean_url(_url))

        log.info("Retrieving linked pages for %s URLs...", len(to_parse_urls))

        _urls = []
        for _url in to_parse_urls:
            _urls += get_linked_urls(get_html(context, _url))
            parsed_urls[_url] = 1

        to_parse_urls = [_url for _url in _urls if not parsed_urls.get(_url)]


@asset(io_manager_key="bq_io_manager", required_resource_keys={"bq_table_name"})
def ensure_bq_destination_table_exists():
    # As listings are retrieved and ingested in parallel, multiple jobs may attempt to
    # create the dataset concurrently, leading to conflicts. Could handle these
    # conflicts and retry, but ensure_bq_destination_table_exists is more explicit.
    df = pd.DataFrame(
        columns=[
            "url",
            "title",
            "currency",
            "price",
            "description",
            "run_id",
            "created",
            "run_started_at",
        ]
    ).astype(
        {
            "url": str,
            "title": str,
            "currency": str,
            "price": float,
            "description": str,
            "run_id": str,
            "created": "datetime64[ns]",
            "run_started_at": "datetime64[ns]",
        }
    )
    return df


@asset(io_manager_key="bq_io_manager", required_resource_keys={"bq_table_name"})
def listings(context: OpExecutionContext, url: str) -> pd.DataFrame:
    log = get_dagster_logger()

    # Retrieve stored html
    html_filename = (
        context.instance.get_event_records(
            EventRecordsFilter(
                asset_key=AssetKey(html_asset_key(url)),
                event_type=DagsterEventType.ASSET_MATERIALIZATION,
            ),
            limit=1,
        )[0]
        # There has to be an easier way to access the asset...
        .event_log_entry.dagster_event.event_specific_data.materialization.metadata[
            "filename"
        ].text
    )
    with open(html_filename, "r") as f:
        html = f.read()

    # Parse html
    soup = BeautifulSoup(html, "lxml")

    # Get listings
    products = soup.find_all("div", class_="thumbnail")
    listing_records = []
    now = datetime.now(timezone.utc)
    run_started_at = datetime.fromtimestamp(
        context.instance.get_run_stats(context.run_id).start_time
    )
    for product in products:
        title = product.find("a", class_="title").text
        price = product.find("h4", class_="price").text
        price_part = re.search(r"([\D]+)([\d,]+)", price)
        description = product.find("p", class_="description").text
        listing_records.append(
            {
                "url": url,
                "title": title,
                "currency": price_part.group(1),
                "price": float(price_part.group(2)),
                "description": description,
                "run_id": context.run.run_id,
                "created": now,
                "run_started_at": run_started_at,
            }
        )

    log.info("Found %s listings", len(listing_records))

    return pd.DataFrame.from_records(listing_records)


@job(
    resource_defs={
        "start_url": make_values_resource(),
        "bq_table_name": make_values_resource(),
        "bq_io_manager": bigquery_pandas_io_manager.configured(
            {
                "credentials": {"env": "BIGQUERY_SERVICE_ACCOUNT_CREDENTIALS"},
                "project_id": {"env": "BIGQUERY_PROJECT_ID"},
            }
        ),
    }
)
def get_listings():
    urls = get_all_urls()
    ensure_bq_destination_table_exists()
    urls.map(listings)
