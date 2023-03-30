from dagster import Definitions

from .get_listings import get_listings
from .join_listings import listings_updated_sensor

defs = Definitions(
    jobs=[get_listings],
    sensors=[listings_updated_sensor],
)
