"""MongoDB collection helpers used by Airflow tasks and pipeline tests."""

from dataclasses import dataclass
from typing import Any, Dict, Iterable


@dataclass(frozen=True)
class MongoCollections:
    """Container for every Mongo collection touched by the ETL pipelines."""

    national_data: Any
    national_trends: Any
    national_series: Any
    regional_data: Any
    regional_trends: Any
    regional_series: Any
    regional_breakdown: Any
    provincial_data: Any
    provincial_trends: Any
    provincial_series: Any
    provincial_breakdown: Any
    vax_admins: Any
    vax_admins_summary: Any
    population: Any


def replace_collection(
    collection: Any,
    records: Iterable[Dict],
    *,
    ordered: bool = False,
    batch_size: int = 1000,
) -> int:
    """Drop a collection, insert records in batches, and return the inserted count."""
    items = list(records)
    collection.drop()
    if not items:
        return 0

    inserted_count = 0
    for start in range(0, len(items), batch_size):
        batch = items[start : start + batch_size]
        result = collection.insert_many(batch, ordered=ordered)
        inserted_count += len(result.inserted_ids)
    return inserted_count


def replace_one(collection: Any, record: dict) -> Any:
    """Drop a collection and insert a single replacement document."""
    collection.drop()
    return collection.insert_one(record).inserted_id


def collection_from_airflow_variable(mongo_hook: Any, variable: Any, name: str) -> Any:
    """Resolve an Airflow variable into a Mongo collection from a hook."""
    return mongo_hook.get_collection(variable.get(name))


def collections_from_airflow(mongo_hook: Any, variable: Any) -> MongoCollections:
    """Build the collection registry from Airflow variables."""
    return MongoCollections(
        national_data=collection_from_airflow_variable(
            mongo_hook, variable, "NATIONAL_DATA_COLLECTION"
        ),
        national_trends=collection_from_airflow_variable(
            mongo_hook, variable, "NATIONAL_TRENDS_COLLECTION"
        ),
        national_series=collection_from_airflow_variable(
            mongo_hook, variable, "NATIONAL_SERIES_COLLECTION"
        ),
        regional_data=collection_from_airflow_variable(
            mongo_hook, variable, "REGIONAL_DATA_COLLECTION"
        ),
        regional_trends=collection_from_airflow_variable(
            mongo_hook, variable, "REGIONAL_TRENDS_COLLECTION"
        ),
        regional_series=collection_from_airflow_variable(
            mongo_hook, variable, "REGIONAL_SERIES_COLLECTION"
        ),
        regional_breakdown=collection_from_airflow_variable(
            mongo_hook, variable, "REGIONAL_BREAKDOWN_COLLECTION"
        ),
        provincial_data=collection_from_airflow_variable(
            mongo_hook, variable, "PROVINCIAL_DATA_COLLECTION"
        ),
        provincial_trends=collection_from_airflow_variable(
            mongo_hook, variable, "PROVINCIAL_TRENDS_COLLECTION"
        ),
        provincial_series=collection_from_airflow_variable(
            mongo_hook, variable, "PROVINCIAL_SERIES_COLLECTION"
        ),
        provincial_breakdown=collection_from_airflow_variable(
            mongo_hook, variable, "PROVINCIAL_BREAKDOWN_COLLECTION"
        ),
        vax_admins=collection_from_airflow_variable(mongo_hook, variable, "VAX_ADMINS_COLLECTION"),
        vax_admins_summary=collection_from_airflow_variable(
            mongo_hook, variable, "VAX_ADMINS_SUMMARY_COLLECTION"
        ),
        population=collection_from_airflow_variable(mongo_hook, variable, "POP_COLLECTION"),
    )
