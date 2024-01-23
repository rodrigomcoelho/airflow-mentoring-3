from airflow.datasets import Dataset

DATASET_ROOT = "/opt/airflow/data"

# LINX_DATASET_RAW = Dataset(uri=f"{DATASET_ROOT}/raw_linx.csv")

LINX_DATASET_RAW = Dataset(uri="rawLinx")
LINX_DATASET_TRUSTED = Dataset(uri="trustedLinx")
POKEAPI_DATASET_TRUSTED = Dataset(uri="trustedPokeApi")
SCOOBYDB_DATASET_TRUSTED = Dataset(uri="trustedScoobydb")
