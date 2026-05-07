import io
import requests
import pandas as pd
from sqlalchemy.engine import Engine


def upload_df_to_blob(
    df: pd.DataFrame,
    engine: Engine,
    storage_account: str,
    container_name: str,
    blob_name: str,
    sas_token: str,
) -> None:
    """
    Upload a DataFrame as a CSV file to an Azure Blob Storage container.

    Args:
        df:               DataFrame to upload.
        engine:           SQLAlchemy Engine (available for any pre-upload DB ops).
        storage_account:  Azure storage account name.
        container_name:   Target container name.
        blob_name:        Destination blob path, e.g. "folder/file.csv".
        sas_token:        SAS token string (without leading "?").
    """
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_bytes = csv_buffer.getvalue().encode("utf-8")

    url = (
        f"https://{storage_account}.blob.core.windows.net"
        f"/{container_name}/{blob_name}?{sas_token}"
    )

    headers = {
        "x-ms-blob-type": "BlockBlob",
        "Content-Type": "text/csv; charset=utf-8",
        "Content-Length": str(len(csv_bytes)),
    }

    response = requests.put(url, data=csv_bytes, headers=headers, timeout=120)
    response.raise_for_status()
