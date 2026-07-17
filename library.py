"""
lib/fetch_sharepoint_from_blob.py

Reusable function to fetch a SharePoint-sourced JSON file from
edwdatalakestorage for a given date, and return selected fields as a
flattened pandas DataFrame, ready for loading into MSSQL.

Requires:
    pip install pandas azure-identity azure-storage-blob --break-system-packages
"""

import json
import os
from datetime import datetime

import pandas as pd
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient

STORAGE_ACCOUNT = "edwdatalakestorage"
ACCOUNT_URL = f"https://{STORAGE_ACCOUNT}.blob.core.windows.net"


def fetch_sharepoint_json(
    client_secret: str,
    blob_prefix: str,
    data_date: datetime,
    fields: list[str],
    filename_prefix: str,
) -> pd.DataFrame:
    """
    Fetch a SharePoint list's JSON file for a given date from
    edwdatalakestorage and return it as a DataFrame containing only the
    requested fields. If more than one file matches (e.g. an entity that
    isn't overwritten and gets multiple timestamped files per day), only
    the most recently written file is used.

    Args:
        client_secret: Service Principal client secret for this call.
            Tenant ID and Client ID are read internally from env vars
            AZURE_TENANT_ID / AZURE_CLIENT_ID, since the same Service
            Principal and tenant are used for every call.
        blob_prefix: Container plus path, up through (but not including)
            the date folders, e.g.
            "inbound/SharePoint/TechnologyDataCentral/ScorecardRespMgmt"
            The first path segment is treated as the container name
            ("inbound"); the remainder is the folder path within it.
        data_date: The date of data to pull. Used to build the
            /yyyy/mm/dd/ folder path, matching the Logic App's folder
            convention.
        fields: List of SharePoint field (column) names to extract from
            each item, in the order they should appear in the returned
            DataFrame. The SharePoint item id is always included as
            "_sp_item_id", regardless of what's passed here.
        filename_prefix: The start of the filename to match within that
            date's folder, e.g. "ScorecardRespMgmt". Does not need to
            include the date suffix -- matching is done with
            "starts with", so this works whether the actual file is named
            "ScorecardRespMgmt_20260716.json" (date only) or
            "ScorecardRespMgmt_20260716T103000.json" (date + time).

    Returns:
        pd.DataFrame: one row per SharePoint list item, containing
        "_sp_item_id" plus only the requested fields. Returns an empty
        DataFrame (same columns, zero rows) if the matched file exists
        but has no items -- this is not treated as an error.

    Raises:
        ValueError: if blob_prefix doesn't contain both a container and
            at least one folder segment.
        FileNotFoundError: if no blob matches filename_prefix within
            that date's folder.

    Note:
        If a requested field is itself a complex SharePoint column type
        (Person, Lookup, Managed Metadata), Graph returns it as a nested
        dict/list rather than a scalar value. This function does not
        auto-flatten those -- the raw structure will land in that
        column's cells as-is. Handle those specific fields separately
        if needed.
    """
    container_name, _, path_prefix = blob_prefix.strip("/").partition("/")
    if not container_name or not path_prefix:
        raise ValueError(
            "blob_prefix must include both a container and a folder path, "
            f"e.g. 'inbound/SharePoint/.../ScorecardRespMgmt'. Got: {blob_prefix!r}"
        )

    date_folder = f"{path_prefix}/{data_date:%Y}/{data_date:%m}/{data_date:%d}/"
    search_prefix = f"{date_folder}{filename_prefix}"

    tenant_id = os.environ["AZURE_TENANT_ID"]
    client_id = os.environ["AZURE_CLIENT_ID"]

    credential = ClientSecretCredential(tenant_id, client_id, client_secret)
    blob_service_client = BlobServiceClient(ACCOUNT_URL, credential=credential)
    container_client = blob_service_client.get_container_client(container_name)

    # List every blob in that date folder matching the filename prefix,
    # then pick the most recently written one by actual blob metadata --
    # not by trying to parse a timestamp out of the filename.
    matching_blobs = list(container_client.list_blobs(name_starts_with=search_prefix))
    if not matching_blobs:
        raise FileNotFoundError(
            f"No blob found matching: {container_name}/{search_prefix}*"
        )

    latest_blob = max(matching_blobs, key=lambda b: b.last_modified)
    blob_client = container_client.get_blob_client(latest_blob.name)

    raw_bytes = blob_client.download_blob().readall()
    items = json.loads(raw_bytes)

    columns = ["_sp_item_id"] + fields

    if not items:
        return pd.DataFrame(columns=columns)

    records = []
    for item in items:
        item_fields = item.get("fields", {})
        row = {"_sp_item_id": item.get("id")}
        for field_name in fields:
            row[field_name] = item_fields.get(field_name)
        records.append(row)

    return pd.DataFrame(records, columns=columns)
