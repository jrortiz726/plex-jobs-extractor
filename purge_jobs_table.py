from dotenv import load_dotenv
from cognite.client import CogniteClient
from cognite.client.config import ClientConfig
from cognite.client.credentials import OAuthClientCredentials
import os

load_dotenv()

creds = OAuthClientCredentials(
    token_url=os.environ["CDF_TOKEN_URL"],
    client_id=os.environ["CDF_CLIENT_ID"],
    client_secret=os.environ["CDF_CLIENT_SECRET"],
    scopes=["user_impersonation"],
)

client = CogniteClient(
    ClientConfig(
        client_name="plex-raw-reset",
        base_url=os.environ["CDF_HOST"],
        project=os.environ["CDF_PROJECT"],
        credentials=creds,
    )
)

rows = client.raw.rows.list("plex_raw", "jobs", limit=-1)
if rows:
    client.raw.rows.delete("plex_raw", "jobs", [row.key for row in rows])
    print(f"Deleted {len(rows)} rows from plex_raw.jobs")
else:
    print("No rows to delete.")