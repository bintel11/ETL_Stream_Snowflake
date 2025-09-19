from app.connectors.s3_client import S3Client
from app.connectors.snowflake_client import SnowflakeClient
from app.clients.http_client import HttpClient


class ETLPipeline:
    def __init__(self):
        self.http = HttpClient(base_url="http://dummy.local")
        self.snowflake = SnowflakeClient(connect=False)
        self.s3 = S3Client()

    def run(self) -> bool:
        """Run the ETL pipeline."""
        # Step 1: extract
        data = self.http.get("/data")

        # Step 2: load (mocked in tests)
        self.snowflake.load_data(data)

        # Step 3: merge/upsert (mocked in tests)
        self.snowflake.merge_data()

        return True
