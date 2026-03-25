import os
from main import run_pipeline                                                                                                                                                                                    
                                                                                                                                                                                                                   
final = run_pipeline(                                                                                                                                           source_type=os.getenv("SOURCE_TYPE", "csv"),
    source_config={"path": os.getenv("SOURCE_PATH", "datasets/coursea_data.csv")},
    target_db={
        "type": "sqlite",
        "path": os.getenv("TARGET_DB_PATH", "output/etl_output.db"),
        "table": os.getenv("TARGET_TABLE", "courses"),
        "if_exists": os.getenv("IF_EXISTS", "replace"),
    },
    user_instructions=os.getenv(
        "USER_INSTRUCTIONS",
        "drop rows where rating is null"
    ),
    connection_port=int(os.getenv("CONNECTION_PORT", "5000")),
)
