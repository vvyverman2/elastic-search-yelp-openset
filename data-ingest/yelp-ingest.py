# -------------------------------------------
#  One time load script for files in /data
# -------------------------------------------

from elasticsearch import Elasticsearch, helpers
import json
from tqdm import tqdm
import os
from dotenv import load_dotenv

load_dotenv() # load local .env file

# -----------------------------
#  CONFIG FOR ELASTIC CLOUD
# -----------------------------
# values stored in .env
API_KEY = os.getenv("API_KEY")
ELASTIC_URL = os.getenv("ELASTIC_URL")
print(f"ELASTIC_URL: {ELASTIC_URL}\n\n")

DATA_FILES = [
    "yelp_academic_dataset_business.json",
    "yelp_academic_dataset_review.json",
    "yelp_academic_dataset_user.json",
    "yelp_academic_dataset_checkin.json",
    "yelp_academic_dataset_tip.json"
]

INDEX_NAMES = {
    "business": "yelp-business",
    "review": "yelp-review",
    "user": "yelp-user",
    "checkin": "yelp-checkin",
    "tip": "yelp-tip"
}

DOC_ID_KEY = {
    "yelp-business": "business_id",
    "yelp-review": "review_id",
    "yelp-user": "user_id",
    "yelp-checkin": "business_id",
    "yelp-tip": "business_id"
}

MAPPINGS = {
    "yelp-business": {
        "mappings": {
            "properties": {
                "text": {"type": "text"},
                "name": {"type": "text"},
                "stars": {"type": "float"},
                "date": {"type": "date"},
                "latitude": {"type": "float"},
                "longitude": {"type": "float"},
                "yelping_since": {"type": "date"}
            }
        }
    },
    "yelp-review": {
        "mappings": {
            "properties": {
                "user_id": {"type": "keyword"},
                "business_id": {"type": "keyword"},
                "stars": {"type": "float"},
                "useful": {"type": "integer"},
                "funny": {"type": "integer"},
                "cool": {"type": "integer"},
                "text": {"type": "text"},           # searchable content
                "date": {"type": "text"} # todo should be type date, but date data is malformed
            }
        }
    },
    "yelp-user": {
        "mappings": {
            "properties": {
                "user_id": {"type": "keyword"},
                "name": {"type": "text"},
                "review_count": {"type": "integer"},
                 "yelping_since": { "type": "date", "format": "yyyy-MM-dd HH:mm:ss" },
                "friends": {"type": "keyword"},    # list of friend IDs
                "useful": {"type": "integer"},
                "funny": {"type": "integer"},
                "cool": {"type": "integer"},
                "fans": {"type": "integer"},
                "average_stars": {"type": "float"},
            }
        }
    },
    "yelp-checkin": {
        "mappings": {
            "properties": {
                "business_id": {"type": "keyword"},
                "checkin_info": {"type": "object"}   # JSON object storing time/day counts
            }
        }
    },
    "yelp-tip": {
        "mappings": {
            "properties": {
                "text": {"type": "text"},
                "date": { "type": "date", "format": "yyyy-MM-dd HH:mm:ss" },
                "user_id": {"type": "keyword"},
                "business_id": {"type": "keyword"},
                "likes": {"type": "integer"}
            }
        }
    }
}

# -----------------------------
#  ELASTIC CLOUD CLIENT
# -----------------------------
es = Elasticsearch(
    ELASTIC_URL,
    api_key=os.getenv("API_KEY"),
)

# -----------------------------
#  CREATE INDICES
# -----------------------------
def create_indices():
    for idx in INDEX_NAMES.values():
        # skips creation if indicies already exist 
        if not es.indices.exists(index=idx):
            # print(f"\nmapping: {MAPPINGS[idx]}\n")
            es.indices.create(index=idx, body=MAPPINGS[idx])
            print(f"Created index: {idx}")

# -----------------------------
#  BULK LOADER
# -----------------------------
def generate_actions(filepath, index_name):
    print(f"DOC_ID_KEY[index_name]: {DOC_ID_KEY[index_name]}")
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            record = json.loads(line)
            doc_id = record.get(DOC_ID_KEY[index_name])
            yield {
                "_index": index_name,
                "_id": doc_id,
                "_source": record
            }

def bulk_load(filepath, index_name):
    print(f"\nIngesting {filepath} to {index_name}")
    response=[]
    try:
        response = helpers.bulk(es, generate_actions(filepath, index_name), chunk_size=2000, stats_only=False)
    except Exception as e:
        print(f"Error uploading {filepath}: {str(e)}")

    if len(response) > 1:
        print(f"Done ingesting {filepath}  {response[0]} docs and failed for {len(response[1])} docs")

# -----------------------------
#  MAIN
# -----------------------------
if __name__ == "__main__":
    print("Connecting to Elastic Cloud...")
    print(es.info())

    create_indices()

    for file in DATA_FILES:
        key = file.replace("yelp_academic_dataset_", "").replace(".json", "")
        index_name = INDEX_NAMES[key]
        bulk_load(f"data/{file}", index_name)

    print("\n✔ All Yelp data ingested into Elastic Cloud!")
