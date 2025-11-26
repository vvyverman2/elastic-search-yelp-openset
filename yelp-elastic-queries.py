
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

# -----------------------------
#  ELASTIC CLOUD CLIENT
# -----------------------------
es = Elasticsearch(
    ELASTIC_URL,
    api_key=os.getenv("API_KEY"),
)

BUSINESS_INDEX = "yelp-business"
REVIEW_INDEX =  "yelp-review"
USER_INDEX = "yelp-user"
CHECKIN_INDEX  = "yelp-checkin"
TIP_INDEX =  "yelp-tip"

def search_keyword(index, keyword, size=10):
    query = {
        "query": {
            "match": {
                "text": {
                    "query": keyword,
                    "operator": "and"  # requires all words to appear
                }
            }
        },
        "size": size # first returned
    }

    res = es.search(index=index, body=query)
    return res["hits"]["hits"]

def search_reviews(keyword):
    reviews = search_keyword(REVIEW_INDEX, keyword)
    if not reviews:
        print("No reviews found.\n")
        return

    print(f"\nFound {len(reviews)} review(s) with keyword {keyword}:\n")
    for hit in reviews:
        src = hit["_source"]
        print("-------------------------------------------")
        print(f"- ⭐ {src['stars']} stars on {src['date']}")
        print(f"  {src['text'][:200]}...\n")  # print first 200 chars


def search_onestar_reviews(size=10):
    query = {
        "query": {
            "bool": {
                "must":  {"term": {"stars": 1.0}}
            }
        },
        "size": size,
    }

    resp = es.search(index=REVIEW_INDEX, body=query)
    reviews = [hit["_source"] for hit in resp["hits"]["hits"]]

    print("-------------------------------------------")
    print(f"\nFound {len(reviews)} 1-star review(s):\n")
    for r in reviews:
        print(r["text"][:120], "...\n")

def search_business_zip(zip, size=10):

    query = {
        "query": {
            "term": {
                "postal_code": zip
            }
        },
        "size": size,
        "sort": [
            {"review_count": {"order": "desc"}}
        ]
    }

    resp = es.search(index=BUSINESS_INDEX, body=query)
    results = [hit["_source"] for hit in resp["hits"]["hits"]]

    print("-------------------------------------------")
    print(f"\nBusinesses in {zip}\n")
    for business in results:
        print(f"{business['name']})")

def search_business_state(state, size=10):
    query = {
        "query": {
            "multi_match": {
                "query": state,
                "fields": ["state"]
            }
        },
        "size": size,
        "sort": [
            {"review_count": {"order": "desc"}}
        ]
    }

    resp = es.search(index=BUSINESS_INDEX, body=query)
    results = [hit["_source"] for hit in resp["hits"]["hits"]]

    print("-------------------------------------------")
    print(f"Top {len(results)} highest rated businesses in {state}\n")
    for business in results:
        print(f"{business['name']} in {business['city']}")
    print()


def main():
    search_reviews("beware")
    search_reviews("amazing")
    search_onestar_reviews()
    # search_business_zip(85705)
    search_business_state("AZ")
    search_business_state("CA")


main()