
from elasticsearch import Elasticsearch, helpers
import json
from tqdm import tqdm
import os
from dotenv import load_dotenv

from flask import Flask, request, jsonify
from flask_cors import CORS
import requests

load_dotenv() # load local .env file

app = Flask(__name__)
CORS(
    app,
    resources={r"/*": {"origins": [
        "http://localhost:8081"
    ]}},
)

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

# -----------------------------
# SEARCH INDEX BY KEYWORD
# -----------------------------
def search_keyword(index, keyword, size, page):
    query = {
        "query": {
            "match": {
                "text": {
                    "query": keyword,
                    "operator": "and"  # requires all words to appear
                }
            }
        },
        "from": page * size - size,
        "size": size
    }

    res = es.search(index=index, body=query)
    return res["hits"]["hits"]


#-----------------------------------------------------
# Get business info for business ids found in reviews
#-----------------------------------------------------
def search_business_id(reviews):
    business_ids = list({hit["_source"]["business_id"] for hit in reviews})
    query = {
        "query": {
            "terms": {
                "business_id.keyword": business_ids
            },
        },
        "size": len(business_ids), # must include size, otherwise defaults to 10
    }

    business_results = es.search(index=BUSINESS_INDEX, body=query)
    bus_id_mapping = {}
    for b in business_results["hits"]["hits"]:
        bus_id_mapping[b["_source"]["business_id"]] = b["_source"]
    return bus_id_mapping

#----------------------------------------
# Build review results with business info
#----------------------------------------
def build_review_results(reviews, business_lookup):
    results = []

    for hit in reviews:
        r = hit["_source"]
        business = business_lookup.get(r["business_id"], {})
        results.append({
            "text": r["text"],
            "stars": r["stars"],
            "date": r["date"],
            "business_name": business.get("name"),
            "business_city": business.get("city"),
            "business_state": business.get("state")
        })

    return results

#-------------------------------------
# SEARCH REVIEWS BASED ON KEYWORD
#-------------------------------------
@app.post("/search-reviews")
def searchkeyword():
    results = {}
    status = 500
    try:
        req_data = request.json
        query = req_data.get("query")
        page = req_data.get("page", 1)   # default to 1
        size = req_data.get("size", 10)  # default to 10

        reviews = search_keyword(REVIEW_INDEX, keyword=query, size=size, page=page)
        status = 200 if reviews else 404

        business_lookup = search_business_id(reviews)
        results = build_review_results(reviews, business_lookup) if reviews else {"message": "No results found."}
    except Exception as e:
        print(f"Error during search: {e}")
        results = {"error": str(e)}

    return results, status



#-------------------------------------
# SEARCH BUSINESS BASED ON LOCATION 
#-------------------------------------
def scrub_business_id(data):
    for item in data:
        print(f"Scrubbing business_id from item: {item}")
        if isinstance(item, dict):
            item.pop("business_id", None)
    return data

def search_location(query, page, size):
    query = {
        "from": page * size - size,
        "size": size,
        "query": {
            "multi_match": {
                "query": query,
                "fields": ["state", "postal_code", "city"]
            }
        },
        "sort": [
            {"review_count": {"order": "desc"}}
        ]
    }

    resp = es.search(index=BUSINESS_INDEX, body=query)
    return [hit["_source"] for hit in resp["hits"]["hits"]]

@app.post("/search-location")
def searchlocations():
    response = {}
    status = 500
    try:
        req_data = request.json
        query = req_data.get("query")
        page = req_data.get("page", 1)   # default to 1
        size = req_data.get("size", 10)  # default to 10
        data = search_location(query, page=page, size=size)
        status = 200 if data else 404
        if not data:
            print("No data found for the given location query.")
            return {"message": "No results found."}, status
        results = scrub_business_id(data)
        response["count"] = len(results)
        response["results"] = results

    except Exception as e:
        print(f"Error during search: {e}")
        response = {"error": str(e)}

    return response, status

if __name__ == "__main__":
    app.run(port=4000, debug=True)