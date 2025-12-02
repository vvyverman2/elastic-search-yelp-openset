#!/bin/zsh
curl -X POST "http://localhost:4000/search-reviews" \
  -H "Content-Type: application/json" \
  -d '{
        "query": "coffee"
      }'

# curl -X POST "http://localhost:4000/search-location" \
#   -H "Content-Type: application/json" \
#   -d '{
#         "query": "AZ",
#         "page": 10,
#         "size": 5
#       }'