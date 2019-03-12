**Issues while moving tweets to Elastic search**

`PUT -->/twitter/_settings


{
    "settings": {
        "index.mapping.depth.limit": "2000"
    }
}`


curl -XGET https://wn6yc16u6y:wtlntcq95h@kafka-elastic-search-8699235957.us-west-2.bonsaisearch.net/_cat/indices

green open .kibana_1 pZ4TeYOiQBWaG185kADkWA 1 1  1 0 7.4kb 3.7kb
green open twitter   y8ecLA7VR7K6MN8ZyB-sew 1 1 66 0 3.7mb 1.8mb

