from elasticsearch import Elasticsearch
import json
from datetime import datetime, timedelta
es = Elasticsearch(['http://172.168.200.202:9200'], request_timeout=100)

def query_keyword_with_topic(es, start_date_str, end_date_str, type, query_data_file):
    try:
        body1 = {
            "query": {
                "bool": {
                    "must": [
                        {"match_phrase": {"type": type}},
                        {"range": {"created_time": {"gte": start_date_str, "lte": end_date_str}}}
                    ]
                }
            },
            "sort": [{"created_time": {"order": "asc"}}],
            "_source": ["content", "title", "created_time", "topic_id"]
        }

        result = es.search(index='posts', scroll='2m', size=100, body=body1, request_timeout=2000)
        scroll_id = result['_scroll_id']
        scroll_size = len(result['hits']['hits'])

        records = []

        while scroll_size > 0:
            for hit in result['hits']['hits']:
                # Bổ sung trường 'topic_id' nếu không tồn tại
                if 'topic_id' not in hit['_source']:
                    hit['_source']['topic_id'] = []
                # if 'hashtag' not in hit['_source']:
                #     hit['_source']['hashtag'] = []

                records.append(hit)

            result = es.scroll(scroll_id=scroll_id, scroll='2m', request_timeout=2000)
            scroll_id = result['_scroll_id']
            scroll_size = len(result['hits']['hits'])


    
        with open(query_data_file, 'w', encoding='utf-8') as f:
            json.dump(records, f, ensure_ascii=False, indent=4)

        return records
    
    except Exception as e:
        print(f"An error occurred: {e}")
        with open(query_data_file, 'w', encoding='utf-8') as f:
            json.dump([], f, ensure_ascii=False, indent=4)


        return []
if __name__ == '__main__':
    # start_date = "02-01-2024"
    # end_date = "02-25-2024"
    # start_date = datetime.strptime(start_date, "%m-%d-%Y")
    # end_date = datetime.strptime(end_date, "%m-%d-%Y")
    # start_date_str = start_date.strftime("%m/%d/%Y 00:00:01")
    # end_date_str = end_date.strftime("%m/%d/%Y 00:00:00")

    # query_day(start_date_str, end_date_str)
    
    start_date = "10-10-2024"
    end_date = "10-17-2024"
    start_date = datetime.strptime(start_date, "%m-%d-%Y")
    end_date = datetime.strptime(end_date, "%m-%d-%Y")
    start_date_str = start_date.strftime("%m/%d/%Y 00:00:01")
    end_date_str = end_date.strftime("%m/%d/%Y 23:59:59")
    # query_day_3(start_date_str , end_date_str)
    # query_comment_by_dates(start_date ,end_date )
    # combined_queries(start_date, end_date)
    query_data_file = ''
    query_keyword_with_topic(es, start_date_str ,end_date_str , "media",  "content_test_newquery_filter_media.json" )
