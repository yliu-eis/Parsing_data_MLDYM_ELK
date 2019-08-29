from elasticsearch import Elasticsearch as ES
import json
import pandas as pd

# Elastic Setup config
host = "10.81.97.126"
port = 9200
final_scroll_data_all=pd.DataFrame([])#used to collect search query from each month
for i in range(1, 31):#day 1 to day 31 for this month if available
    if i <10:
        i=str(0)+str(i)
    print(i)
    index = str("txn-30-day-2019.08.")+str(i)

    es = ES(host=host, port=port, timeout=100)

    # Initialize the scroll
    scroll_data = es.search(
      index = index,
      scroll = '2m',#search for a total of 2minutes
      size = 1000,
      body = {
      #    "_source": ["details.requestor_info.interface_id", "details.search_intelligence_info.auto_correct_info.suggestion","details.search_intelligence_info.qualified_term","details.search_intelligence_info.auto_correct_info.confidence"],
      "query" : {
         "bool" : {
             ##1:
             "must": 
             [
               {
                "exists": {
                    "field": "details.search_intelligence_info.auto_correct_info.suggestion",
                } 
               },
                 {
                "exists": {
                    "field": "details.search_intelligence_info.qualified_term"
                }    
               }
             ],
             ##2:
             "must_not": [
               { "match": {"details.search_intelligence_info.auto_correct_info.confidence": "Rejected" }},
               { "match": {"details.search_intelligence_info.auto_correct_info.confidence": "NoSuggestion" }}
             ],
             ##3:
             "should" : [
             { "term" : { "details.requestor_info.interface_id" : "eds" }},
             { "term" : { "details.requestor_info.interface_id" : "ehost" }}
           ]
         }
       }
      }
    )

    sid = scroll_data['_scroll_id']
    scroll_size = scroll_data['hits']['total']
    final_scroll_data = pd.DataFrame([])#used to collect search query from each day

    while (scroll_size > 0):#search all available data from each day
        each_scroll = es.scroll(scroll_id = sid, scroll = '2m')
        sid = each_scroll['_scroll_id']
        scroll_size = len(each_scroll['hits']['hits'])
        
        #get user entered search query
        raw_query=pd.Series(each_scroll['hits']['hits']).apply(lambda x: x['_source']['details']['search_intelligence_info']['qualified_term'])
        #get suggested search query
        corrected_query=pd.Series(each_scroll['hits']['hits']).apply(lambda x: x['_source']['details']['search_intelligence_info']['auto_correct_info']['suggestion'])
        #combine them
        df=pd.concat([raw_query, corrected_query], axis=1)
        df.columns=['raw_query', 'corrected_query']
        final_scroll_data=pd.concat([final_scroll_data, df]).drop_duplicates()#unique search query
    final_scroll_data_all=pd.concat([final_scroll_data_all, final_scroll_data])


final_scroll_data_all=final_scroll_data_all.drop_duplicates()
final_scroll_data_all.to_csv('ELK_data.csv')#save the file to the directory you located
print("total unique search query:", len(final_scroll_data_all))
final_scroll_data_all.head(50)
