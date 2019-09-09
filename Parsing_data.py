from elasticsearch import Elasticsearch as ES
import json
import pandas as pd

# Elastic Setup config
host = "10.81.97.126"
port = 9200
final_scroll_data_all=pd.DataFrame([])#used to collect search query from each month

date_list=[]#record all available days of index
for i in range(len(c)):
    j=c.iloc[i][0].split(' ')[5]
    if 'txn-30-day-' in j:
        date_list.append(j)
date_list.sort()#sort the list by date
print('days of data available:', len(date_list))

for i in range(len(date_list)):#all days available in past month
    index= date_list[i]
    print(index)

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
        
        #cid
        cid=pd.Series(each_scroll['hits']['hits']).apply(lambda x: x['_source']['cid'].split(' ')[0])
        #user_strategy
        user_strategy=pd.Series(each_scroll['hits']['hits']).apply(lambda x: x['_source']['details']['search_intelligence_info']['auto_correct_info']['user_strategy'])
        #confidence
        confidence=pd.Series(each_scroll['hits']['hits']).apply(lambda x: x['_source']['details']['search_intelligence_info']['auto_correct_info']['confidence'])
        #get user entered search query
        raw_query=pd.Series(each_scroll['hits']['hits']).apply(lambda x: x['_source']['details']['search_intelligence_info']['qualified_term'])
        #get suggested search query
        corrected_query=pd.Series(each_scroll['hits']['hits']).apply(lambda x: x['_source']['details']['search_intelligence_info']['auto_correct_info']['suggestion'])
        #combine them
        df=pd.concat([cid, user_strategy, confidence, raw_query, corrected_query], axis=1)
        df.columns=['cid', 'user_strategy', 'confidence', 'raw_query', 'corrected_query']
        final_scroll_data=pd.concat([final_scroll_data, df]).drop_duplicates()#unique search query
    final_scroll_data_all=pd.concat([final_scroll_data_all, final_scroll_data])
        

final_scroll_data_all=final_scroll_data_all.drop_duplicates()
final_scroll_data_all.to_csv('ELK_data.csv')#save the file to the directory you located
print("total unique search query:", len(final_scroll_data_all))
final_scroll_data_all.head(50)
