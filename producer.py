#!/usr/bin/env python3

#importing the necessary libraries

import sys
import json
from kafka import KafkaProducer

server='localhost:9092'  #configuring the server(?)

# topics comment, like, share
# topics list has all the topics.
topic1, topic2, topic3 = sys.argv[1],sys.argv[2],sys.argv[3]
topics=[topic1,topic2,topic3]
# print(topic1)
# print(topic2)
# print(topic3)

producer = KafkaProducer(bootstrap_servers=server)

for line in sys.stdin:
    line=line.strip()
    if(line=='EOF'):
        for t in topics:
            producer.send(t,b'EOF')
        break
    
    parts=line.split(' ',4)
    act=parts[0]
    if (act=='comment'):
        data={
            'action': 'comment',
            'commenter':parts[1],
            'com_on_post_of':parts[2],
            'postid':parts[3],
            'comment' : parts[4][parts[4].find('"')+1:parts[4].find('"',parts[4].find('"')+1)]
            
        }
        producer.send(topic1,json.dumps(data).encode('utf-8'))

    elif (act=='like'):
        data={
            'action': 'like',
            'liker':parts[1],
            'like_post_of':parts[2],
            'postid':parts[3]
        }
        producer.send(topic2,json.dumps(data).encode('utf-8'))
        
    elif act == 'share':
        data={
        'action': 'share',
        'shared_by':parts[1],
        'share_post_of':parts[2],
        'postid':parts[3],
        'shared_to_users':parts[4].strip().split(' ')
        }

    producer.send(topic3, json.dumps(data).encode('utf-8'))
        
producer.close()



