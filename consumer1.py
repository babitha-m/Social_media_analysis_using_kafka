#!/usr/bin/env python3

#importing the necessary libraries

import sys
import json
from kafka import KafkaConsumer as kc

server='localhost:9092' 

# topics list has all the topics.
topic1, topic2, topic3 = sys.argv[1],sys.argv[2],sys.argv[3]
topics=[topic1,topic2,topic3]

consumer =kc(topic1, bootstrap_servers=server)

#client1 is to list down all the comments received on posts for all users.

#this dictionary will have the collection of all the comments.
comments_col=dict()

#consuming the messages from producer.

for message in consumer:
    msg_val=message.value.decode('utf-8')
    if msg_val=="EOF":
        break
    msg_val=json.loads(message.value.decode('utf-8'))
    comment_on_post = msg_val['com_on_post_of']
    comment = msg_val['comment']

#checking for comments if already present in the dictionary , if not append it to the list of values.

    if comment_on_post not in comments_col:
        comments_col[comment_on_post] = [] 
    
    
    comments_col[comment_on_post].append(comment)

            
print(json.dumps(comments_col, indent=4,sort_keys=True))

consumer.close()
