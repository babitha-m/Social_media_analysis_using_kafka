#!/usr/bin/env python3

#importing the necessary libraries

import sys
import json
from kafka import KafkaConsumer as kc

server='localhost:9092' 

# topics list has all the topics.
topic1, topic2, topic3 = sys.argv[1],sys.argv[2],sys.argv[3]
topics=[topic1,topic2,topic3]

consumer =kc(topic2, bootstrap_servers=server)

#client2 is to list down the number of likes received on different posts for each user.
#creating a dictionary that stores user as key and likes as values

likes_col=dict()

#consuming messages from producer topic2
for message in consumer:
    msg_val = message.value.decode('utf-8')
    if msg_val=="EOF":
        break
    msg_val= json.loads(message.value.decode('utf-8'))
    liked_by = msg_val['liker']
    post_of_liked= msg_val['like_post_of']
    post_id = int(msg_val['postid'])
    if post_of_liked not in likes_col:
        likes_col[post_of_liked]=dict()
    if post_id not in likes_col[post_of_liked]:
        likes_col[post_of_liked][post_id]=0
    likes_col[post_of_liked][post_id]=likes_col[post_of_liked][post_id]+1

#sorting and printing

print(json.dumps({k: v for k, v in sorted(likes_col.items())}, indent=4, sort_keys=False))


consumer.close()



