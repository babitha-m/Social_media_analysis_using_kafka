#!/usr/bin/env python3

#importing the necessary libraries

import sys
from kafka import KafkaConsumer as kc
import json

topic1 = sys.argv[1]
topic2 = sys.argv[2]
topic3 = sys.argv[3]

consumer = kc( topic3, bootstrap_servers='localhost:9092')

comments_dict = dict()
likes_dict= dict()
shares_dict=dict()

for msg in consumer:
    msg_val = msg.value.decode('utf-8')
    if msg_val=="EOF":
        break
    msg_val = json.loads(msg_val)
    action = msg_val["action"]

    if (action == 'comment'):
        comment_by = msg_val["commenter"]
        comment_on = msg_val["com_on_post_of"]
        comment = msg_val["comment"]
        if comment_on not in comments_dict:
            comments_dict[comment_on] = 0
        comments_dict[comment_on] = comments_dict[comment_on]+1

    elif (action == 'like'):
        liked_by = msg_val["liker"]
        liked_of = msg_val["like_post_of"]
        post_id = msg_val["postid"]
        
        if liked_of not in likes_dict:
            likes_dict[liked_of] = 0
        likes_dict[liked_of] = likes_dict[liked_of]+1

    elif (action == 'share'):
        shared_on = msg_val["share_post_of"]
        shared_to = msg_val["shared_to_users"]
       
        if shared_on not in shares_dict:
            shares_dict[shared_on] = 0
        shares_dict[shared_on] =shares_dict[shared_on] +len(shared_to)


userpopularity = {}
users =set(comments_dict) | set(likes_dict) | set(shares_dict)

for user in users:
    
    likes = likes_dict.get(user, 0)
    shares = shares_dict.get(user, 0)
    comments = comments_dict.get(user, 0)
  
    popularity = (likes + 20 * shares + 5 * comments) / 1000
    userpopularity[user] = popularity


print(json.dumps({k: v for k, v in sorted(userpopularity.items(), key=lambda item: item[0])}, indent=4, sort_keys=False))

 
consumer.close()
