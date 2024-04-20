Dataset
You will be working with the a social media dataset. The dataset is artificailly generated. You can find the required files here. The rows in the dataset are of three
types, based on the social media action they are describing.
1. Like - Describes when a user likes a post of another user. Format: [like @user_liking @user_who_posted post_id]
2. Share - Describes when a user shares the post of another user. Format: [share @user_sharing @user_who_posted post_id @user_shared_to_1
@user_shared_to_2 ...]
3. Comment - Describes when a user comments on the post of another user. Format: [comment @user_commenting @user_who_posted post_id "comment
enclosed in double quotes"] NOTE: Post_id is unique to each user, but not unique across users.

Problem Statement
Using the dataset provided to you, generate output files for 3 different clients based on their requirements. Client 1: Wants all the comments which have been made
on a particular user's account. Client 2: Wants a count of the number of likes for every post made by a user Client 3: Wants to calculate the popularity of a user, based
on the number of likes, comments, and shares on their profile.
Description
Use the student-dataset.txt file to prepare the desired outputs for each client. You are required to use Kafka's producer and consumer APIs to solve the problem
statement.
A sample input of the dataset is as follows. Consider the scenario
Instagram user2 made a post (with id 1) . user1 liked the post, commented “So beautiful”, and shared it with three other users: user3, user4, and user 5
Instagram user3 made a post (with id 1). user1 liked that post, and shared it with one other user: user5. user2 liked the post and commented "Brilliant" on it.
Instagram user2 made a post (with id 2). user1 liked that post, commented “Stunning” and shared it with seven other users: user3, user4, user5, user6, user7, user8,
user9
comment @user1 @user2 1 “So beautiful”
like @user1 @user2 1
share @user1 @user2 1 @user3 @user4 @user5
like @user1 @user3 1
share @user1 @user3 1 @user5
like @user2 @user3 1
comment @user2 @user3 1 "Brilliant"
comment @user1 @user2 2 “Stunning”
like @user1 @user2 2
share @user1 @user2 2 @user3 @user4 @user5 @user6 @user7 @user8 @user9
Towards this, you will have to write three consumer files, one for each client.
