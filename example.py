import pickle
import time
from retweetcascade import *

"""Example script showing how the library can be used.
"""

# Which technique should be used to estimate the cascade, interaction-based or friendship-based?
interaction_based = True # set this to False to use the friendship-based technique

if interaction_based:
    print('Interaction-based technique enabled')
else:
    print('Friendship-based technique enabled')

# LOAD retweets and other needed data from pickle files,
# for instance using the sample dataset https://data.d4science.net/3FjC
data_path = 'XXX_PATH_TO_DATASET_XXX/'

retweets  = pickle.load(open(data_path + 'retweets.pickle', "rb"))
followers = pickle.load(open(data_path + 'followers.pickle', "rb"))
if interaction_based:
    tweets    = pickle.load(open(data_path + 'tweets.pickle', "rb"))
else:
    friends   = pickle.load(open(data_path + 'friends.pickle', "rb"))

# Get the root id from any retweet in retweets.
root_id = retweets[0]['retweeted_status']['user']['id_str']
print('ROOT_ID', root_id, 'Retweets:', len(retweets), 'Followers:', len(followers), end=' ')

# Print some stats on loaded data
if interaction_based:
    print('Interactions:', len(tweets))
else:
    print('')

print('Starting cascade estimation...', end='')
start_time = time.time()

# There are two options to estimate the cascade (interaction-based vs friendship-based),
# uncomment the preferred method.
if interaction_based:
    result = rt_cascade_interactions(retweets, followers, tweets, qt=1.0, re=1.0, rt=1.0)
else:
    result = rt_cascade_friendships(retweets, followers, friends)
print(' done in', round(time.time()-start_time, 1), 'seconds\n')

print('Estimated cascade DataFrame:')
print(result)

info = rt_cascade_info(result, root_id)
print('Cascade main information:')
print('Disconnected nodes:', info['disconnected'])
print('Nodes per level:', info['levels'])
print('Top 3 influencers:')
print(info['influencers'].head(3))
