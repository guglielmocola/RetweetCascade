import pandas as pd
from retweetcascade.utility_functions import __convert_to_pandas_list_tw, __explode_dict


def rt_cascade_friendships(retweets, followers, friends, **kwargs):
    """Estimate the retweet cascade based on friendship among retweeters.
    A retweeter X is linked to the root R (original tweet's author) if he/she is a follower
    of R, otherwise it is linked to the last friend who retweeted before X. If X is not
    a follower of R and there are no friends who retweeted before X, then it is not possible
    to automatically link X to another retweeter, and X is disconnected from the cascade graph.

    :param retweets: list of retweets, each retweet being a tweet object.
    :param followers: list of followers of the root (id_str values).
    :param friends: dictionary describing the friends of retweeters (only the friends of
    retweeters who are not followers of the root are useful); each key is the id_str
    of a retweeter and points to the list of friends (id_str values).

    :keyword verbose: Verbose mode (default False)
    :keyword qt: Weight assigned to quotes (default 1.0)
    :keyword re: Weight assigned to replies (default 1.0)
    :keyword rt: Weight assigned to retweets (default 1.0)

    :return: Returns a pandas DataFrame with columns source and target, where each
    row describes an edge of the retweet cascade graph. Disconnected nodes are included with a
    target equal to NaN.
    A string with an error message is returned in case of error.
    """

    # Set keyword arguments, start from default values
    verbose = False
    if 'verbose' in kwargs:
        verbose = kwargs['verbose']

    # Find the root from a retweet.
    root_id = retweets[0]['retweeted_status']['user']['id_str']

    # DataFrame with RETWEETS (in case of multiple retweets from the same subject, keep the oldest)
    df_rt = __convert_to_pandas_list_tw(retweets, ['created_at', 'user.id_str'])
    df_rt = df_rt.sort_values(by=['created_at'], ascending=False)
    df_rt = df_rt.groupby(df_rt['user.id_str']).last().reset_index()  # last is the oldest

    # List of retweeters who also are followers ("direct retweeters")
    direct_rt_list = []
    # List of non-follower retweeters (will be useful later to find disconnected nodes)
    nf_rt_list = []
    for rt in retweets:
        rt_user = rt['user']['id_str']
        if rt_user in followers:
            direct_rt_list.append(rt_user)
        else:
            nf_rt_list.append(rt_user)
    # Remove duplicates
    direct_rt_list = list(set(direct_rt_list))

    # Create DataFrame for these users, then add it to the main one.
    df_direct = pd.DataFrame(direct_rt_list, columns=['source'])
    df_direct['target'] = root_id

    # Create rt DataFrame with just non-follower retweeters.
    df_nf_rt = df_rt[~df_rt['user.id_str'].isin(direct_rt_list)].reset_index(drop=True)

    # Create DataFrame for friendships, with <user.id, friend.id> info
    df_friends = pd.DataFrame(__explode_dict(friends)).T
    df_friends.columns = ['follower_id_str', 'friend_id_str']

    # First merge links non-follower retweeters with their friends
    df_merge1 = df_nf_rt.merge(df_friends, left_on='user.id_str', right_on='follower_id_str')

    # Second merge adds retweet information for friends (this time the merge needs to be with
    # the entire retweets DataFrame)
    df_merge2 = df_merge1.merge(df_rt, left_on='friend_id_str', right_on='user.id_str', suffixes=('', '_y'))

    # Remove rows where 'created_at_y' > 'created_at'
    df_merge2['delta'] = (df_merge2['created_at'] - df_merge2['created_at_y']).dt.total_seconds()
    df_merge2 = df_merge2[df_merge2['delta'] > 0]

    df_final = df_merge2[['user.id_str', 'created_at', 'friend_id_str', 'created_at_y', 'delta']]
    df_final = df_final.sort_values(by=['delta'], ascending=False)
    df_final = df_final.groupby(df_final['user.id_str']).last().reset_index()  # last is the oldest

    # Prepare cascade DataFrame based on friendship, then cat it with direct followers
    cascade_df = pd.DataFrame()
    cascade_df['source'] = df_final['user.id_str']
    cascade_df['target'] = df_final['friend_id_str']

    # Save list of "friend-based" non follower retweeters for later.
    fb_rt_list = cascade_df['source'].tolist()

    # Cat with direct retweeters (followers of root)
    cascade_df = pd.concat([cascade_df, df_direct], ignore_index=True)

    # Finally, find disconnected nodes, and add a row with NaN target for them.
    disconnected_nodes = set(nf_rt_list) - set(fb_rt_list)

    # print('dis:', len(disconnected_nodes), 'nf:', len(set(nf_rt_list)), 'fb-estimated', len(set(fb_rt_list)))

    # fw_in_int = set(fb_rt_list) - set(direct_rt_list)
    # print(len(fw_in_int))

    # Add disconnected nodes with 'NaN' target
    disconnected_df = pd.DataFrame(
        {'source': list(disconnected_nodes),
         'target': [float("NaN")] * len(disconnected_nodes),
         })

    # Find final edges df, including disconnected nodes
    cascade_df = pd.concat([cascade_df, disconnected_df], ignore_index=True)

    # Remove the root from source, if present
    cascade_df.drop(cascade_df.loc[cascade_df['source'] == root_id].index, inplace=True)
    cascade_df.reset_index(inplace=True, drop=True)

    return cascade_df
