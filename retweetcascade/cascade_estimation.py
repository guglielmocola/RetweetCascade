import pandas as pd
from retweetcascade.utility_functions import __convert_to_pandas_list_tw, __find_interaction, __explode_dict


def rt_cascade_interactions(retweets, followers, tweets, **kwargs):
    """Estimate the retweet cascade based on interactions among retweeters.
    A retweeter X is linked to the root R (original tweet's author) if he/she is a follower
    of R, otherwise interactions are used to establish the other retweeter Y that most
    likely acted as "influencer" and made the tweet visible to X.
    For each X, all the interactions (i.e., quotes, replies, retweets) made with
    other retweeters who retweeted before X are considered. The total interaction strength
    with other retweeters is determined using the specified weight parameters.
    If X is not a follower of R and there are no interactions with retweeters who retweeted
    before X, then it is not possible to automatically link X to another retweeter,
    and X is disconnected from the cascade graph.
    For more detail on the method see doi.org/10.3390/app10238394.

    :param retweets: list of retweets, each retweet being a tweet object.
    :param followers: list of followers of the root (id_str values).
    :param tweets: list of interactions, each being a tweet object (only the interactions of
    retweeters who are not followers of the root are useful).

    :keyword verbose: Verbose mode (default False)
    :keyword qt: Weight assigned to quotes (default 1.0)
    :keyword re: Weight assigned to replies (default 1.0)
    :keyword rt: Weight assigned to retweets (default 1.0)

    :return: Returns a pandas DataFrame with columns source and target, where each
    row describes an edge of the retweet cascade graph. Disconnected nodes are included with
    target equal to NaN.
    A string with an error message is returned in case of error.
    """

    # Set keyword arguments, start from default values
    verbose = False
    if 'verbose' in kwargs:
        verbose = kwargs['verbose']
    weights = {'qt': 1.0, 're': 1.0, 'rt': 1.0}
    for k in weights.keys():
        if k in kwargs:
            weights[k] = kwargs[k]
    if verbose:
        print('Weights:', weights)

    # Add row with the original tweet from the root to retweets, so that interactions w.r.t. the
    # root are considered as well.
    root_rt = retweets[0]['retweeted_status']
    root_id = root_rt['user']['id_str']
    retweets.append(root_rt)

    if verbose:
        print('ROOT ID:', root_id, 'Retweets:', len(retweets))

    # Prepare DataFrames about retweets and tweets (interactions)

    # Dataframe with RETWEETS (in case of multiple retweets from the same subject, keep the oldest)
    df_rt = __convert_to_pandas_list_tw(retweets, ['created_at', 'user.id_str'])
    df_rt = df_rt.sort_values(by=['created_at'], ascending=False)
    df_rt = df_rt.groupby(df_rt['user.id_str']).last().reset_index()  # last is the oldest

    # Dataframe with TWEETS by retweeters; keep columns about "interactions" (quote, reply, retweet)
    df_tw = __convert_to_pandas_list_tw(tweets, ['user.id_str', 'created_at',
                                                 'quoted_status.user.id_str', 'in_reply_to_user_id_str',
                                                 'retweeted_status.user.id_str'])

    if verbose:
        print('Retweeters:', len(df_rt), 'Tweets:', len(df_tw))

    # Find DataFrames highlighting INTERACTIONS
    # Each row is in the format <user.id_str, interacted_user_id_str, count>
    # where "interacted_user_id_str" is actually one of the three possible interaction fields
    # ('quoted_status.user.id_str', 'in_reply_to_user_id_str', or 'retweeted_status.user.id_str'),
    # depending on the evaluated interaction (quote, reply or retweet, respectively), and
    # "count" is the number of times the interaction occurred.
    df_int_qt = __find_interaction(df_rt, df_tw, 'quote')
    df_int_re = __find_interaction(df_rt, df_tw, 'reply')
    df_int_rt = __find_interaction(df_rt, df_tw, 'retweet')

    if verbose:
        print('Quotes:', len(df_int_qt), 'Replies:', len(df_int_re), 'Retweets:', len(df_int_rt))

    # Merge different interactions into a single DataFrame, using outer joins
    # 1. retweets and quotes.
    df_int_all = pd.merge(df_int_rt, df_int_qt, how='outer',
                          left_on=['user.id_str', 'retweeted_status.user.id_str'],
                          right_on=['user.id_str', 'quoted_status.user.id_str'], suffixes=('_rt', '_qt'))
    # When an interacted user has been either only quoted or retweeted (not both), the row contains
    # a N/A instead of the id_str, which can be filled using the present id_str value.
    df_int_all['quoted_status.user.id_str'].fillna(df_int_all['retweeted_status.user.id_str'], inplace=True)
    df_int_all['retweeted_status.user.id_str'].fillna(df_int_all['quoted_status.user.id_str'], inplace=True)

    # 2. retweets+quotes and replies
    df_int_all = pd.merge(df_int_all, df_int_re, how='outer',
                          left_on=['user.id_str', 'retweeted_status.user.id_str'],
                          right_on=['user.id_str', 'in_reply_to_user_id_str'])

    # Fill missing retweeted_status values using in_reply_to column.
    df_int_all['retweeted_status.user.id_str'].fillna(df_int_all['in_reply_to_user_id_str'], inplace=True)
    # Rename retweeted_status to interacted_user and remove the other (now redundant) columns.
    df_int_all.rename(columns={'retweeted_status.user.id_str': 'interacted_user'}, inplace=True)
    df_int_all.drop(['in_reply_to_user_id_str', 'quoted_status.user.id_str'], axis=1, inplace=True)

    # Rename count for replies properly.
    df_int_all.rename(columns={'count': 'count_re'}, inplace=True)

    # N/A count values are filled with zeros.
    df_int_all['count_qt'].fillna(0, inplace=True)
    df_int_all['count_rt'].fillna(0, inplace=True)
    df_int_all['count_re'].fillna(0, inplace=True)

    # Find interaction strength, then sort by it and only retain the strongest interaction
    # for each interacting user.
    df_int_all['is'] = df_int_all['count_qt'] * weights['qt'] + \
        df_int_all['count_re'] * weights['re'] + \
        df_int_all['count_rt'] * weights['rt']

    df_int_all = df_int_all.sort_values(by=['is'], ascending=False)
    df_int_final = df_int_all.groupby('user.id_str').first().reset_index()

    # Convert into "cascade DataFrame" with columns 'source' and 'target'
    # These format can be easily used to create a newtorkx object
    cascade_df = pd.DataFrame()
    cascade_df['source'] = df_int_final['user.id_str']
    cascade_df['target'] = df_int_final['interacted_user']

    # Save list of "interacting" non follower retweeters for later.
    int_rt_list = cascade_df['source'].tolist()

    # Add rows related to "direct retweeters", i.e., retweeters who also are followers
    # of the root.
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

    # Add "direct retweeters" to the "edges DataFrame"
    cascade_df = pd.concat([cascade_df, df_direct], ignore_index=True)

    # Use groupby source to remove duplicate entries, for instance if tweets from followers
    # were mistakenly included into tweets list.
    cascade_df = cascade_df.groupby('source', sort=False).last().reset_index()
    # edges_df

    # Finally, find disconnected nodes, and add a row with target NaN for them.
    disconnected_nodes = set(nf_rt_list) - set(int_rt_list) - set(direct_rt_list) - {root_id}
    if verbose:
        print('Disconnected:', len(disconnected_nodes),
              'Non-Followers:', len(set(nf_rt_list)),
              'Interacting', len(set(int_rt_list)))

    # Add disconnected nodes
    disconnected_df = pd.DataFrame(
        {'source': list(disconnected_nodes),
         'target': [float("NaN")] * len(disconnected_nodes),
         })

    cascade_df = pd.concat([cascade_df, disconnected_df], ignore_index=True)
    return cascade_df


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


def rt_cascade_info(cascade_df, root_id):
    """Find basic information on the cascade.
    The following metrics are found: number of disconnected nodes; levels in the cascade tree (depth);
    contribution of single "influencers" in spreading the original message.

    :param cascade_df: DataFrame with the edges of a retweet cascade tree, as produced by the
        other rt_cascade_* functions.
    :param root_id: id_str of the cascade's root, i.e., the author of the original tweet.

    :return: Returns a dictionary with keys ['disconnected', 'levels', 'influencers']
        'disconnected' is the number of disconnected nodes
        'levels' is a list with one int per tree level, each being the number of nodes in that
            level. The first int in the list indicates the number of nodes that directly
            retweeted from the root.
        'influencers' is a DataFrame with columns ['influencer', 'rt_count'], where 'influencer'
        is the id of a node and 'rt_count' indicates how many other nodes retweeted through that
        influencer. The DataFrame is ordered by 'rt_count' in descending order.
    """

    # Prepare return variable
    ret = {'disconnected': 0, 'levels': [], 'influencers': pd.DataFrame()}

    # 1. Disconnected count
    # Find nodes with target NaN
    disconnected_count = cascade_df.target.isna().sum()
    disc_list = cascade_df[cascade_df.target.isna()]['source'].tolist()
    # Add iteratively nodes that point to a previously found disconnected node.
    while True:
        disc_list = cascade_df[cascade_df.target.isin(disc_list)]['source'].tolist()
        if len(disc_list) == 0:
            break
        #     level_n += 1
        disconnected_count += len(disc_list)

    print('tot', len(cascade_df))
    print('disc', disconnected_count)
    ret['disconnected'] = disconnected_count
    # print(disconnected_count/len(cascade_df))

    # 2. Count the number of levels (tree depth), and the number of nodes at each level.
    # Considers only connected nodes (i.e., nodes with a path to the root).
    level_counts = []
    upper_level_ids = [root_id]
    level_n = 0
    while True:
        level_list = cascade_df[cascade_df.target.isin(upper_level_ids)]['source'].tolist()
        if len(level_list) == 0:
            break
        level_n += 1
        level_counts.append(len(level_list))
        upper_level_ids = level_list

    ret['levels'] = level_counts

    ## 3. Find the "influencers" df, describing how different accounts contributed to spreading the
    # original tweet. Root ID is excluded from this evaluation.
    # A DataFrame is prepared with columns ['influencer', 'rt_count'], ordered by rt_count (descending).
    # rt_count indicates how many retweets were made "through" the influencer.
    inf_df = cascade_df[cascade_df['target'] != root_id].groupby(['target']).count().reset_index()
    inf_df.sort_values(by=['source'], ascending=False, inplace=True)
    inf_df.reset_index(inplace=True, drop=True)
    inf_df.rename(columns={'target': 'influencer', 'source': 'rt_count'}, inplace=True)
    # inf_df.set_index('influencer', inplace=True)

    ret['influencers'] = inf_df

    return ret
