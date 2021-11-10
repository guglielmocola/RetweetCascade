import pandas as pd
from retweetcascade.utility_functions import __convert_to_pandas_list_tw, __find_interaction

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