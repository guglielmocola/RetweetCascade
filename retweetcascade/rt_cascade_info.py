import pandas as pd

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
