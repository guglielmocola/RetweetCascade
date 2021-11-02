import pandas as pd
from pandasticsearch import Select

## Utility functions
# EDITED version to manage LIST of TW objects instead of QUERY
def __convert_to_pandas_list_tw(list_tw, fields=None):
    if fields is None:
        fields = []
    tw_like_query = []
    for tw in list_tw:
        item = {'_source': tw}
        tw_like_query.append(item)

    query_df = Select.from_dict({'took': 0, 'hits': {'hits': tw_like_query}}).to_pandas()
    if len(fields) > 0:
        query_df = query_df.filter(fields, axis=1)
    if 'created_at' in query_df:
        query_df['created_at'] = pd.to_datetime(query_df['created_at'])
    return query_df


# Find a dataframe showing the number of interactions between users.
# Only interactions with users who retweeted BEFORE are considered.
#
# @par df_rt: dataframe with the retweets
# @par df_tw: dataframe with the tweets including possible interactions
# @par interaction_type: type of interaction to be considered ('quote', 'reply', or 'retweet')
#
# @ret Dataframe with the number of interactions.
#  Columns are: <user.id_str, "int_field", count>
#   user.id_str is the id of the interacting user;
#   int_field is one among 'quoted_status.user.id_str', in_reply_to_user_id_str','retweeted_status.user.id_str',
#    and indicated the id of the interacted user;
#   count indicated the number of interactions between the two users.
def __find_interaction(df_rt, df_tw, interaction_type):
    interaction_types = {
        'quote': 'quoted_status.user.id_str',
        'reply': 'in_reply_to_user_id_str',
        'retweet': 'retweeted_status.user.id_str'}

    if interaction_type not in interaction_types:
        print('Error: unknown interaction', interaction_type)
        print('Supported interactions are:', list(interaction_types.keys()))
        return -1  # error

    int_field = interaction_types[interaction_type]

    # Drop rows where there is not the selected type of interaction (int_field is Null)
    df_int = df_tw.dropna(subset=[int_field])

    # Only consider interactions with "interacted" users who retweeted BEFORE the "interacting" user
    # To this end, two created_at dates are added to the dataframe through join operations.
    # First merge: add created_at of the interacting user's retweet
    df_merge = pd.merge(df_int, df_rt[['user.id_str', 'created_at']], on='user.id_str', suffixes=('', '_user'))
    # Second merge: add created_at of the interacted user's retweet (created_at_interacted)
    df_merge = pd.merge(df_merge, df_rt[['user.id_str', 'created_at']], left_on=int_field,
                        right_on='user.id_str', suffixes=('', '_interacted'))

    # Only keep the rows where created_at_user is bigger than created_at_interacted, hence the interacting
    # user retweeted AFTER the interacted and the interaction might have influenced the user.
    df_merge['delta'] = (df_merge['created_at_user'] - df_merge['created_at_interacted']).dt.total_seconds()
    df_merge = df_merge[df_merge['delta'] > 0]

    # Group interacting-interacted couples and count the occurrences.
    grouped = df_merge.groupby(['user.id_str', int_field]).agg({'delta': 'count'})
    grouped = grouped.reset_index()
    grouped.rename(columns={'delta': 'count'}, inplace=True)

    return grouped[['user.id_str', int_field, 'count']]


# "Explode" a dictionary in format key: [list of values] into 'index': [key, value]
# Useful to obtain a dataframe where each key, value pair is on a separate row.
def __explode_dict(d):
    ret = {}
    count = 0
    for key in d:
        for el in d[key]:
            ret[str(count)] = [str(key), str(el)]
            count += 1
    return ret
