[![SBD++](https://img.shields.io/badge/Available%20on-SoBigData%2B%2B-green)](https://sobigdata.d4science.org/group/sobigdata-gateway/explore?siteId=20371853)

Retweet cascade estimation
=========================================================
A collection of tools for the estimation and analysis of a "retweet cascade", i.e., the tree structure describing how the original tweet was spread by retweeters. Concerning estimation, the library includes two methods: 
* rt_cascade_interactions 
* rt_cascade_friendship

*rt_cascade_interactions* relies on previous interactions among users to estimate the most likely "influencer", whereas *rt_cascade_friendship* is based on "friendship" among users (it is supposed that the user retweeted from the last friend who retweeted). Both methods return a pandas DataFrame where each row describes an edge in the tree (source, target). Disconnected nodes are included with a target equal to NaN. As such, the returned DataFrame actually describes a "forest", where the nodes connected to disconnected nodes may form separate trees.

Concerning analysis, the library provides the method:
* rt_cascade_info

*rt_cascade_info* to find basic metrics from the cascade DataFrame: number of disconnected nodes, levels in the cascade tree (depth), contribution of single "influencers" in spreading the original message. 

The library will be also made available in the method development area of the SoBigData++ infrastructure (https://sobigdata.d4science.org/group/sobigdatalab/jupyterhub).

For more information on the methods and the required parameters, please refer to source code documentation.

Use example
------------------------------------------------

File *use_example.py* provides examples on how the methods described above can be imported and used.

To test the library, a small test dataset is available in the SoBigData++ platform at this link: XXX. The .zip file includes four .pickle files with retweets, a follower list, friend lists, and interactions among users.

References
-------------------------------------------------
Zola, P., Cola, G., Mazza, M. and Tesconi, M., 2020. Interaction strength analysis to model retweet cascade graphs. Applied Sciences, 10(23), p.8394. DOI: https://doi.org/10.3390/app10238394

License
-------------------------------------------------

Released under the terms of the MIT license (https://opensource.org/licenses/MIT).
