#!/usr/bin/env python
# coding: utf-8

# In[2]:


from pyclbr import Function
import sys
from statsmodels.stats.multicomp import pairwise_tukeyhsd
from scipy import stats
from pyspark.sql import SparkSession, functions, types 
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import json


# In[ ]:


file_path = "E:\\Downloads1\\cmpt353\\project\\submissions_merged.json"

data_list = []
with open(file_path, "r", encoding="utf-8") as file:
    for line in file:
        data_dict = json.loads(line)
        data_list.append(data_dict)

reddit_submissions_df = pd.DataFrame(data_list)


# In[ ]:


reddit_submissions_df


# In[ ]:


unique_subreddits = reddit_submissions_df['subreddit'].unique()
subreddit_groups = {subreddit: reddit_submissions_df[reddit_submissions_df['subreddit'] == subreddit] for subreddit in unique_subreddits}

tukey_data = pd.concat([group['score'] for group in subreddit_groups.values()])
labels = [subreddit for subreddit in subreddit_groups.keys() for _ in range(len(subreddit_groups[subreddit]))]
tukey_result = pairwise_tukeyhsd(tukey_data, labels, alpha=0.05)


# In[ ]:


print(tukey_result.summary())


# In[ ]:


reddit_submissions_df['created_utc'] = pd.to_datetime(reddit_submissions_df['created_utc'], unit='s')
reddit_submissions_df['year'] = reddit_submissions_df['created_utc'].dt.year


# In[ ]:


print(reddit_submissions_df[['created_utc', 'year', 'subreddit', 'score']])


# In[ ]:


sns.set(style="whitegrid")
plt.figure(figsize=(12, 8))
sns.scatterplot(x='created_utc', y='score', hue='subreddit', data=reddit_submissions_df, alpha=0.7)
plt.xlabel('Creation Time (UTC)')
plt.ylabel('Score')
plt.title('Scatter Plot of Scores vs. Creation Time for Different Subreddits')
plt.xticks(rotation=45, ha='right')
plt.legend()
plt.savefig('scatterplot.png')
plt.show()


# In[ ]:


sns.set(style="whitegrid")
plt.figure(figsize=(12, 8))
sns.scatterplot(x='created_utc', y='score', hue='subreddit', data=reddit_submissions_df[reddit_submissions_df['subreddit'] !='xkcd'], alpha=0.7)
plt.xlabel('Creation Time (UTC)')
plt.ylabel('Score')
plt.title('Scatter Plot of Scores vs. Creation Time for Different Subreddits')
plt.xticks(rotation=45, ha='right')
plt.legend()
plt.savefig('scatterplot_exclude_xkcd.png')
plt.show()


# In[ ]:


reddit_submissions_df['created_utc'] = pd.to_datetime(reddit_submissions_df['created_utc'], unit='s')
reddit_submissions_df['year_month'] = reddit_submissions_df['created_utc'].dt.to_period('M')
sns.set(style="whitegrid")
exlude_outliers=reddit_submissions_df[reddit_submissions_df['subreddit'] !='xkcd']
plt.figure(figsize=(12, 8))
sns.scatterplot(x='created_utc', y='score', hue='subreddit', data=exlude_outliers, alpha=0.7)

for subreddit in exlude_outliers['subreddit'].unique():
    avg_scores = exlude_outliers[exlude_outliers['subreddit'] == subreddit].groupby('year_month')['score'].mean()
    plt.plot(avg_scores.index.astype(str), avg_scores.values, marker='o', linestyle='-', label=f'{subreddit} Average')


plt.xlabel('Creation Time (UTC)')
plt.ylabel('Score')
plt.title('Scatter Plot of Scores vs. Creation Time for Different Subreddits (Except xkcd) with Subreddit Monthly Averages')
plt.xticks(ticks=['2016-01', '2017-01', '2018-01', '2019-01', '2020-01', '2021-01'], labels=['2016-01', '2017-01', '2018-01', '2019-01', '2020-01', '2021-01'], rotation=45, ha='right')
plt.legend()
plt.savefig('lineplot_excluded_xkcd.png')
plt.show()


# In[ ]:


reddit_submissions_df['created_utc'] = pd.to_datetime(reddit_submissions_df['created_utc'], unit='s')
reddit_submissions_df['year_month'] = reddit_submissions_df['created_utc'].dt.to_period('M')

sns.set(style="whitegrid")
plt.figure(figsize=(12, 8))
sns.scatterplot(x='created_utc', y='score', hue='subreddit', data=reddit_submissions_df, alpha=0.7)

for subreddit in reddit_submissions_df['subreddit'].unique():
    avg_scores = reddit_submissions_df[reddit_submissions_df['subreddit'] == subreddit].groupby('year_month')['score'].mean()
    plt.plot(avg_scores.index.astype(str), avg_scores.values, marker='o', linestyle='-', label=f'{subreddit} Average')

plt.xlabel('Creation Time (UTC)')
plt.ylabel('Score')
plt.title('Scatter Plot of Scores vs. Creation Time for Different Subreddits with Subreddit Monthly Averages')
plt.xticks(ticks=['2016-01', '2017-01', '2018-01', '2019-01', '2020-01', '2021-01'], labels=['2016-01', '2017-01', '2018-01', '2019-01', '2020-01', '2021-01'], rotation=45, ha='right')
plt.ylim(bottom=-200)
plt.legend()
plt.savefig('lineplot_averages.png')
plt.show()


# In[ ]:


reddit_submissions_df['created_utc'] = pd.to_datetime(reddit_submissions_df['created_utc'], unit='s')
reddit_submissions_df['year_month'] = reddit_submissions_df['created_utc'].dt.to_period('M')
def calculate_summary_stats(subreddit_data):
    stats = {
        'Min': subreddit_data['score'].min(),
        'Q1': subreddit_data['score'].quantile(0.25),
        'Median': subreddit_data['score'].median(),
        'Q3': subreddit_data['score'].quantile(0.75),
        'Max': subreddit_data['score'].max()
    }
    return pd.Series(stats)
summary_stats_df = pd.DataFrame(columns=['Subreddit', 'Year', 'Min', 'Q1', 'Median', 'Q3', 'Max'])
for subreddit in reddit_submissions_df['subreddit'].unique():
    for year in reddit_submissions_df['year'].unique():
        subset = reddit_submissions_df[(reddit_submissions_df['subreddit'] == subreddit) & (reddit_submissions_df['year'] == year)]
        summary_stats = calculate_summary_stats(subset)
        summary_stats_df = summary_stats_df.append({'Subreddit': subreddit, 'Year': year, **summary_stats}, ignore_index=True)

summary_stats_df = summary_stats_df.sort_values(by=['Subreddit', 'Year']).reset_index(drop=True)
summary_stats_df.to_csv("Scores_by_IQR_Yearly.csv", index=False)


# In[ ]:


summary_stats_df

