# reddit_project

## Sukhmanpreet Singh ssa560 (ID: 301575063)
How to run the files and the related libraries required

pip3 install pyspark to install pyspark
We used the following headers
from pyspark.sql import SparkSession, functions, types, Row
scipy for the stats
sklearn for the machine learning tools

Ways to run the files:-

reddit_comments.py # it is from the coursys website https://coursys.sfu.ca/2023fa-cmpt-353-d1/pages/RedditData, and in order to run it you need to ssh the 

cluster from your terminal as ssh cluster.cs.sfu.ca  Before clustering make sure you upload the file to the cluster

on your terminal type cd / the directory where this file is.

On changing the directory, type scp reddit_comments.py cluster.cs.sfu.ca:

On the terminal make sure you type this

spark-submit reddit_comments.py output_file_name

It will install the data for 2021 in order to get for any year before 2023, on top of the file you have this

reddit_comments_path = '/courses/datasets/reddit_comments_repartitioned/year=2022'  #CHANGE THE NUMBER HERE from 2022 to the year desired
reddit_submissions_path = '/courses/datasets/reddit_submissions_repartitioned/year=2022' #CHANGE THE NUMBER HERE from 2022 to the year desired

These files generated the following in the repository 2016_out, 2017_out, 2019_out, 2020_out, 2021_out


reddit_py # to get the desired data for controversial and non controversial data
running the file
spark-submit reddit.py troublemakers_name controversial_comments_name non_controversial_comments_name

On top of the file change the 2021_out to the year you want to get the file and run it every time for each year
reddit_submissions_path = '/Users/sukhi/Desktop/cmpt353/reddit/2021_out/submissions'
reddit_comments_path = '/Users/sukhi/Desktop/cmpt353/reddit/2021_out/comments'

This file gives me troublemakers, contro and non_contro folders

joiner.py #to get the multiple json files into one file in order to run in jupyter

to run this just do spark-submit joiner.py output_for_troublemakers output_for_controversial_comments output_for_non_controversial_comments
#this file gave me bad_comm, good_comm and trouble
    a = "/Users/sukhi/Desktop/cmpt353/reddit/trouble_makers"
    b = "/Users/sukhi/Desktop/cmpt353/reddit/contro"
    c = "/Users/sukhi/Desktop/cmpt353/reddit/non_contro"
    change the values of these three in order to get the path of the file to read
    
analysis_jupyter.ipynb
data_c = pd.read_json('/Users/sukhi/Desktop/cmpt353/reddit/bad_comm/controversial_comments.json', lines=True)
data_nc = pd.read_json('/Users/sukhi/Desktop/cmpt353/reddit/good_comm/non_controversial_comments.json', lines=True)

change the file name to the files created on the above in order to run this file 

import numpy as np
import pandas as pd
import datetime
import matplotlib.pyplot as plt
from scipy import stats
import statistics as stat
make sure you have all these installed
use pip3 install numpy
pip3 install pandas
pip3 isntall matplotlib
pip3 install scipy
pip3 install statistics

This file will generate the linear regression graphs


the training data is from 
training_data.py which you run by 
spark-submit training_data
reddit_submissions_path = '/Users/sukhi/Desktop/cmpt353/reddit/2021_out/submissions'
output1 = '/Users/sukhi/Desktop/cmpt353/reddit/training'
change the reading and output directory and the input is from cluster output do one year at a time and it generates training folder

analysis_2.ipynb

data = pd.read_csv('/Users/sukhi/Desktop/cmpt353/reddit/training/2021.csv')
change this to the desired reading file generated, i renamed the files in training folder.

this will give results for the machine learning tool in each training model.



## Rajandeep Kaur rka113 (ID: 301541521)

This builds on the fact that we already have the data imported from cluster.

* To check the top commenters
Run reddit.py in file using spark-submit reddit.py data* output_folder_name

data* - I combined all the comments in one file and submissions in another file for all the years
This file will give us the list of top commenters which i limited to 10 but can be changed in code.

*To plot and check the significance between month, author and comment count
Run create_plots.ipynb file 
Used uncompressed comments data for all the years in a single json file 

This will filter the comments data based on top commenters and then plot the month verus number of comments for each top author.

* Second inference
Using the same reddit.py file, this will run to calculate the total number of 'is_self' cases and corrosponding scores.

Based on scores, we will calculate the avergae score for each 'is_Self' case by dividing the total_score by total_instances.



## Amrinder Singh asa445 (ID: 301558389)

There  we used in next to programs is saved in submission_merged.zip, comments_merged1.zip, submission_merged2.zip. Please follow the steps:
1. Please unzip all files. 
2. Merge comments_merged1.json and comments_merged2.json into one file and name it comments_merged.json
3. Put comments_merged.json and submission_merged.json in the same directory of programs.

Tukey_test.ipynb

Change this to desired json file.
file_path = "E:\\Downloads1\\cmpt353\\project\\submissions_merged.json"

How to run this program:
-if you are using jupyter, open the file and run it.

It will perform print tukey_test, plot scatterplots, lineplotes and IQR summary of scores per reddit and years.

if you want to run in cmd, look for file name Tukey_test.py
It is same file but saved as .py.
Use command: python Tukey_test.py

Make sure, install all the following prior to run this
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


top_authors.py

(Similar to previous progran, Tuckey_test.ipynb) The data we used in this programs is saved in submission_merged.zip, comments_merged1.zip, submission_merged2.zip. Please follow the steps:
1. Please unzip all files. 
2. Merge comments_merged1.json and comments_merged2.json into one file and name it comments_merged.json
3. Put comments_merged.json and submission_merged.json in the same directory of programs.

Change this according to the need, to read files
reddit_submissions_path = 'submissions_merged.json'
reddit_comments_path = 'comments_merged.json'

Also change this to print the results of desired type and location
filtered_authors_comments.select('author', 'score', 'gilded').coalesce(1).write.json("Top_authors_comments.json", mode='overwrite')
filtered_authors_submissions.select('author', 'score', 'gilded').coalesce(1).write.json("Top_authors_submissions.json", mode='overwrite')

Make sure pyspark and the following are installed on computer, prior to run
from pyclbr import Function
import sys
from scipy import stats
from pyspark.sql import SparkSession, functions, types 

To run this file in cmd, use command: spark-submit top_authors.py

This will create folders named top_authors_comments and top_authors_submissions in same directory.
under these folder, check json file which contains all the top authors.














