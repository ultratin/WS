# Web Science Assignment
This project is a Web Science assignment for crawling twitter and clustering it. This README will go through the steps to set up the project environment and running the program

# Getting Started
## Prerequisites
This program uses the following technologies.
- Python 3.6
- Jupyter Notebook
- MongoDB

## Packages needed
There is several packages that need to be installed before the program can start.
- Tweepy for accesing the Twitter API
- spaCy for Name Entity Recognition(NER) analysis of the tweets
- NumPy prerequisite for other packages
- SciPy prerequisite for other packages
- Science Kit Learn for creating bag of words and clustering
- NLTK for lemmatization, tokenization and word tagger
- Matplotlib for graph plotting
- PRAW for accessing the Reddit API

## Installing
Run the following command to install all packages
```
pip install tweepy spacy numpy scipy scikit-learn nltk matplotlib praw
```
spaCy requires a model to run which can be downloaded with the following command
```
python -m spacy download en
```

# Running the program
To run the twitter crawler, cd into the directory and run the command
```
python twitter.py
```

To run the twitter counter open up jupyter notebook with the command `jupyter notebook` and select the twitter_count.ipynb notebook 

To run the clustering open up jupyter notebook with the command `jupyter notebook` and select the kmeans.ipynb notebook 

To run the reddit crawler, cd into the directory and run the command
command
```
python reddit_crawler.py
```

To run the reddit couunt, cd into the directory and run the command
command
```
python count_reddit.py
```

