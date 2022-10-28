How to launch: python app.py /path/to/dataset.txt
Two operating modes, depending on the user input
1 - Top 5 trending topics a day
2 - Top N topics per selected timestamp (also user input)

Timestamp ranges:
For small dataset, the trending topics interval is between 2020-05-29 15:26:47 and 
2020-05-29 15:38:52.
For large dataset: between 2020-05-28 15:34:34 and 2020-05-29 15:42:04

Notes: Since the collected data is in Dutch, experienced some issues with transforming and cleaning the data 
stopwords, verbs, nouns identification - need to train and validate some NLTK modules. Instead used pre-parced hashtags by Twitter, they are stored in the entities Twitter object, removing non-Dutch hashtags and saving the barplot of top Trending topics. 
