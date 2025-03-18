from utils import *

# Reading & Writing ND JSON files
def read_and_write(path) -> list:
    '''
    param path (str): directory where the input json file is
    return list_of_tweets (iterable): list containing tweets as json objects with relevant data read in from the file(s) within path input
    '''

    # List to store fields from the tweets
    list_of_tweets = []

    with zipfile.ZipFile(path , 'r') as infile:
        for geoEurope in infile.namelist():                
            # this goes inside the geoEurope folder
            with infile.open(geoEurope) as f:                    
                # iterate through each line , i.e. each tweet object
                for line in f:                        
                    tweet = json.loads(line)
                    
                    # row-wise inclusion into a sub-list
                    list_of_field = []

                    # # User ID
                    # try:
                    #     list_of_field.append(tweet.get('user').get('id')) # use id only if this takes up more mem/time
                    # except AttributeError:
                    #     list_of_field.append("None")

                    # Timestamp (UTC)
                    list_of_field.append(tweet.get('timestamp_ms'))

                    # # Created At (Local Time)
                    # list_of_field.append(tweet.get('created_at'))

                    # Tweet ID
                    list_of_field.append(tweet.get('id'))


                    # # Mentions
                    # mentions = []
                    # try:
                    # # If there are multiple mentions, iterate thru them
                    #     for mention in tweet.get('extended_tweet').get('entities').get('user_mentions'):
                    #         mentions.append(mention.get('id'))
                    #     # Add all mentions as a list element to the list of field
                    #     list_of_field.append(mentions)
                    # except AttributeError:
                    #     try:
                    #         # If there are multiple mentions, iterate thru them
                    #         for mention in tweet.get('entities').get('user_mentions'):
                    #             mentions.append(mention.get('id'))
                    #         # Add all mentions as a list element to the list of field
                    #         list_of_field.append(mentions)
                    #     # If there are no mentions
                    #     except:
                    #         list_of_field.append("None")

                    # # Coordinates
                    # try:
                    #     list_of_field.append(tweet.get('coordinates').get('coordinates'))
                    # except AttributeError:
                    #     list_of_field.append("None")

                    # Country
                    try:
                        list_of_field.append(tweet.get('place').get('country'))
                    except AttributeError: # something to do with EOL char for each file. Dunno why. Might investigate later!
                        list_of_field.append("None")

                    # # Bounding Box - Point 1
                    # try:
                    #     list_of_field.append(tweet.get('place').get('bounding_box').get('coordinates')[0][0])
                    # except AttributeError:
                    #     list_of_field.append("None")

                    # # Bounding Box - Point 3
                    # try:
                    #     list_of_field.append(tweet.get('place').get('bounding_box').get('coordinates')[0][2])
                    # except AttributeError:
                    #     list_of_field.append("None")
                    
                    # # Full Text
                    # try: 
                    #     text = tweet.get('extended_tweet').get('full_text')
                    #     text_re = text.replace("\n"," ").replace("\r"," ").replace("\\n" , " ")
                    # except AttributeError:
                    #     try:
                    #         text = tweet.get('text')
                    #         text_re = text.replace("\n"," ").replace("\r"," ").replace("\\n" , " ")
                    #     except:
                    #         text_re = "None"
                    # list_of_field.append(text_re)

                    # Hashtags
                    hashtags = []
                    # If there are multiple hashtags, iterate thru them
                    try:
                        for hashtag in tweet.get('extended_tweet').get('entities').get('hashtags'):
                            hashtags.append(hashtag.get('text'))
                        # Add all hashtags as a list element to the list of field
                        list_of_field.append(hashtags)
                    except AttributeError:
                        try:
                            # If there are multiple mentions, iterate thru them
                            for hashtag in tweet.get('entities').get('hashtags'):
                                hashtags.append(hashtag.get('text'))
                            # Add all hashtags as a list element to the list of field
                            list_of_field.append(hashtags)
                        # If there are no hashtags
                        except:
                            list_of_field.append("None")

                    # Add this list to the bigger list
                    list_of_tweets.append(list_of_field)

    return list_of_tweets

# Parallelization Function (parallel read)
def parallel_read(path_list) -> list:
    '''
    param path_list (iterable): list of paths containing tweets
    return tweets (iterable): list containing several lists, each of which contain tweets as json objects
    '''
    # List containing File_num/N number of lists, each containing dictionaries of tweets
    tweets = []
    # Init parallel process
    with Pool() as pool_exec:
        # mapping the read_and_write function onto the list of file paths
        results = pool_exec.map(read_and_write , path_list)
        # Iterating through Generator Object
        for result in results:
            tweets.append(result)
    return tweets