from utils import *

# Reading & Writing ND JSON files
def read_and_write(path) -> [list]:
    '''
    param path (str): directory where the input json file is
    return list_of_tweets (iterable): list containing tweets as json objects with relevant data read in from the file(s) within path input
    '''
    # List to store fields from the tweets
    list_of_tweets = []

    with zipfile.ZipFile(path , 'r') as infile:
        for file_name in infile.namelist():                
            # this goes inside the geoEurope folder
            with infile.open(file_name) as f:                    
                # iterate through each line , i.e. each tweet object
                for line in f:                        
                    tweet = json.loads(line)
                    
                    # row-wise inclusion into a sub-list
                    list_of_field = []
                    try:
                        list_of_field.append(tweet.get('user').get('id')) # use id only if this takes up more mem/time
                    except AttributeError:
                        list_of_field.append(None)
                    # try:
                    #     list_of_field.append(tweet.get('coordinates').get('coordinates'))
                    # except AttributeError:
                    #     list_of_field.append(None)
                    # try:
                    #     list_of_field.append(tweet.get('place').get('bounding_box').get('coordinates')[0][0])
                    # except AttributeError:
                    #     list_of_field.append(None)
                    # try:
                    #     list_of_field.append(tweet.get('place').get('bounding_box').get('coordinates')[0][1])
                    # except AttributeError:
                    #     list_of_field.append(None)
                    # try:
                    #     list_of_field.append(tweet.get('place').get('bounding_box').get('coordinates')[0][2])
                    # except AttributeError:
                    #     list_of_field.append(None)
                    # try:
                    #     list_of_field.append(tweet.get('place').get('bounding_box').get('coordinates')[0][3])
                    # except AttributeError:
                    #     list_of_field.append(None)
                    # try:
                    #     list_of_field.append(tweet.get('entities').get('user_mentions').get('id'))
                    # except AttributeError:
                    #     list_of_field.append(None)
                    list_of_field.append(tweet.get('timestamp_ms'))
                    list_of_field.append(tweet.get('id'))
                    
                    # Add this list to the bigger list
                    list_of_tweets.append(list_of_field)

    return list_of_tweets

# Parallelization Function (parallel read)
def parallel_read(path_list) -> [list]:
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