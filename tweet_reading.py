# Necessary imports
from helper_func_2 import parallel_read
from utils import *

if __name__ == "__main__":
    # Paths
    base_path = os.path.join(os.getcwd() , in_folder) # from utils
    path_list = [os.path.join(base_path , file_name) for file_name in os.listdir(base_path)]
    
    # Get start time
    start = time.perf_counter()
    
    # Run the function
    parallel_lists = parallel_read(path_list)

    # File to store the parsed data!
    # Remove if already present
    write_file_path = out_file_for_hashtag # from utils
    if os.path.isfile(write_file_path):
        os.remove(write_file_path)

    # Write all the tweets into a tractable text file
    with open(write_file_path , 'wb') as f:
        for list_of_tweets in parallel_lists:
            for tweet in list_of_tweets:
                # Write the tweet followed by new-line char
                write_stuff = ('|'.join(map(str, tweet))).encode('utf-8') # need encoding to bytes to deal with non-english chars
                f.write(write_stuff)
                f.write('\n'.encode('utf-8')) # need encoding to bytes to deal with non-english chars

    # Get end time
    finish = time.perf_counter()
    print(f'Finished in {finish-start} seconds')
    # Finished in 134.14955680000094 seconds
    # Finished in ~470 seconds