# Necessary imports
from helper_func_2 import parallel_read
from utils import *

if __name__ == "__main__":
    # Paths
    base_path = os.path.join(os.getcwd() , 'TwitterJune2022')
    path_list = [os.path.join(base_path , file_name) for file_name in os.listdir(base_path)]
    
    # Get start time
    start = time.perf_counter()
    
    # Run the function
    parallel_lists = parallel_read(path_list)
    
    # File to store the parsed data!
    # Remoe if already present
    write_file_path = 'tweets_large.txt'
    if os.path.isfile(write_file_path):
        os.remove(write_file_path)

    # Write all the tweets into a tractable text file
    with open(write_file_path , 'a') as out_file:
        for list_of_tweets in parallel_lists:
            for tweet in list_of_tweets:
                # Write the tweet followed by new-line char
                out_file.write(','.join(map(str, tweet)))
                out_file.write('\n')

    # Get end time
    finish = time.perf_counter()
    print(f'Finished in {finish-start} seconds')