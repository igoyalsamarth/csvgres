import os
import random

def get_random_word() -> str:
    """
    Reads words from words.txt file in the same directory and returns a random word
    
    Returns:
        str: A randomly selected word from the file
    """
    # Get the directory of the current file
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Path to words.txt in same directory
    words_file = os.path.join(current_dir, 'words.txt')
    
    try:
        with open(words_file, 'r') as file:
            # Read all words and remove any empty lines
            words = [word.strip() for word in file.readlines() if word.strip()]
            
            if not words:
                raise ValueError("Words file is empty")
                
            # Return a random word
            return random.choice(words)
            
    except FileNotFoundError:
        raise FileNotFoundError(f"Could not find words.txt file at {words_file}")
