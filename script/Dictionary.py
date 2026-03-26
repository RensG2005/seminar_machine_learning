import pandas as pd
import gensim.downloader as api

df = pd.read_csv('dictionary.csv') 

#Extract the 'Term' column and convert it to a standard Python list
seed_list = df['Term'].dropna().tolist()
print(f"Successfully loaded {len(seed_list)} seed words from the CSV.\n")

#Define the expansion function
def expand_dictionary(seed_words, top_n=5):
    model = api.load("glove-wiki-gigaword-100") 
    
    expanded_dict = {}
    
    print("\n--- Expansion Results ---")
    for word in seed_words:
        core_word = str(word).split()[-1].lower() 
        
        try:
            similar_words = model.most_similar(core_word, topn=top_n)
            word_list = [match[0] for match in similar_words]
            expanded_dict[word] = word_list
            print(f"Seed: '{word}' -> AI suggests: {', '.join(word_list)}")
            
        except KeyError:
            print(f"Seed: '{word}' -> '{core_word}' not found in the model's vocabulary.")

    return expanded_dict

# 4. Run the function using your newly loaded CSV list!
new_dictionary = expand_dictionary(seed_list, top_n=5)
