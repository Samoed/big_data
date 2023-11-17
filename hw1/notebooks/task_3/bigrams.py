from mrjob.job import MRJob
from collections import Counter
import nltk
from nltk.util import ngrams
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import string
from nltk.stem import WordNetLemmatizer

class MRSongCount(MRJob):
    def mapper_init(self):
        nltk.download('punkt')
        nltk.download('stopwords')
        nltk.download('wordnet')
        

    def mapper(self, _, text):
        lines = text.split("\n")
        replics = []
        for line in lines:
            # skip head lines
            if len(line.split('" "')) != 3:
                continue
            _, character, replic = line[:-1].split('" "')
            replics.append(replic)
        text = (" ".join(replics)).lower()
        text = text.translate(str.maketrans("", "", string.punctuation))
        tokens = word_tokenize(text)
        lemmatizer = WordNetLemmatizer()
        lemmatized_tokens = [lemmatizer.lemmatize(word) for word in tokens]

        stop_words = set(stopwords.words('english'))
        
        filtered_tokens = [word for word in lemmatized_tokens if word not in stop_words]
        bigrams = list(ngrams(filtered_tokens, 2))
    
        for words, replic in Counter(bigrams).items():
            return_word = " ".join(words)
            yield return_word, replic

    def reducer(self, word, values):
        yield (word, sum(values))
        
if __name__ == "__main__":
    MRSongCount.run()
