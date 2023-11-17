
from mrjob.job import MRJob

class MRSongCount(MRJob):
    def mapper(self, _, text):
        lines = text.split("\n")
        for line in lines:
            # skip head lines
            if len(line.split('" "')) != 3:
                continue
            _, character, replic = line.split('" "')
            yield (character, 1)

    def reducer(self, word, values):
        yield (word, sum(values))
        
if __name__ == "__main__":
    MRSongCount.run()
