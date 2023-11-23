
from mrjob.job import MRJob
from mrjob.step import MRStep

class MRSongCount(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_first,
                   reducer=self.reducer_first),
            MRStep(reducer=self.reducer_second)
        ]

    def mapper_first(self, _, text):
        lines = text.split("\n")
        for line in lines:
            # skip head lines
            if len(line.split('" "')) != 3:
                continue
            _, character, replic = line.split('" "')
            yield (character, 1)

    def reducer_first(self, word, values):
        yield None, (word, sum(values))

    def reducer_second(self, _, values):
        for key, val in sorted(values, key=lambda x: x[1], reverse=True)[:20]:
            yield key, val
        
if __name__ == "__main__":
    MRSongCount.run()
