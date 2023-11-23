
from mrjob.job import MRJob
from mrjob.step import MRStep
from collections import defaultdict

class MRSongCount(MRJob):
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper_first,
                   reducer=self.reducer_first),
            MRStep(reducer=self.reducer_second)
        ]

    def mapper_first(self, _, text):
        lines = text.split("\n")
        character_max_replic = defaultdict(str)
        for line in lines:
            # skip head lines
            if len(line.split('" "')) != 3:
                continue
            _, character, replic = line[:-1].split('" "')
            if len(character_max_replic[character]) < len(replic):
                character_max_replic[character] = replic

        for char, replic in character_max_replic.items():
            yield char, replic

    def reducer_first(self, character, values):
        character_max_replic = defaultdict(str)
        for replic in values:
            if len(character_max_replic[character]) < len(replic):
                character_max_replic[character] = replic

        for char, replic in character_max_replic.items():
            yield None, (char, replic)

    def reducer_second(self, _, values):
        for key, val in sorted(values, key=lambda x: len(x[1])):
            yield key, val
        
if __name__ == "__main__":
    MRSongCount.run()
