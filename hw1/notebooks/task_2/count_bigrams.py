
from mrjob.job import MRJob
from collections import defaultdict

class MRSongCount(MRJob):
    def mapper(self, _, text):
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

    def reducer(self, character, values):
        character_max_replic = defaultdict(str)
        for replic in values:
            if len(character_max_replic[character]) < len(replic):
                character_max_replic[character] = replic

        for char, replic in character_max_replic.items():
            yield char, replic
        
if __name__ == "__main__":
    MRSongCount.run()
