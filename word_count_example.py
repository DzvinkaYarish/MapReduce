import itertools
import map_reduce


class WordCountMapper(map_reduce.Mapper):
    def __init__(self, index):
        map_reduce.Mapper.__init__(self, index)

    def map(self, element):
        results = []
        for line in element:
            values = line.split()
            results.extend([(value, 1) for value in values if value.isalpha()])
            print(values)
        return results

    def combine(self,  mapper_results):
        return [(sum(i[1] for i in group), key) for key, group in
            itertools.groupby(sorted(mapper_results, key=lambda i: i[0]), lambda i: i[1])]


class WordCountReducer(map_reduce.Reducer):
    def __init__(self, index):
        map_reduce.Reducer.__init__(self, index)

    def reduce(self, key, value):
        if key in self.map:
            self.map[key] += value
        else:
            self.map[key] = value


if __name__ == "__main__":
    word_count_job = map_reduce.Job()
    word_count_job.run(WordCountMapper, WordCountReducer)








