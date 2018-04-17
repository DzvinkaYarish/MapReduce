import multiprocessing
import fileinput
import os
DIRS = {'input': 'input/', 'mapper': 'temp_mapper/', 'reducer': 'temp_reducer/', 'output': 'output/'}

# temp_mapper/reducer_1/mapper_1.txt, mapper_2.txt

with open('configurations.txt') as file:
    NUM_MAPPERS = int(file.readline().split('=')[1].strip())
    NUM_REDUCERS = int(file.readline().split('=')[1].strip())
    DATA_DIR = file.readline().split('=')[1].strip()
    COMBINE = bool(file.readline().split('=')[1].strip())


class Mapper(multiprocessing.Process):
    def __init__(self, index):
        super(Mapper, self).__init__()
        self.index = index

    def __find_reducer(self, key):
        return hash(key) % NUM_REDUCERS

    def map(self, element):
        pass

    def combine(self, mapper_results):
        pass

    def shuffle(self, mapper_results):
        for rdr_i in range(NUM_REDUCERS):
            if not os.path.exists('{}reducer_{}/'.format(DIRS['mapper'], rdr_i)):
                os.makedirs('{}reducer_{}/'.format(DIRS['mapper'], rdr_i))
            file_for_ith_reducer = open('{}reducer_{}/mapper_{}.txt'.format(DIRS['mapper'], rdr_i, self.index), 'w+')
            [file_for_ith_reducer.write(key + '\t' + str(value) + '\n') for (key, value) in mapper_results if rdr_i == self.__find_reducer(key)]
            file_for_ith_reducer.close()

    def run(self):
        for filename in os.listdir(DIRS['input']):
            if filename.split('.')[0].endswith(str(self.index)):
                with open(DIRS['input'] + filename, 'r') as file:
                    mapper_results = self.map(file.readlines())

                    if COMBINE:
                       mapper_results = self.combine(mapper_results)
                    self.shuffle(mapper_results)
                break


class Reducer(multiprocessing.Process):
    def __init__(self, index):
        super(Reducer, self).__init__()
        self.index = index
        self.map = {}

    def reduce(self, key, value):
        pass

    def run(self):
        for filename in os.listdir('{}reducer_{}/'.format(DIRS['mapper'], self.index)):
            with open('{}reducer_{}/'.format(DIRS['mapper'], self.index) + filename, 'r') as file:
                for line in file.readlines():
                    self.reduce(line.split('\t')[0], line.split('\t')[1])
        with open('{}output_{}.txt'.format(DIRS['reducer'], self.index), 'w+') as file:
            for (key, value) in self.map.items():
                file.write(key + '\t' + str(value) + '\n')


class Job:

    def run(self, mapper_name, reducer_name):
        for dir in DIRS:
            if not os.path.exists(DIRS[dir]):
                os.makedirs(DIRS[dir])
        fhandler = FileHandler()
        fhandler.split_files()
        map_workers = []
        rdc_workers = []

        for map_indx in range(NUM_MAPPERS):
            m = mapper_name(map_indx)
            map_workers.append(m)
            m.start()
        [t.join() for t in map_workers]
        # run the reduce step
        for rdc_indx in range(NUM_REDUCERS):
            r = reducer_name(rdc_indx)
            rdc_workers.append(r)
            r.start()
        [t.join() for t in rdc_workers]
        fhandler.join_files()


class FileHandler():
    def split_files(self):
        files = [open(DIRS['input'] + 'input_%d.txt' % i, 'w+') for i in range(NUM_MAPPERS)]
        for filename in os.listdir(DATA_DIR):
            with open(DATA_DIR + filename, 'r') as infp:
                for i, line in enumerate(infp):
                    files[i % NUM_MAPPERS].write(line)
        for f in files:
            f.close()

    def join_files(self):
        with open('{}result.txt'.format(DIRS['output']), 'w+') as out_file:
            f_to_join = [DIRS['reducer'] + f for f in os.listdir(DIRS['reducer'])]
            allfiles = fileinput.input(f_to_join)
            for l in allfiles:
                out_file.write(l)



