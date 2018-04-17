import multiprocessing
import fileinput
import os
DIRS = {'input':'input', 'mapper':'temp_mapper', 'reducer':'temp_reducer', 'output':'output'}

class Mapper(multiprocessing.Process):
    def __init__(self, index):
        super(Mapper, self).__init__()
        self.index = index
        # multiprocessing.Process.init()

    def map(self, element):
        pass

    def combine(self, mapper_results):
        pass

    def shuffle(self, mapper_results):



    def run(self):
        for filename in os.listdir(DIRS['input']):
            if filename.split('.').endswith(str(self.index)):
                with open(filename, 'r') as file:
                    mapper_results = self.map(file)






class Reducer(multiprocessing.Process):
    def reduce(self, key, value):
        pass
    def run(self):
        pass

class Job():
    def __init__(self, input_files, num_mappers, num_reducers, dirs):
        self.input_files = input_files
        self.num_mappers = num_mappers
        self.num_reducers = num_reducers
        self.dirs  = dirs
    def run(self):
        fhandler = FileHandler()
        fhandler.split_files(self.input_files, self.num_mappers, self.dirs)
        map_workers = []
        rdc_workers = []

        for i in range(self.num_mappers):
            m = Mapper(i)
            map_workers.append(m)
            m.start()
        [t.join() for t in map_workers]
        # run the reduce step
        for thread_id in range(self.num_reducers):
            r = Reducer()
            rdc_workers.append(r)
            r.start()
        [t.join() for t in rdc_workers]
        fhandler.join_files(self.dirs)


class FileHandler():
    def split_files(self, filenames, num_splits, dirs):
        files = [open(dirs['input'] + '/input_%d.txt' % i, 'w') for i in range(num_splits)]
        for filename in filenames:
            with open(filename, 'r') as infp:
                for i, line in enumerate(infp):
                    files[i % num_splits].write(line)
        for f in files:
            f.close()

    def join_files(self, dirs):
        with open(dirs['output'] + '/result.txt', 'w') as out_file:
            f_to_join = [f for f in os.listdir(dirs['reducer'])]
            allfiles = fileinput.input(f_to_join)
            for l in allfiles:
                out_file.write(l)



