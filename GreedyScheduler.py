import sys
from Task import Task


class GreedyScheduler:
    dag = []
    ready_tasks = []
    curTasks = []
    assigned_task = []
    processors = {}
    proc_end_time = {}  # the end time of each processor
    typeTask = []
    inDegree = []
    runTime = []
    amount_per_type = []
    numTasks = 0
    num_first_tasks = 0
    curTime = 0
    numProcessorTypes = 0
    dependencies = [[]]

    def __init__(self, dag_file):

        dag_file = open(dag_file, 'r')
        self.analyze_dag(dag_file)  # initialize the dag details and the dependencies matrix
        self.initialize_inDegree()
        self.initialize_processors()
        self.update_readyTask()
        self.init_first_tasks()
        self.greedy_scheduling()
        self.print_result()

    def analyze_dag(self, dag_file):
        """
        creates the dag data structures. the first line is the number of types of processors there are.
        the second line is the amount of processors of each type. Third line - tha total number of tasks.
        Forth line - running times of each task. Fifth line - of which processor type each task can run.
        Sixth line until EOF - a route matrix, what tasks does each task points to.
        """
        line_count = 0  # in which line we are in each iteration
        matrix_line = 0  # count the number of the row that need to insert to the matrix
        lines = dag_file.readlines()
        num_of_row_column_dependencies = len(lines) - 7
        self.dependencies = [[0 for j in range(num_of_row_column_dependencies)] for i in
                             range(num_of_row_column_dependencies)]

        for line in lines:
            line_count += 1
            if line_count < 5:
                self.dag.append(line.split())
            if line_count == 6:
                self.dag.append(line.split())
                self.typeTask = line.split()
                self.typeTask = [int(string) for string in self.typeTask]
            if 7 < line_count < len(lines):
                self.initialize_dependencies(line, matrix_line)
                matrix_line += 1
        self.dag = [[int(i) for i in sublist] for sublist in self.dag]
        self.numTasks = int(self.dag[2][0])
        self.numProcessorTypes = int(self.dag[0][0])
        self.runTime = self.dag[3]
        self.amount_per_type = self.dag[1]

    def initialize_processors(self):
        e = 0
        for i in range(self.numProcessorTypes):
            self.processors[i] = []
            u = 0
            self.proc_end_time[i] = []
            while u < self.amount_per_type[e]:
                self.proc_end_time[i].append([0])

                self.processors[i].append([])
                u += 1
            e += 1

        '''
        for i in range(self.numProcessorTypes):
            p = Processor(i, 0, [])
            #p.assignedTasks.append([])
            self.processors[i] = [p]
            for j in self.amount_per_type:
                u = 0
                while u < j:
                    self.processors[i].assignedTasks.append([])
                    u += 1
        '''

    def initialize_dependencies(self, line, num_of_row):
        """
        Initialize the matrix, if the content in '1' - there is a path between the row number task to the
        column number task, zero else.
        """
        tasks = line.split()
        i = 1
        while i < len(tasks):
            self.dependencies[num_of_row][int(tasks[i])] = 1
            i += 1

    def initialize_inDegree(self):
        """
        sum the list of any task in the dependencies matrix to count how much dependencies there are for every task
        """
        for i in range(self.numTasks):
            sum = 0
            for j in range(self.numTasks):
                sum += self.dependencies[j][i]
            self.inDegree.append(sum)

    def update_readyTask(self):  # fixed to o(n)
        """
        all the tasks that are ready to run
        """
        i = 0
        while i < len(self.inDegree):
            if self.inDegree[i] == 0 and i not in self.ready_tasks: #possibley we can make it more efficient
                self.ready_tasks.append(i)
            i += 1

    def init_first_tasks(self):
        """the function initialize the first tasks of the ready tasks"""
        for i in self.ready_tasks:
            end, proc = self.minimal_processor_end_time(self.typeTask[i])
            if end <= self.curTime:
                task = Task(self.curTime, self.curTime + int(self.runTime[i]), i, self.typeTask[i])
                self.curTasks.append(task)
                self.insert_to_proc(task, self.typeTask[i], proc)
                self.update_proc_end_time(self.typeTask[i], proc, self.curTime + int(self.runTime[i]))
                self.assigned_task.append(i)

    def insert_to_proc(self, task, type, processor):
        self.processors[type][processor].append(task)

    def update_proc_end_time(self, type, processor, end_time):
        self.proc_end_time[type][processor][0] = end_time

    def minimal_processor_end_time(self, key):
        '''This function iterate all over the processor from a specific type and return the minimal end time of a
        processor from this type'''
        processor = 0
        minimal = 0
        l = self.proc_end_time[key]
        minimal = l[0][0]
        if len(l) == 1:
            return minimal, 0
        i = 1
        while i < len(l):
            if l[i][0] < minimal:
                minimal = l[i][0]
                processor = i
            i += 1
        return minimal, processor

    def greedy_scheduling(self):

        while self.curTasks:
            min_end_time, index = self.extract_min()
            self.curTime = min_end_time
            self.update_inDegree(index)
            self.update_readyTask()
            for i in self.ready_tasks:
                if i not in self.assigned_task:
                    end, proc = self.minimal_processor_end_time(self.typeTask[i])
                    if end <= self.curTime:
                        task = Task(self.curTime, self.curTime + int(self.runTime[i]), i, self.typeTask[i])
                        self.curTasks.append(task)
                        self.insert_to_proc(task, self.typeTask[i], proc)
                        self.update_proc_end_time(self.typeTask[i], proc, self.curTime + int(self.runTime[i]))
                        self.assigned_task.append(i)

    def extract_min(self):
        """remove the task with the minimum end time from cur tasks"""
        min_end = self.curTasks[0].end
        t = 0  # the index of the minimum end time task
        index = self.curTasks[0].index
        i = 1

        while i < len(self.curTasks):
            if self.curTasks[i].end < min_end:
                min_end = self.curTasks[i].end
                index = self.curTasks[i].index
                t = i
            i += 1
        del self.curTasks[t]
        self.ready_tasks.remove(index)
        return min_end, index

    def update_inDegree(self, index):
        self.inDegree[index] -= 1
        counter = 0
        for i in self.dependencies[index]:
            if i == 1:
                self.inDegree[counter] -= 1
                counter += 1
            else:
                counter += 1

    def print_result(self):
        time = 0
        first = True
        for key, value in self.processors.items():
            num = 0
            for l in value:
                print("Processor from type %d number %d:\n" % (key, num))
                num += 1
                if l:#check if the list is not empty
                    for task in l:
                        if first == True:
                            time = task.end
                            first = False
                        elif task.end > time:
                            time = task.end
                        print("    task %d:" % task.index)
                        print("        start-time: %d" % task.start)
                        print("        end-time: %d\n" % task.end)
        print("The running time of this algorithm is: %d" % time)
        
GreedyScheduler(sys.argv[1])
