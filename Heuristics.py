import sys
from Task import Task
import random

class OptimalScheduling:
    dag = []  # *
    ready_tasks = {}  # *
    curTasks = []  # *
    assigned_task = []  # *
    processors = {}  # *
    proc_end_time = {}  # the end time of each processor #*
    typeTask = []  # *
    priority = []  # 0 for a normal task. 1 fot an urgent task#*
    inDegree = []  # *
    runTime = []  # *
    idle_tasks = []
    try_list = []
    last = []
    tasks = {}  # Temporary#*
    exposures = {}  # *
    amount_per_type = []  # *
    assigned_exp = 0  # *
    numTasks = 0  # *
    curTime = 0  # *
    numProcessorTypes = 0  # *
    tot_run_time = 0
    origin_run_time = 0
    dependencies = [[]]  # *
    file = ""

    def __init__(self, dag_file):
        '''
        self.create_dag(100)
        self.initialize_ready_task()
        self.initialize_inDegree()
        self.update_readyTask()
        self.init_first_tasks()
        self.greedy_scheduling()
        self.print_result()
        '''
        self.file = open("data.txt", 'r')
        dag_filee = open(dag_file, 'r')
        self.analyze_dag(dag_filee)  # initialize the dag details and the dependencies matrix
        self.initialize_ready_task()
        self.initialize_priority()
        self.initialize_inDegree()
        print("in degree:")
        print(self.inDegree)
        self.initialize_processors()
        self.update_readyTask()
        print("ready tasks:")
        print(self.ready_tasks)
        print(self.dependencies)
        self.init_first_tasks()
        self.greedy_scheduling()
        print("Greedy Scheduling result: ")
        self.print_result()
        self.origin_run_time = self.tot_run_time
        self.idle()
        self.idle_urgent(dag_file)
        self.vvv(dag_file)
        lines = self.file.readlines()
        for item in self.idle_tasks:
            deps = []
            for line in lines:
                if line == "\n":
                    break
                line = line.split()
                line = list(map(int, line))
                task = line[0]
                i = 0
                while i < len(line) - 1:
                    i += 1
                    if line[i] == item:
                        deps.append(task)
            print()
            print()
            print("When the dependencies of task %d are urgent, the running time is:" % item)
            self.heuristic(dag_file, deps)

        print()
        print()
        print("When all the tasks that made the running time be lower became urgent:")
        print("-----------------------------------------------------------------------------------")
        self.try_times(dag_file)

        print()
        print()
        print()
        print("These are the tasks that reduces the running time:")
        print(self.last)

        print("Final runtime after improvements:")
        self.final_running_time(dag_file)

    def vvv(self, dag_file):
        dag_file = open(dag_file, 'r')
        self.analyze_dag(dag_file)  # initialize the dag details and the dependencies matrix

        for i in range(0, self.numTasks):
            self.curTime = 0
            self.curTasks = []
            self.assigned_task = []
            self.initialize_ready_task()
            self.initialize_priority_two(i)
            self.initialize_inDegree()
            self.initialize_processors()
            self.update_readyTask()
            self.init_first_tasks()
            self.greedy_scheduling()
            print()
            print()
            print("when task number %d is urgent" % i)
            self.print_result()

    def final_running_time(self, dag_file):
        dag_file = open(dag_file, 'r')
        self.analyze_dag(dag_file)  # initialize the dag details and the dependencies matrix
        self.curTime = 0
        self.curTasks = []
        self.assigned_task = []
        self.initialize_ready_task()
        self.initialize_priority_two(self.last)
        self.initialize_inDegree()
        self.initialize_processors()
        self.update_readyTask()
        self.init_first_tasks()
        self.greedy_scheduling()
        self.print_result()


    def try_times(self, dag_file):
        dag_file = open(dag_file, 'r')
        self.analyze_dag(dag_file)  # initialize the dag details and the dependencies matrix

        for i in self.try_list:
            self.curTime = 0
            self.curTasks = []
            self.assigned_task = []
            self.initialize_ready_task()
            self.initialize_priority_two(i)
            self.initialize_inDegree()
            self.initialize_processors()
            self.update_readyTask()
            self.init_first_tasks()
            self.greedy_scheduling()
            print()
            print()
            print("when task number %d is urgent" % i)
            self.print_result()
            if self.origin_run_time > self.tot_run_time:
                self.last.append(i)

    def idle_urgent(self, dag_file):
        dag_file = open(dag_file, 'r')
        self.analyze_dag(dag_file)  # initialize the dag details and the dependencies matrix
        self.curTime = 0
        self.curTasks = []
        self.assigned_task = []
        self.initialize_ready_task()
        self.initialize_priority_two(self.idle_tasks)
        self.initialize_inDegree()
        self.initialize_processors()
        self.update_readyTask()
        self.init_first_tasks()
        self.greedy_scheduling()
        print("When all the idle tasks are urgent the running time is:")
        self.print_result()


    def heuristic(self, dag_file, line):
        self.clear_data_strucs(dag_file, line)

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
        self.inDegree = []
        for i in range(self.numTasks):
            sum = 0
            for j in range(self.numTasks):
                sum += self.dependencies[j][i]
            self.inDegree.append(sum)

    def initialize_exposures(self):
        """
        This function creates deme tasks with run time of the time that the each task takes and insert them to the cur
        tasks. It will make the exposures vertex to be ready when they should.
        """
        self.exposures[3] = 3
        self.exposures[6] = 14
        self.exposures[8] = 23
        self.exposures[12] = 26

    def initialize_ready_task(self):
        self.ready_tasks["normal"] = []
        self.ready_tasks["urgent"] = []

    def update_readyTask(self):
        """
        all the tasks that are ready to run
        """
        i = 0
        while i < len(self.inDegree):
            if self.inDegree[i] == 0 and i not in self.ready_tasks:
                if self.priority[i] == 0:
                    self.ready_tasks["normal"].append(i)
                else:
                    self.ready_tasks["urgent"].append(i)
            i += 1

    def initialize_priority(self):
        self.priority = [0] * self.numTasks

    def initialize_priority_two(self, list):
        self.priority = [0] * self.numTasks
        if isinstance(list, int):
            self.priority[list] = 1
        else:
            i = 0
            while i < len(list):
                self.priority[list[i]] = 1
                i += 1

    def init_first_tasks(self):
        """the function initialize the first tasks of the ready tasks"""
        for i in self.ready_tasks["urgent"]:
            end, proc = self.minimal_processor_end_time(self.typeTask[i])
            if end <= self.curTime:
                task = Task(self.curTime, self.curTime + int(self.runTime[i]), i, self.typeTask[i])
                self.curTasks.append(task)
                self.insert_to_proc(task, self.typeTask[i], proc)
                self.update_proc_end_time(self.typeTask[i], proc, self.curTime + int(self.runTime[i]))
                self.assigned_task.append(i)

        for i in self.ready_tasks["normal"]:
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
            for i in self.ready_tasks["urgent"]:
                if i not in self.assigned_task:
                    end, proc = self.minimal_processor_end_time(self.typeTask[i])
                    if end <= self.curTime:
                        task = Task(self.curTime, self.curTime + int(self.runTime[i]), i, self.typeTask[i])
                        self.curTasks.append(task)
                        self.insert_to_proc(task, self.typeTask[i], proc)
                        self.update_proc_end_time(self.typeTask[i], proc, self.curTime + int(self.runTime[i]))
                        self.assigned_task.append(i)
            for i in self.ready_tasks["normal"]:
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
        if self.priority[index] == 0:
            self.ready_tasks["normal"].remove(index)
        else:
            self.ready_tasks["urgent"].remove(index)
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


    def is_exposure_ready(self):
        if self.curTime < self.exposures[self.assigned_exp]:
            return
        keys = list(self.exposures.keys())
        index = keys[self.assigned_exp]  # the exposure we assigning now
        self.ready_tasks["urgent"].append(index)
        self.assigned_exp += 1


    def hiuristic(self):
        for i in self.idle_tasks:
            self.priority[i] = 1

    def idle(self):

        print()
        print()

        proc_type = 0
        for value in self.processors.values():
            proc_num = 0
            for list in value:
                if len(list) < 2:
                    continue
                for item in range(len(list) - 1):
                    if item == 0 and list[item].start != 0:
                        self.idle_tasks.append(list[item].index)#maybe unnecasery
                    if list[item].end != list[item + 1].start:
                        self.idle_tasks.append(list[item + 1].index)
                proc_num += 1
            proc_type += 1

    def clear_data_strucs(self, dag_file, line):
        dag_file = open(dag_file, 'r')
        self.curTime = 0
        self.curTasks = []
        self.assigned_task = []
        self.analyze_dag(dag_file)  # initialize the dag details and the dependencies matrix
        self.initialize_ready_task()
        self.initialize_priority_two(line)
        self.initialize_inDegree()
        self.initialize_processors()
        self.update_readyTask()
        self.init_first_tasks()
        self.greedy_scheduling()
        self.print_result()
        if self.origin_run_time > self.tot_run_time:
            for i in line:
                if i not in self.try_list:
                    self.try_list.append(i)



    def print_result(self):
        time = 0
        self.tot_run_time = 0
        first = True
        for key, value in self.processors.items():
            num = 0
            for l in value:
                num += 1
                if l:  # check if the list is not empty
                    for task in l:
                        if first == True:
                            time = task.end
                            first = False
                        elif task.end > time:
                            time = task.end
        self.tot_run_time = time
        print("The running time of this algorithm is: %d" % self.tot_run_time)

OptimalScheduling(sys.argv[1])