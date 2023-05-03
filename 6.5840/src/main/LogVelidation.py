
import os
import re
import subprocess

""" Log-based Tests """
def CheckWorkConserving(logPath):
    prefix = "GetTask: pending count: "
    with open(logPath, "r") as file:
        for line in file:
            if line[:len(prefix)] != prefix:
                continue
            
            pending_task_count = int(line[len(prefix):])
            if pending_task_count < 0:
                return False
    return True


def CheckRejoin(logPath):
    worker2State = {}
    disconnectCount = 0
    with open(logPath, "r") as file:
        for line in file:
            words = line.split()
            workerId = words[1]
            taskType = words[-1]
            if workerId not in worker2State:
                worker2State[workerId] = "connected"

            if "sleep" in line:
                worker2State[workerId] = "disconnected"
                disconnectCount += 1
                continue

            # only consider rejoin sucessful when the worker
            # receive a map or reduce task from the coordinator
            if worker2State[workerId] == "disconnected" \
                and (taskType == "0" or taskType == "1"):
                disconnectCount -= 1
                worker2State[workerId] = "connected"

    return disconnectCount == 0

def CheckDataLocality(logPath):
    with open(logPath, "r") as file:
        for line in file:
            if "Observed pending task locations:" in line:
                task_location = line[line.index("[")+1 : line.index("]")]
            elif "Scheduling decision:" in line:
                pattern = r"worker location -  (\d+) ; data location -  (\d+)"
                match = re.search(pattern, line)
                worker_loc = match.group(1)
                data_loc = match.group(2)

                # Data locality is not enforced only when it's not possible
                if worker_loc != data_loc:
                    task_list = task_location.split()
                    for loc in task_list:
                        if worker_loc == loc:
                            return False
    return True

        
def CheckTaskFailure(logPath):
    failCount = 0
    with open(logPath, "r") as file:
        for line in file:
            if "failed" in line:
                failCount += 1

            if "too many failures, kill the job" in line:
                return failCount == 3
            
    return False

""" Shell Script based Tests """
def CheckCorrectness(output):
    return "wc test: PASS" in output

def CheckFaultToleranceI(output):
    return "crash test: PASS" in output

if __name__ == "__main__":
    # In percentage
    score = 0

    # # Clear all environment variables and perform a general test
    if os.environ.get('TEST_WORK_RESERVING') is not None:
        del os.environ['TEST_WORK_RESERVING']
    if os.environ.get('TEST_REJOIN') is not None:
        del os.environ['TEST_REJOIN']
    if os.environ.get('TEST_LOC') is not None:
        del os.environ['TEST_LOC']
    if os.environ.get('TEST_TASK_FAIL') is not None:
        del os.environ['TEST_TASK_FAIL']

    print("Testing correctness:")
    result = subprocess.run(["bash", "test-correctness.sh"], stdout = subprocess.PIPE, stderr=subprocess.DEVNULL)
    result = result.stdout.decode('utf-8')
    if CheckCorrectness(result):
        score += 30
        print("Correctness Test: 30/30\n")
    else:
        print("Correctness Test: 0/30\n")

    print("Testing Fault Tolerance I:")
    result = subprocess.run(["bash", "test-crash.sh"], stdout = subprocess.PIPE, stderr=subprocess.DEVNULL)
    result = result.stdout.decode('utf-8')
    if CheckFaultToleranceI(result):
        score += 20
        print("Fault Tolerance I Test: 20/20\n")
    else:
        print("Fault Tolerance I Test: 0/20\n")

    # Run the MapReduce Application multiple times to generate log traces
    os.environ['TEST_WORK_RESERVING'] = '1'
    print("Generating traces for work-conserving test")
    subprocess.run(["bash", "wc-test.sh"], stdout = subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    del os.environ['TEST_WORK_RESERVING']
    if CheckWorkConserving("log_work_conserving"):
        score += 10
        print("Work Conserving Test: 10/10\n")
    else:
        print("Work Conserving Test: 0/10\n")

    os.environ['TEST_REJOIN'] = '1'
    print("Generating traces for rejoin test")
    subprocess.run(["bash", "wc-test.sh"], stdout = subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    del os.environ['TEST_REJOIN']
    if CheckRejoin("log_rejoin"):
        score += 20
        print("Rejoin Test: 20/20\n")
    else:
        print("Rejoin Test: 0/20\n")


    os.environ['TEST_LOC'] = '1'
    print("Generating traces for data locality test")
    subprocess.run(["bash", "wc-test.sh"], stdout = subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    del os.environ['TEST_LOC']
    if CheckDataLocality("log_locality"):
        score += 10
        print("Data Locality Test: 10/10\n")
    else:
        print("Data Locality Test: 0/10\n")

    os.environ['TEST_TASK_FAIL'] = '1'
    print("Generating traces for job failure test")
    subprocess.run(["bash", "wc-test.sh"], stdout = subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    del os.environ['TEST_TASK_FAIL']
    if CheckTaskFailure("log_task_failure"):
        score += 10
        print("Task Failure Test: 10/10\n")
    else:
        print("Task Failure Test: 0/10\n")

    if score == 100:
        print("All trace-based test passed! 100/100\n")
    else:
        print("Something goes wrong :-( {}/100".format(score))

