import datetime
import re

from schedtools.shell_handler import ShellHandler

def parse_jobs(data):
    if not isinstance(data, list):
        data = data.split("\n")
    jobs = []
    current_job = {}
    current_key = ""
    current_indent = 0
    for line in data:
        if not len(line.strip()):
            continue
        if line.startswith("Job Id: "):
            if current_job:
                jobs.append(current_job)
            current_job = {"id": re.findall("(?<=Job Id: )[0-9]+", line.strip())[0]}
            current_key = ""
            current_indent = 0
        elif " = " in line:
            indent = line.index(" ") - len(line.lstrip('\t'))
            if indent > current_indent:
                current_job[current_key] += "\n" + line.strip()
                current_indent = indent
            else:
                key, val = line.strip().split(" = ")
                current_job[key] = val
                current_key = key
                current_indent = 0
        else:
            current_job[current_key] += "\n" + line.strip()
    jobs.append(current_job)
    return jobs

def parse_job_percentage(data):
    if not isinstance(data, list):
        data = data.split("\n")
    assert tuple(data[0].strip().split())==('Job', 'id', 'Name', 'User', '%', 'done', 'S', 'Queue')
    running_jobs = {}
    for i in range(2, len(data)):
        line = data[i].strip().split()
        if len(line):
            id_ = line[0][:-4]
            pc = line[-3]
            if pc != '(null)':
                running_jobs[id_] = float(pc.replace("%", ""))
    return running_jobs

def rerun_jobs(handler, threshold=95, log=False, **kwargs):
    """Rerun PBS jobs where elapsed time is greater than threshold (%).
    
    kwargs are provided to pass e.g. passwords to the created handler instance 
    without needing them stored anywhere.
    """
    if log:
        msg = "Rerun task running at {}".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        if isinstance(log,str):
            with open(log, "a") as f:
                f.write(msg+"\n")
        else:
            print(msg)
    if not isinstance(handler, ShellHandler):
        handler = ShellHandler(handler, **kwargs)
    _, stats, _ = handler.execute("qstat -p")
    running_jobs = parse_job_percentage()
    # if verbose:
    #     print("Watching jobs:")
    #     for k,v in running_jobs.items():
    #         print(f"{k}: {v:.1f}%")
    to_rerun = [k for k,v in running_jobs if v >= threshold]
    if len(to_rerun):
        handler.execute(f"qrerun {' '.join(to_rerun)}")