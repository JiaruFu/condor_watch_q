import random
import os
import time

import htcondor

schedd = htcondor.Schedd()

home = os.path.expanduser("~")

t = str(int(time.time()))

for x in range(1, 6):
    log = os.path.join(home, t, "{}.log".format(x))

    # if x == 2:
    #     log = os.path.join(
    #         home,
    #         "deeply",
    #         "nested",
    #         "path",
    #         "to",
    #         "veryveryveryveryveryveryveryveryverylong",
    #         "{}.log".format(x),
    #     )
    # if x == 3:
    #     log = os.path.join(
    #         home,
    #         "deeply",
    #         "nested",
    #         "path",
    #         "to",
    #         "veryveryveryberrylong",
    #         "{}.log".format(x),
    #     )
    # if x == 4:
    #     log = None

    if log is not None:
        try:
            os.makedirs(os.path.dirname(log))
        except OSError:
            pass

    s = dict(
        executable = "/bin/sleep",
        arguments = "1",
        hold = False,
        transfer_input_files = "nope" if x == 4 else "",
        jobbatchname = "batch {}".format(x),
    )
    if log is not None:
        s["log"] = log

    sub = htcondor.Submit(s)
    with schedd.transaction() as txn:
        sub.queue(txn, random.randint(1, 5))
    with schedd.transaction() as txn:
        sub.queue(txn, random.randint(1, 5))

os.system("condor_q")
print()

# os.chdir(os.path.join(home, "deeply", "nested"))
os.chdir(os.path.expanduser("~"))
print("Running from", os.getcwd())
print("-" * 40)
