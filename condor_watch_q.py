#!/usr/bin/env python

# Copyright 2019 HTCondor Team, Computer Sciences Department,
# University of Wisconsin-Madison, WI.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import print_function

import argparse
import collections
import getpass
import itertools
import sys
import textwrap
import enum
import datetime
import os
import time
import operator

import htcondor
import classad


def parse_args():
    parser = argparse.ArgumentParser(
        prog="condor_watch_q",
        description=textwrap.dedent(
            """
            Track the status of HTCondor jobs over time without repeatedly 
            querying the Schedd.
            
            If no users, cluster ids, or event logs are given, condor_watch_q will 
            default to tracking all of the current user's jobs.
            
            Any option beginning with a single "-" can be specified by its unique
            prefix. For example, these commands are all equivalent:
            
                condor_watch_q -clusters 12345
                condor_watch_q -clu 12345
                condor_watch_q -c 12345
                
            By default, condor_watch_q will not exit on its own. You can tell it
            to exit when certain conditions are met. For example, to exit when
            all of the jobs it is tracking are done (with exit code 0) or when any
            job is held (with exit code 1), run
            
                condor_watch_q -exit all done 0 -exit any held 1
            """
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        add_help=False,
    )

    parser.add_argument(
        "-help",
        action="help",
        default=argparse.SUPPRESS,
        help="Show this help message and exit.",
    )

    # select which jobs to track
    parser.add_argument(
        "-users", nargs="+", metavar="USER", help="Which users to track."
    )
    parser.add_argument(
        "-clusters", nargs="+", metavar="CLUSTER_ID", help="Which cluster IDs to track."
    )
    parser.add_argument(
        "-files", nargs="+", metavar="FILE", help="Which event logs to track."
    )
    parser.add_argument(
        "-batches", nargs="+", metavar="BATCH_NAME", help="Which batch names to track."
    )

    # select when (if) to exit
    parser.add_argument(
        "-exit",
        action=ExitConditions,
        metavar="GROUPER,JOB_STATUS[,EXIT_CODE]",
        help=textwrap.dedent(
            """
            Specify conditions under which condor_watch_q should exit. 
            To specify additional conditions, pass this option again.
            
            GROUPER is one of {{{}}}.
            
            JOB_STATUS is one of {{{}}}.
            
            "active" means "in the queue".
            
            To specify multiple exit conditions, pass this option multiple times.
            """.format(
                ", ".join(EXIT_GROUPERS.keys()), ", ".join(EXIT_JOB_STATUS_CHECK.keys())
            )
        ),
    )

    parser.add_argument(
        "-abbreviate",
        action="store_true",
        help="Abbreviate path components to the shortest unique prefix.",
    )

    parser.add_argument(
        "-groupby",
        action="store",
        default="batch",
        choices=("batch", "log", "cluster"),
        help="Select what to group jobs by.",
    )

    parser.add_argument(
        "-debug", action="store_true", help="Turn on HTCondor debug printing."
    )

    args = parser.parse_args()

    return args


class ExitConditions(argparse.Action):
    def __call__(self, parser, args, values, option_string=None):
        v = values.split(",")
        if len(v) == 3:
            grouper, status, exit_code = v
        elif len(v) == 2:
            grouper, status = v
            exit_code = 0
        else:
            parser.error(message="invalid -exit specification")

        if grouper not in EXIT_GROUPERS:
            parser.error(
                message='invalid GROUPER "{}", must be one of {{ {} }}'.format(
                    grouper, ", ".join(EXIT_GROUPERS.keys())
                )
            )

        if status not in EXIT_JOB_STATUS_CHECK:
            parser.error(
                message='invalid JOB_STATUS "{}", must be one of {{ {} }}'.format(
                    status, ", ".join(EXIT_JOB_STATUS_CHECK.keys())
                )
            )

        try:
            exit_code = int(exit_code)
        except ValueError:
            parser.error(
                message='EXIT_CODE must be an integer, but was "{}"'.format(exit_code)
            )

        if getattr(args, self.dest, None) is None:
            setattr(args, self.dest, [])

        getattr(args, self.dest).append((grouper, status, exit_code))


def cli():
    args = parse_args()

    if args.debug:
        print("Enabling HTCondor debug output...", file=sys.stderr)
        htcondor.enable_debug()

    return watch_q(
        users=args.users,
        cluster_ids=args.clusters,
        event_logs=args.files,
        batches=args.batches,
        exit_conditions=args.exit,
        abbreviate_path_components=args.abbreviate,
        groupby=args.groupby,
    )


EXIT_GROUPERS = {"all": all, "any": any, "none": lambda _: not any(_)}
EXIT_JOB_STATUS_CHECK = {
    "active": lambda s: s in ACTIVE_STATES,
    "done": lambda s: s is JobStatus.COMPLETED,
    "idle": lambda s: s is JobStatus.IDLE,
    "held": lambda s: s is JobStatus.HELD,
}


def watch_q(
        users=None,
        cluster_ids=None,
        event_logs=None,
        batches=None,
        exit_conditions=None,
        abbreviate_path_components=False,
        groupby="log",
):
    if users is None and cluster_ids is None and event_logs is None:
        users = [getpass.getuser()]
    if exit_conditions is None:
        exit_conditions = []

    cluster_ids, event_logs, batch_names = find_job_event_logs(
        users, cluster_ids, event_logs, batches,
    )
    if cluster_ids is not None and len(cluster_ids) == 0:
        print("No jobs found")
        sys.exit(0)

    tracker = JobStateTracker(event_logs, batch_names)

    # TODO: this should happen during argument parsing
    groupby = {
        'log': 'event_log_path',
        'cluster': 'cluster_id',
        'batch': 'batch_name',
    }[groupby]

    exit_checks = []
    for grouper, checker, exit_code in exit_conditions:
        disp = "{} {}".format(grouper, checker)
        exit_grouper = EXIT_GROUPERS[grouper.lower()]
        exit_check = EXIT_JOB_STATUS_CHECK[checker.lower()]
        exit_checks.append((exit_grouper, exit_check, exit_code, disp))

    try:
        msg = None

        while True:
            reading = "Reading new events..."
            print(reading, end="")
            sys.stdout.flush()
            processing_messages = tracker.process_events()
            print("\r" + (len(reading) * " ") + "\r", end="")
            sys.stdout.flush()

            if msg is not None:
                prev_lines = list(msg.splitlines())
                prev_len_lines = [len(line) for line in prev_lines]

                move = "\033[{}A\r".format(len(prev_len_lines))
                clear = "\n".join(" " * len(line) for line in prev_lines) + "\n"
                sys.stdout.write(move + clear + move)
                sys.stdout.flush()

            now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            if len(processing_messages) > 0:
                print(
                    "\n".join("{}  {}".format(now, m) for m in processing_messages),
                    file=sys.stderr,
                )

            summary, msg = table_by(tracker.clusters, groupby, abbreviate_path_components=abbreviate_path_components)
            msg = msg.splitlines()
            msg += ["", "Updated at {}".format(now)]
            msg = "\n".join(msg)
            srm = "\n{} jobs; {} completed, {} removed, {} idle, {} running, {} held, {} suspended".format(summary[TOTAL], summary[JobStatus.COMPLETED], summary[JobStatus.REMOVED], summary[JobStatus.IDLE], summary[JobStatus.RUNNING], summary[JobStatus.HELD], summary[JobStatus.SUSPENDED])
            msg += srm
            
            print(msg)

            for grouper, checker, exit_code, disp in exit_checks:
                if grouper((checker(s) for s in tracker.job_states)):
                    print(
                        'Exiting with code {} because of condition "{}" at {}'.format(
                            exit_code, disp, now
                        )
                    )
                    sys.exit(exit_code)

            time.sleep(2)
    except KeyboardInterrupt:
        sys.exit(0)


PROJECTION = ["ClusterId", "Owner", "UserLog", "JobBatchName", "Iwd"]


def find_job_event_logs(users=None, cluster_ids=None, files=None, batches=None):
    if users is None:
        users = []
    if cluster_ids is None:
        cluster_ids = []
    if files is None:
        files = []
    if batches is None:
        batches = []

    constraint = " || ".join(
        itertools.chain(
            ("Owner == {}".format(classad.quote(u)) for u in users),
            ("ClusterId == {}".format(cid) for cid in cluster_ids),
            ("JobBatchName == {}".format(b) for b in batches),
        )
    )
    if constraint != "":
        ads = query(constraint=constraint, projection=PROJECTION)
    else:
        ads = []

    cluster_ids = set()
    event_logs = set()
    batch_names = {}
    already_warned_missing_log = set()
    for ad in ads:
        cluster_id = ad["ClusterId"]
        cluster_ids.add(cluster_id)
        batch_names[cluster_id] = ad.get("JobBatchName")

        try:
            log_path = ad["UserLog"]
        except KeyError:
            if cluster_id not in already_warned_missing_log:
                print(
                    "WARNING: cluster {} does not have a job event log file (set log=<path> in the submit description)".format(
                        cluster_id
                    ),
                    file=sys.stderr,
                )
                already_warned_missing_log.add(cluster_id)
            continue

        # if the path is not absolute, try to make it absolute using the
        # job's initial working directory
        if not os.path.isabs(log_path):
            log_path = os.path.abspath(os.path.join(ad["Iwd"], log_path))

        event_logs.add(log_path)

    for file in files:
        event_logs.add(os.path.abspath(file))

    return cluster_ids, event_logs, batch_names


def query(constraint, projection=None):
    schedd = htcondor.Schedd()
    ads = schedd.query(constraint, projection)
    return ads


TOTAL = "TOTAL"
ACTIVE_JOBS = "JOB_IDS"
EVENT_LOG = "LOG"
CLUSTER_ID = "CLUSTER"
BATCH_NAME = "BATCH"


class Cluster:
    def __init__(self, cluster_id, event_log_path, batch_name):
        self.cluster_id = cluster_id
        self.event_log_path = event_log_path
        self._batch_name = batch_name

        self.job_to_state = {}

    @property
    def batch_name(self):
        return self._batch_name or "ID: {}".format(self.cluster_id)

    def __setitem__(self, key, value):
        self.job_to_state[key] = value

    def __getitem__(self, item):
        return self.job_to_state[item]

    def items(self):
        return self.job_to_state.items()

    def __iter__(self):
        return iter(self.items())


class JobStateTracker:
    def __init__(self, event_log_paths, batch_names):
        event_readers = {}
        for event_log_path in event_log_paths:
            try:
                reader = htcondor.JobEventLog(event_log_path).events(0)
                event_readers[event_log_path] = reader
            except (OSError, IOError) as e:
                print(
                    "WARNING: Could not open event log at {} for reading, so it will be ignored. Reason: {}".format(
                        event_log_path, e
                    ),
                    file=sys.stderr,
                )

        self.event_readers = event_readers
        self.state = collections.defaultdict(lambda: collections.defaultdict(dict))

        self.batch_names = batch_names

        self.cluster_id_to_cluster = {}

    def process_events(self):
        messages = []

        for event_log_path, events in self.event_readers.items():
            while True:
                try:
                    event = next(events)
                except StopIteration:
                    break
                except Exception as e:
                    messages.append(
                        "ERROR: failed to parse event from {}. Reason: {}".format(
                            event_log_path, e
                        )
                    )
                    continue

                new_status = JOB_EVENT_STATUS_TRANSITIONS.get(event.type, None)
                if new_status is None:
                    continue

                cluster = self.cluster_id_to_cluster.setdefault(
                    event.cluster,
                    Cluster(
                        cluster_id=event.cluster,
                        event_log_path=event_log_path,
                        batch_name=self.batch_names.get(event.cluster),
                    ),
                )

                cluster[event.proc] = new_status

        return messages

    @property
    def clusters(self):
        return self.cluster_id_to_cluster.values()

    @property
    def job_states(self):
        for cluster in self.clusters:
            for job, state in cluster:
                yield state


def table_by(clusters, attribute, abbreviate_path_components):
    key = {
        'event_log_path': EVENT_LOG,
        'cluster_id': CLUSTER_ID,
        'batch_name': BATCH_NAME,
    }[attribute]

    summary = collections.defaultdict(int)
    rows = []
    for attribute_value, clusters in group_clusters_by(clusters, attribute).items():
        row_data = row_data_from_job_state(clusters)
        
        summary[TOTAL] += row_data[TOTAL]
            
        for status in JobStatus:
                   if status != JobStatus.TRANSFERRING_OUTPUT:
                      summary[status] += row_data[status]

        row_data[key] = attribute_value

        rows.append(row_data)

    if key == EVENT_LOG:
        for row in rows:
            row[key] = normalize_path(
                row[key],
                abbreviate_path_components=abbreviate_path_components,
            )

    rows.sort(key=lambda r: r[key])

    headers, rows = strip_empty_columns(rows)
    
    for r in rows:
        for x in r:
            if r.get(x) == 0:
                r[x] = "-"

    return summary, table(headers=[key] + headers, rows=rows, alignment=TABLE_ALIGNMENT)


def group_clusters_by(clusters, attribute):
    getter = operator.attrgetter(attribute)

    groups = collections.defaultdict(list)
    for cluster in clusters:
        groups[getter(cluster)].append(cluster)

    return groups


def strip_empty_columns(rows):
    dont_include = set()
    for h in HEADERS:
        if all((row[h] == 0 for row in rows)):
            dont_include.add(h)
    dont_include -= ALWAYS_INCLUDE
    headers = [h for h in HEADERS if h not in dont_include]
    for row_data in dont_include:
        for row in rows:
            row.pop(row_data)

    return headers, rows


def row_data_from_job_state(clusters):
    row_data = {js: 0 for js in JobStatus}
    active_job_ids = []

    for cluster in clusters:
        for proc_id, job_state in cluster:
            row_data[job_state] += 1

            if job_state in ACTIVE_STATES:
                active_job_ids.append("{}.{}".format(cluster.cluster_id, proc_id))

    row_data[TOTAL] = sum(row_data.values())

    if len(active_job_ids) > 3:
        active_job_ids = [active_job_ids[0], active_job_ids[-1]]
        row_data[ACTIVE_JOBS] = " ... ".join(active_job_ids)
    else:
        row_data[ACTIVE_JOBS] = ", ".join(active_job_ids)

    return row_data
    

def normalize_path(path, abbreviate_path_components=False):
    possibilities = []

    abs = os.path.abspath(path)
    home_dir = os.path.expanduser("~")
    cwd = os.getcwd()

    possibilities.append(abs)

    relative_to_user_home = os.path.relpath(path, home_dir)
    possibilities.append(os.path.join("~", relative_to_user_home))

    relative_to_cwd = os.path.relpath(path, cwd)
    if not relative_to_cwd.startswith("../"):  # i.e, it is not *above* the cwd
        possibilities.append(os.path.join(".", relative_to_cwd))

    if abbreviate_path_components:
        possibilities = map(abbreviate_path, possibilities)

    return min(possibilities, key=len)


def abbreviate_path(path):
    abbreviated_components = []
    path_to_here = ""
    components = split_all(path)
    for component in components[:-1]:
        path_to_here = os.path.expanduser(os.path.join(path_to_here, component))

        if component in ("~", "."):
            abbreviated_components.append(component)
            continue

        contents = os.listdir(os.path.dirname(path_to_here))
        if len(contents) == 1:
            longest_common = ""
        else:
            longest_common = os.path.commonprefix(contents)

        abbreviated_components.append(component[: len(longest_common) + 1])

    abbreviated_components.append(components[-1])

    return os.path.join(*abbreviated_components)


def split_all(path):
    allparts = []
    while 1:
        parts = os.path.split(path)
        if parts[0] == path:  # sentinel for absolute paths
            allparts.insert(0, parts[0])
            break
        elif parts[1] == path:  # sentinel for relative paths
            allparts.insert(0, parts[1])
            break
        else:
            path = parts[0]
            allparts.insert(0, parts[1])
    return allparts


class JobStatus(enum.Enum):
    REMOVED = "REMOVED"
    HELD = "HELD"
    IDLE = "IDLE"
    RUNNING = "RUN"
    COMPLETED = "DONE"
    TRANSFERRING_OUTPUT = "TRANSFERRING_OUTPUT"
    SUSPENDED = "SUSPENDED"

    def __str__(self):
        return self.value

    @classmethod
    def ordered(cls):
        return (
            cls.REMOVED,
            cls.HELD,
            cls.SUSPENDED,
            cls.IDLE,
            cls.RUNNING,
            cls.TRANSFERRING_OUTPUT,
            cls.COMPLETED,
        )


ACTIVE_STATES = {
    JobStatus.IDLE,
    JobStatus.RUNNING,
    JobStatus.TRANSFERRING_OUTPUT,
    JobStatus.HELD,
    JobStatus.SUSPENDED,
}

ALWAYS_INCLUDE = {
    JobStatus.IDLE,
    JobStatus.RUNNING,
    JobStatus.COMPLETED,
    EVENT_LOG,
    TOTAL,
}

TABLE_ALIGNMENT = {
    EVENT_LOG: "ljust",
    CLUSTER_ID: "ljust",
    TOTAL: "rjust",
    ACTIVE_JOBS: "ljust",
    BATCH_NAME: "ljust",
}
for k in JobStatus:
    TABLE_ALIGNMENT[k] = "rjust"

HEADERS = list(JobStatus.ordered()) + [TOTAL, ACTIVE_JOBS]

JOB_EVENT_STATUS_TRANSITIONS = {
    htcondor.JobEventType.SUBMIT: JobStatus.IDLE,
    htcondor.JobEventType.JOB_EVICTED: JobStatus.IDLE,
    htcondor.JobEventType.JOB_UNSUSPENDED: JobStatus.IDLE,
    htcondor.JobEventType.JOB_RELEASED: JobStatus.IDLE,
    htcondor.JobEventType.SHADOW_EXCEPTION: JobStatus.IDLE,
    htcondor.JobEventType.JOB_RECONNECT_FAILED: JobStatus.IDLE,
    htcondor.JobEventType.JOB_TERMINATED: JobStatus.COMPLETED,
    htcondor.JobEventType.EXECUTE: JobStatus.RUNNING,
    htcondor.JobEventType.JOB_HELD: JobStatus.HELD,
    htcondor.JobEventType.JOB_SUSPENDED: JobStatus.SUSPENDED,
    htcondor.JobEventType.JOB_ABORTED: JobStatus.REMOVED,
}


def table(headers, rows, fill="", header_fmt=None, row_fmt=None, alignment=None):
    if header_fmt is None:
        header_fmt = lambda _: _
    if row_fmt is None:
        row_fmt = lambda _: _
    if alignment is None:
        alignment = {}

    headers = tuple(headers)
    lengths = [len(str(h)) for h in headers]

    align_methods = [alignment.get(h, "center") for h in headers]

    processed_rows = []
    for row in rows:
        processed_rows.append([str(row.get(key, fill)) for key in headers])

    for row in processed_rows:
        lengths = [max(curr, len(entry)) for curr, entry in zip(lengths, row)]

    header = header_fmt(
        "  ".join(
            getattr(str(h), a)(l) for h, l, a in zip(headers, lengths, align_methods)
        ).rstrip()
    )

    lines = [
        row_fmt(
            "  ".join(getattr(f, a)(l) for f, l, a in zip(row, lengths, align_methods))
        )
        for row in processed_rows
    ]

    output = "\n".join([header] + lines)

    return output


if __name__ == "__main__":
    cli()
