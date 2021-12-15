import logging

import luigi

from luigi.task import flatten

from core.target import NOAADBTableTarget
from noaadb import DATABASE_URI
from noaadb.schema.models import *

def toggle_force_to_false(func):
    # Wrap the run task so that when run is normally called
    # we call this task instead which
    # first calls cleanup() before calling run()
    def wrapper(self, *args, **kwargs):
        if self.lock and self.complete():
            raise Exception("Hit run() on a locked task.?!")
        logger = logging.getLogger('core_debug')
        logger.debug('CLEANUP: ' + self.task_id)
        if not self.lock:
            self.cleanup()
        self.force = False
        logger.debug('RUN: ' + self.task_id)
        return func(self, *args, **kwargs)

    return wrapper


class ForcibleTask(luigi.Task):
    """A luigi task which can be forceably rerun"""
    force = luigi.BoolParameter(significant=False, default=False)
    force_upstream = luigi.BoolParameter(significant=False, default=False)
    lock = luigi.BoolParameter(significant=False, default=False)

    def cleanup(self):
        # ForcibleTask's must define a cleanup function
        raise NotImplementedError('Must define cleanup %s' % self.task_id)

    # override the complete function to return False when we have forced the task to run
    # if we have not forced the task to run return as normal
    def complete(self):
        logger = logging.getLogger('core_debug')
        if self.force and not self.lock:
            logger.debug('COMPLETE_ret_False: ' + self.task_id)
            return False

        outputs = flatten(self.output())
        if len(outputs) == 0:
            return False

        # return as normal
        logger.debug('COMPLETE_ret_normal: ' + self.task_id)
        return all(map(lambda output: output.exists(), outputs))

    # walk through tree of upstream dependencies and return
    # this is called if a task has force_upstream to true
    def get_upstream_tasks(self):
        done = False
        tasks = [self]
        checked = []
        while not done:
            tasks += luigi.task.flatten(tasks[0].requires())
            checked.append(tasks.pop(0))
            if len(tasks) == 0:
                done = True
        return checked

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.force_upstream:
            # Force upstream children to be rerun
            self.force = True
            children = list(reversed(self.get_upstream_tasks()))
            for child in children:
                child.force = True

    def __init_subclass__(cls):
        super().__init_subclass__()
        # override task run with our wrapper run function that also cleans up
        cls.run = toggle_force_to_false(cls.run)


def toggle_complete_to_true(func):
    def wrapper(self, *args, **kwargs):
        self.is_complete = True
        return func(self, *args, **kwargs)

    return wrapper


class AlwaysRunTask(luigi.Task):
    is_complete = False

    def complete(self):
        return self.is_complete

    def __init_subclass__(cls):
        super().__init_subclass__()
        cls.run = toggle_complete_to_true(cls.run)

class CreateTableTask(luigi.Task):
    children = luigi.ListParameter()
    lock = luigi.BoolParameter(default=True)
    force = luigi.BoolParameter(default=False)

    def requires(self):
        if len(list(self.children)) > 1:
            tables = list(reversed(self.children))
            return CreateTableTask(children=list(reversed(tables[1:])), lock=self.lock, force=self.force)
        return []

    def output(self):
        tables = list(reversed(self.children))
        self.table_name = tables.pop(0)
        t = globals()[self.table_name]
        return NOAADBTableTarget(DATABASE_URI, t)
    #
    # def cleanup(self):
    #     self.output().remove()

    def run(self):
        self.output().touch()