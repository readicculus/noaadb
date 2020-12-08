import logging

import luigi


from luigi.task import flatten




def toggle_force_to_false(func):
    """Toggle self.force permanently to be False. This is required towards
    the end of the task's lifecycle, when we need to know the true value
    of Target.exists()"""
    def wrapper(self, *args, **kwargs):
        logger = logging.getLogger('core_debug')
        logger.debug('CLEANUP: ' + self.task_id)
        self.cleanup()
        self.force = False
        logger.debug('RUN: ' + self.task_id)
        return func(self, *args, **kwargs)
    return wrapper

#
# def toggle_exists(output_func):
#     """Patch Target.exists() if self.force is True"""
#     def wrapper(self):
#         outputs = output_func(self)
#         for out in luigi.task.flatten(outputs):
#             # Patch Target.exists() to return False
#             if self.force:
#                 out.exists = lambda *args, **kwargs: False
#             # "Unpatch" Target.exists() to it's original form
#             else:
#                 out.exists = lambda *args, **kwargs: out.__class__.exists(out, *args, **kwargs)
#         return outputs
#     return wrapper




class ForcibleTask(luigi.Task):
    """A luigi task which can be forceably rerun"""
    force = luigi.BoolParameter(significant=False, default=False)
    force_upstream = luigi.BoolParameter(significant=False, default=False)
    lock = luigi.BoolParameter(significant=False, default=False)

    def cleanup(self):
        raise NotImplementedError('Must define cleanup %s' % self.task_id)



    def complete(self):
        logger = logging.getLogger('core_debug')
        if self.force and not self.lock:
            logger.debug('COMPLETE_ret_False: ' + self.task_id)
            return False

        outputs = flatten(self.output())
        if len(outputs) == 0:
            return False

        logger.debug('COMPLETE_ret_normal: ' + self.task_id)
        return all(map(lambda output: output.exists(), outputs))


    def get_tree(self):
        done = False
        tasks = [self]
        checked = []
        while not done:
            tasks += luigi.task.flatten(tasks[0].requires())
            checked.append(tasks.pop(0))
            if len(tasks) == 0:
                done = True
        return checked

    def __init__(self,  *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Force children to be rerun
        if self.force_upstream:
            self.force = True
            children = list(reversed(self.get_tree()))
            for child in children:
                child.force = True

    def __init_subclass__(cls):
        super().__init_subclass__()
        # cls.output = toggle_exists(cls.output)
        # Later on in the task's lifecycle, 'run' and 'trigger_event' are called so we can use
        # these as an opportunity to toggle "force = False" to allow the Target.exists()
        # to return it's true value at the end of the Task
        cls.run = toggle_force_to_false(cls.run)
        # cls.trigger_event = toggle_force_to_false(cls.trigger_event)



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