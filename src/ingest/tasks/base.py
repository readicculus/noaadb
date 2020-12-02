import luigi


class BaseTask(luigi.Task):

    force = luigi.BoolParameter()

    def complete(self):
        outputs = luigi.task.flatten(self.output())
        for output in outputs:
            if self.force and output.exists():
                self.cleanup(output)
                output.remove()
        return all(map(lambda output: output.exists(), outputs))

    def cleanup(self):
        raise NotImplementedError('Must define cleanup')