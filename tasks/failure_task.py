from luigi import Task


class FailureTask(Task):
    def run(self):
        1/0
