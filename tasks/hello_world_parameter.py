import luigi


class Word2FileTask(luigi.Task):

    word = luigi.Parameter()
    filename = luigi.Parameter()

    def run(self):
        with self.output().open('w') as f:
            f.write(self.word)

    def output(self):
        return luigi.LocalTarget(self.filename)


class HelloWorldTask(luigi.Task):
    """Combines tasks and write the result to hello_world.txt"""
    def requires(self):
        return [
            Word2FileTask(word='hello', filename='hello.txt'),
            Word2FileTask(word='world', filename='world.txt'),
        ]

    def run(self):
        hello, world = self.input()

        with self.output().open('w') as output:
            with hello.open() as fh, world.open() as fw:
                output.write('{} {}\n'.format(fh.read(), fw.read()))

    def output(self):
        return luigi.LocalTarget('hello_world.txt')
