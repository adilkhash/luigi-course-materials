import luigi


class mysection(luigi.Config):
    host = luigi.Parameter()
    port = luigi.IntParameter()


class ShowMySection(luigi.Task):
    def run(self):
        section = mysection()
        print('Host is: {}\nPort is: {}'.format(section.host, section.port))


class CustomParamTask(luigi.Task):
    message = luigi.Parameter()

    def run(self):
        print('The message is {}'.format(self.message))
