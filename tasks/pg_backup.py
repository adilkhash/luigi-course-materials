import os
import datetime as dt

import luigi
from luigi.contrib.external_program import ExternalProgramTask
from luigi.contrib.s3 import S3Client


class PgDumpTask(ExternalProgramTask):

    host = luigi.Parameter(default='localhost')
    port = luigi.Parameter(default=5432)
    user = luigi.Parameter(default='notion')
    database = luigi.Parameter(default='notion')

    def program_environment(self):
        env = super().program_environment()
        env['PGPASSWORD'] = 'password'
        return env

    def program_args(self):
        args = ['pg_dump']

        common_args = [
            '-a',
            '-Fc',
            '-Z9',
            ('-U', self.user),
            ('-h', self.host),
            ('-d', self.database),
            ('-p', self.port),
            ('-f', self.output().path)
        ]

        for arg in common_args:
            if isinstance(arg, (tuple, list)):
                args += [str(s) for s in arg]
            else:
                args.append(str(arg))
        return args

    def output(self):
        return luigi.LocalTarget(
            os.path.join(os.path.expanduser('~'), f'dump-{dt.date.today()}.dump')
        )


class UploadToS3(luigi.Task):
    s3_path = luigi.Parameter()

    def requires(self):
        return PgDumpTask()

    def run(self):
        s3 = S3Client()
        file_path = self.input().path
        filename = file_path.split('/')[-1]
        s3.put(file_path, f's3://{self.s3_path}/{filename}')

        os.unlink(file_path)
