
"""
Author:     Alan Danque
Date:       20201216
Class:      DSC 650
Exercise:   4.2
"""
import luigi
import time
import os
import json
from pathlib import Path
import zipfile
import email
from email.policy import default
from email.parser import Parser
from datetime import timezone
from collections import namedtuple
import gzip
from collections import OrderedDict
import pandas as pd
import s3fs
from bs4 import BeautifulSoup
from dateutil.parser import parse
from chardet.universaldetector import UniversalDetector

from pyspark.ml import Pipeline
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.feature import HashingTF, Tokenizer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.pipeline import Transformer
from pyspark.sql.functions import udf
#from pyspark.sql.types import StructType, StringType
from pyspark.sql.types import *

import pandas as pd

# luigi --port 8082
# python ./run_assignment_4.2.py --scheduler-host localhost ProcessEnronEmails


#current_dir = Path(os.getcwd()).absolute()

current_dir = Path('C:/Users/aland/class/DSC650/dsc650/')
results_dir = current_dir.joinpath('results')
results_dir.mkdir(parents=True, exist_ok=True)
data_dir = current_dir.joinpath('data')
data_dir.mkdir(parents=True, exist_ok=True)
processed_directory = data_dir.joinpath('processed/enron')
processed_directory.mkdir(parents=True, exist_ok=True)
processed_directory = str(processed_directory)
enron_data_dir = str(data_dir.joinpath('external/enron'))

#print(data_dir)
#print(enron_data_dir)

emails_directory = enron_data_dir


class ProcessMailbox(luigi.Task):
    mailbox_directory = luigi.Parameter()
    processed_directory = luigi.Parameter()
    #processed_directory.mkdir(parents=True, exist_ok=True)

    def output(self):
        pass

    def run(self):
        #pass
        print('in class ProcessMailbox')


class ProcessEnronEmails(luigi.WrapperTask):
    emails_directory = luigi.Parameter()
    processed_directory = luigi.Parameter()
    #print("emails_directory")
    #print(emails_directory)

    def requires(self):
        directories = []
        #print("emails_directory")
        #print(emails_directory)
        ignored = {"enron"}
        for file in os.listdir(emails_directory):
            d = os.path.join(emails_directory, file)
            if os.path.isdir(d):
                #print(d)
                if d not in ignored:
                    directories.append(d)

        #for root, dirs, files in os.walk(emails_directory):

        for directory in directories:
            #print("directory")
            print(directory)
            yield ProcessMailbox(
                mailbox_directory=str(directory),
                processed_directory=str(self.processed_directory)
            )

    def output(self):
        pass

    def run(self):
        print('in class ProcessEnronEmails')
        #pass

def main():
    tasks = [
        ProcessEnronEmails(emails_directory=emails_directory, processed_directory=processed_directory)
    ]
    luigi.build(tasks, workers=8, local_scheduler=True)


if __name__ == '__main__':
    main()


"""    
Create and run
this
workflow.The
following is the
example
of
a
workflow
that
completed
with one failed task.Failures could be caused by problems with the workflow or problems with the data.The advantage of using a tool like Luigi is that you don’t need to re-run all the tasks, only the ones that failed.

== == = Luigi
Execution
Summary == == =

Scheduled
151
tasks
of
which:
*23
complete
ones
were
encountered:
- 23
ProcessMailbox(mailbox_directory=dsc650 / data / external / enron / dean - c,
               processed_directory=dsc650 / data / processed / enron)...
*126
ran
successfully:
- 126
ProcessMailbox(mailbox_directory=dsc650 / data / external / enron / allen - p,
               processed_directory=dsc650 / data / processed / enron)...
*1
failed:
- 1
ProcessMailbox(mailbox_directory=dsc650 / data / external / enron / stokley - c,
               processed_directory=dsc650 / data / processed / enron)
*1
were
left
pending, among
these:
*1
had
failed
dependencies:
- 1
ProcessEnronEmails(emails_directory=dsc650 / data / external / enron,
                   processed_directory=dsc650 / data / processed / enron)

This
progress
looks: (because there were failed tasks

== == = Luigi Execution Summary == == =
This is an example of a workflow that ran without errors.

INFO:
== == = Luigi Execution Summary == == =

Scheduled 12 tasks of which:
* 12 ran successfully:
- 1 ProcessEnronEmails(emails_directory=dsc650 / data / external / enron, processed_directory=dsc650 / data / processed / enron)
       - 11
ProcessMailbox(mailbox_directory=dsc650 / data / external / enron / davis - d,
               processed_directory=dsc650 / data / processed / enron)...

    This
progress
looks:) because
there
were
no
failed
tasks or missing
dependencies

== == = Luigi
Execution
Summary == == =
Additionally, Luigi
has
a
web - based
central
scheduler
that
you
use
to
view and manage
the
progress
of
your
workflows.

"""