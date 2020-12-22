# luigid --port 80
# https://bionics.it/posts/luigi-tutorial
# https://vsupalov.com/new-to-luigi-good-to-know/
# https://medium.com/@prasanth_lade/luigi-all-you-need-to-know-f1bc157b20ed (MUST READ)
# https://towardsdatascience.com/data-pipelines-luigi-airflow-everything-you-need-to-know-18dc741449b7 (To Run about Task and Targets)
# https://luigi.readthedocs.io/en/stable/tasks.html For Diagram

# https://luigi.readthedocs.io/en/stable/central_scheduler.html
# start server: luigid

# luigi --port 8082
# python ./LuigiExample.py --scheduler-host localhost NameSubstituter --name Adnan
# python ./LuigiExample.py --scheduler-host localhost NameSubstituter --name Alan


import time

import luigi
#luigid --port 8082

# Task A
class HelloWorld(luigi.Task):
    def requires(self):
        return None

    def output(self):
        return luigi.LocalTarget('helloworld.txt')

    def run(self):
        time.sleep(15)
        with self.output().open('w') as outfile:
            outfile.write('Hello World!\n')
        time.sleep(15)


# Task B
class NameSubstituter(luigi.Task):
    name = luigi.Parameter()

    def requires(self):
        return HelloWorld()

    def output(self):
        return luigi.LocalTarget(self.input().path + '.name_' + self.name)

    def run(self):
        time.sleep(15)
        with self.input().open() as infile, self.output().open('w') as outfile:
            text = infile.read()
            text = text.replace('World', self.name)
            outfile.write(text)
        time.sleep(15)


if __name__ == '__main__':
    luigi.run()


"""
class GenerateWords(luigi.Task):

    def output(self):
        return luigi.LocalTarget('words.txt')

    def run(self):
        # write a dummy list of words to output file﻿
        words = ['apple', 'banana','grapefruit']
        with self.output().open('w') as f:
            for word in words:
                f.write('{word}\n'.format(word=word))

class CountLetters(luigi.Task):

    def requires(self):
        return GenerateWords()

    def output(self):
        return luigi.LocalTarget('letter_counts.txt')

    def run(self):
        # read in file as list﻿
        with self.input().open('r') as infile:
            words = infile.read().splitlines()
        # write each word to output file with its corresponding letter count﻿
        with self.output().open('w') as outfile:
            for word in words:
                outfile.write('{word} | {letter_count}\n'.format(word=word,letter_count=len(word)))

"""