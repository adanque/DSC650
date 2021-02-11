"""
Author:     Alan Danque
Date:       20210101
Class:      DSC 650
Exercise:   7.1d Python Function to pass an input list of keys in which returns a sorted and partitioned list of keys
            based on how many partitions needed
"""

def balance_partitions (keys, num_partitions):
    uniquevals = sorted(set(keys))
    num_uniq_vals = len(uniquevals)
    partition_counts = (num_uniq_vals / num_partitions)+1
    partitions  = []
    currow = 1
    partnum = 1
    for i in range(num_uniq_vals):
        curkeyval ={}
        if currow <= partition_counts:
            curkeyval[uniquevals[i]] = partnum
            currow = currow + 1
        else:
            currow = 1
            partnum = partnum + 1
            curkeyval[uniquevals[i]] = partnum
            currow = currow + 1
        partitions .append(curkeyval)
    return partitions

# Testing Review
keys = ['test1', 'test2', 'test3', 'test4', 'test5', 'test6', 'test7', 'test8', 'test9', 'test10', 'test11']
num_partitions = 3
partitions  = balance_partitions(keys, num_partitions)
print(type(partitions ))
print(partitions )

keys = ['test1', 'test2', 'test3', 'test4', 'test5', 'test6', 'test7', 'test8', 'test9', 'test10', 'test11', 'test12', 'test13', 'test14', 'test15', 'test16']
num_partitions = 3
partitions  = balance_partitions(keys, num_partitions)
print(type(partitions ))
print(partitions )

