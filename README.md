Big files sorter
================

Application which sorts very large text files line by line

Install
-------

tar zxvf dpinol\_sorter.tgz

mvn compile

Run
---

The script runs a java application which sorts the inputFile into the outputFile
using an optionally specified temporary folder. By default it used 4G of memory.

./run.sh inputFile outputFile [optional temporary folder]

Algorithm
---------

I’ve experimented with several algorithms and techniques. I selected the own
which performed a better performance for 50.000.000 lines (not 5.000.000 as in
the instructions). The tuning will depend on the machine: how many processors,
how much RAM, speed of memory, speed of disk, ...

-   BigFileSorter is the main class which performs a kind of map and reduce on
    the input list of lines.

-   The map phase cuts the input file into buckets of consecutive lines, which
    are  distributed to several instances of ChunkSorter. They sort their lines
    in parallel and write the partial lists in file to avoid running out of
    disk.

-   The reduce phase is performed by the ParallelFilesMerger, which

    -   Splits the list of files in segments of files. Each segment of files is
        in parallel merged by an Input Merger, which will push the sorted lines
        sorted into their own queue.

    -   In parallel, the Output Merger merges all the lines read from all queues
        into the output file.

 
