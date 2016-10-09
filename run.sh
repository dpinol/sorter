#!/bin/bash
date
time java -Xmx4000M -cp target/classes/ org.dpinol.BigFileSorter $1 ~/appDev/shibs/output.txt
date
