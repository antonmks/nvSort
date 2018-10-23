## Welcome to nvsort - file sort on gpu

### How to build?

Run make

### How to use it ?

Examples :

./nvsort myfile.txt   <!--- // sort file myfile.txt -->

./nvsort -t "|" -k 16 myfile.txt // sort file myfile.txt alphabetically on field 16 using | as separator

./nvsort -t "|" -k 1n myfile.txt // sort file myfile.txt numerically on field 1 using | as separator

./nvsort -t "|" -k 1 -k 16 myfile.txt // sort file myfile.txt numerically on fields 1 and 16 using | as separator

./nvsort -t "|" -r -k 1 -k 16 myfile.txt // reverse sort file myfile.txt numerically on field 1 using | as separator


### I tested it on Linux Ubuntu 16.04 and Windows 10 with the latest VisualStudio

### Some benchmarks :
Hardware : Intel i3-4130, 16GB of RAM, GTX 1080, Ubuntu 16.04, 120GB SSD as a storage

Sorting an 800MB file on one of the string fields 
on cpu using --parallel=2 total time = 20 seconds

on gpu total time = 4 seconds

The actual aorting time is about 16 seconds on a cpu and about 0.8 seconds on a gpu(including copying data to and from a gpu).

