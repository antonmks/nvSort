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