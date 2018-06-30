## Welcome to nvsort - file sort on gpu

### How to build?

Run make

### How to use it ?

Examples :

```console
./nvsort myfile.txt    # sort file myfile.txt

./nvsort -t "|" -f 16 myfile.txt  # sort a file alphabetically on field 16 using "|" as separator

./nvsort -t "|" -f 1n myfile.txt  # sort a file numerically on field 1 using "|" as separator

./nvsort -t "|" -f 1 -f 16 myfile.txt  # sort a file numerically on fields 1 and 16 using "|" as separator

./nvsort -t "|" -r -f 1 -f 16 myfile.txt  #reverse sort a file numerically on field 1 using "|" as separator
```

### Tests
I tested it on Linux Ubuntu 16.04 with Nvidia Gtx 1080
