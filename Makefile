# Makefile
#
CFLAGS=--machine 64 -O3 -std=c++11 --expt-extended-lambda -I moderngpu/src/moderngpu/

GENCODE_SM61	:= -gencode arch=compute_61,code=sm_61
GENCODE_FLAGS	:= $(GENCODE_SM61) 

nvsort : main.o str_sort.o \

	nvcc $(CFLAGS) -L . -o nvsort main.o str_sort.o \

nvcc = nvcc $(CFLAGS) $(GENCODE_FLAGS) -c

main.o : main.cu
	$(nvcc) main.cu
str_sort.o : str_sort.cu str_sort.h
	$(nvcc) str_sort.cu
	
clean : 
	$(RM) nvsort *.o
