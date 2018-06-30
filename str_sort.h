/*This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/    

#include <fstream>
#include <iomanip>
#include <map>
#include <iostream>
#include <ctime>
#include <time.h>
#include <stdint.h>
#include <thrust/device_vector.h>
#include <thrust/gather.h>
#include <thrust/scatter.h>
#include <thrust/sequence.h>
#include <thrust/system/cuda/experimental/pinned_allocator.h>
#include <thrust/iterator/constant_iterator.h>
#include <thrust/iterator/counting_iterator.h>
#include <thrust/iterator/discard_iterator.h>
#include <thrust/count.h>
#include <kernel_intervalmove.hxx>
#include <kernel_mergesort.hxx>
#include <kernel_merge.hxx>


struct gpu_atof
{
    const char *source;
    double *dest;
	const unsigned int* key_num;
	const char *delimiter;
	const int *pos;		

    gpu_atof(const char *_source, double *_dest, const unsigned int* _key_num, const char* _delimiter, const int* _pos):
        source(_source), dest(_dest), key_num(_key_num), delimiter(_delimiter), pos(_pos) {}
    template <typename IndexType>
    __host__ __device__
    void operator()(const IndexType & i) {
        const char *p;
        int frac;
        double sign, value, scale;
		unsigned int j= 0, cnt = 0;		
		
        while(cnt < key_num[0]-1)	{
			if(source[pos[i]+j] == delimiter[0])
				cnt++;	
			j++;
		};					
		
        p = &source[pos[i]+j];

        while (*p == ' ') {
            p += 1;
        }

        sign = 1.0;
        if (*p == '-') {
            sign = -1.0;
            p += 1;
        } else
            if (*p == '+') {
                p += 1;
            }

        for (value = 0.0; *p >= '0' && *p <= '9'; p += 1) {
            value = value * 10.0 + (*p - '0');
        }

        if (*p == '.') {
            double pow10 = 10.0;
            p += 1;
            while (*p >= '0' && *p <= '9') {
                value += (*p - '0') / pow10;
                pow10 *= 10.0;
                p += 1;
            }
        }

        frac = 0;
        scale = 1.0;
        dest[i] = sign * (frac ? (value / scale) : (value * scale));
    }
};


struct gpu_find_pos
{
    const char *source;
    char **dest;
	const unsigned int* key_num;
	const char *delimiter;
	const int *pos;		

    gpu_find_pos(const char *_source, char **_dest, const unsigned int* _key_num, const char* _delimiter, const int* _pos):
        source(_source), dest(_dest), key_num(_key_num), delimiter(_delimiter), pos(_pos) {}
    template <typename IndexType>
    __host__ __device__
    void operator()(const IndexType & i) {
		unsigned int j= 0, cnt = 0;		
		
        while(cnt < key_num[0]-1)	{
			if(source[pos[i]+j] == delimiter[0])
				cnt++;	
			j++;
		};					
		dest[i] = (char*)&source[pos[i]+j];		
    }
};

struct compare_fields
{
    char delimiter;
    unsigned int* fields;
    unsigned int* fields_numeric;
    size_t field_count;

    compare_fields(char _delimiter, unsigned int* _fields, unsigned int* _fields_numeric, size_t _field_count) {
        delimiter = _delimiter;
        fields = _fields;
        fields_numeric = _fields_numeric;
        field_count = _field_count;
    }
    __host__ __device__
    bool operator()(const char* t1, const char* t2)
    {
        for(unsigned int i = 0; i < field_count; i++)
        {    
            unsigned int j= 0, cnt = 0, z = 0;                     
            while(cnt < fields[i]-1)   {
                if(t1[j] == delimiter)
                    cnt++;  
                j++;
            };                  
            cnt = 0;                     
            while(cnt < fields[i]-1)   {
                if(t2[z] == delimiter)
                    cnt++;  
                z++;
            };                  

            if(fields_numeric[i] == 0)
            {    
                const unsigned char *s1 = (const unsigned char*)&(t1[j]);
                const unsigned char *s2 = (const unsigned char*)&(t2[z]);
                unsigned char c1, c2;

                do
                {
                    c1 = (unsigned char) *s1++;
                    c2 = (unsigned char) *s2++;
                    if (c1 == delimiter) {
                        if(c1 != c2)
                            return 1;                    
                        else break;
                    }
                    if(c2 == delimiter)
                        return 0;
                }
                while (c1 == c2);
                if(c1 != c2)
                    return c1 < c2;        

            }
            else
            {
                double r1, r2; 
                const char *p;
                double sign, value, scale;
                int frac;
                for(unsigned int zz = 0; zz < 2; zz++) {       
                    if(zz == 0)        
                        p = (char*)&(t1[j]);
                    else
                        p = (char*)&(t2[z]);

                    while (*p == ' ') {
                        p += 1;
                    }

                    sign = 1.0;
                    if (*p == '-') {
                        sign = -1.0;
                        p += 1;
                    } else
                        if (*p == '+') {
                            p += 1;
                        }

                    for (value = 0.0; *p >= '0' && *p <= '9'; p += 1) {
                        value = value * 10.0 + (*p - '0');
                    }

                    if (*p == '.') {
                        double pow10 = 10.0;
                        p += 1;
                        while (*p >= '0' && *p <= '9') {
                            value += (*p - '0') / pow10;
                            pow10 *= 10.0;
                            p += 1;
                        }
                    }

                    frac = 0;
                    scale = 1.0;
                    if(zz == 0)        
                        r1 = sign * (frac ? (value / scale) : (value * scale));
                    else 
                        r2 = sign * (frac ? (value / scale) : (value * scale));

                };    

                if(r1 != r2)
                    return r1 < r2;
            }  
        };  
        return 0;  
    }   
};

struct compare_fields_desc
{
    char delimiter;
    unsigned int* fields;
    unsigned int* fields_numeric;
    size_t field_count;

    compare_fields_desc(char _delimiter, unsigned int* _fields, unsigned int* _fields_numeric, size_t _field_count) {
        delimiter = _delimiter;
        fields = _fields;
        fields_numeric = _fields_numeric;
        field_count = _field_count;
    }
    __host__ __device__
    bool operator()(const char* t1, const char* t2)
    {
        for(unsigned int i = 0; i < field_count; i++)
        {    
            unsigned int j= 0, cnt = 0, z = 0;                     
            while(cnt < fields[i]-1)   {
                if(t1[j] == delimiter)
                    cnt++;  
                j++;
            };                  
            cnt = 0;                     
            while(cnt < fields[i]-1)   {
                if(t2[z] == delimiter)
                    cnt++;  
                z++;
            };                  

            if(fields_numeric[i] == 0)
            {    
                const unsigned char *s1 = (const unsigned char*)&(t1[j]);
                const unsigned char *s2 = (const unsigned char*)&(t2[z]);
                unsigned char c1, c2;

                do
                {
                    c1 = (unsigned char) *s1++;
                    c2 = (unsigned char) *s2++;
                    if (c2 == delimiter) {
                        if(c1 != c2)
                            return 1;                    
                        else break;
                    }
                    if(c1 == delimiter)
                        return 0;
                }
                while (c1 == c2);
                if(c1 != c2)
                    return c1 > c2;        

            }
            else
            {
                double r1, r2; 
                const char *p;
                double sign, value, scale;
                int frac;
                for(unsigned int zz = 0; zz < 2; zz++) {       
                    if(zz == 0)        
                        p = (char*)&(t1[j]);
                    else
                        p = (char*)&(t2[z]);

                    while (*p == ' ') {
                        p += 1;
                    }

                    sign = 1.0;
                    if (*p == '-') {
                        sign = -1.0;
                        p += 1;
                    } else
                        if (*p == '+') {
                            p += 1;
                        }

                    for (value = 0.0; *p >= '0' && *p <= '9'; p += 1) {
                        value = value * 10.0 + (*p - '0');
                    }

                    if (*p == '.') {
                        double pow10 = 10.0;
                        p += 1;
                        while (*p >= '0' && *p <= '9') {
                            value += (*p - '0') / pow10;
                            pow10 *= 10.0;
                            p += 1;
                        }
                    }

                    frac = 0;
                    scale = 1.0;
                    if(zz == 0)        
                        r1 = sign * (frac ? (value / scale) : (value * scale));
                    else 
                        r2 = sign * (frac ? (value / scale) : (value * scale));

                };    

                if(r1 != r2)
                    return r1 > r2;
            }  
        };  
        return 0;  
    }   
};




struct sort_str
{
    char delimiter;
    sort_str(char _delimiter) {
        delimiter = _delimiter;
    }
    __host__ __device__
    bool operator()(const char* t1, const char* t2)
    {
        const unsigned char *s1 = (const unsigned char *) t1;
        const unsigned char *s2 = (const unsigned char *) t2;
        unsigned char c1, c2;
        do
        {
            c1 = (unsigned char) *s1++;
            c2 = (unsigned char) *s2++;
            if (c1 == delimiter)
                return c2 != delimiter;
            if (c2 == delimiter)
                return 0;            
        }
        while (c1 == c2);
        return c1 < c2;        
    }	
};

struct sort_str_desc
{
    char delimiter;
    sort_str_desc(char _delimiter) {
        delimiter = _delimiter;
    }
    __host__ __device__
    bool operator()(const char* t1, const char* t2)
    {
        const unsigned char *s1 = (const unsigned char *) t1;
        const unsigned char *s2 = (const unsigned char *) t2;
        unsigned char c1, c2;
        do
        {
            c1 = (unsigned char) *s1++;
            c2 = (unsigned char) *s2++;
            if (c2 == delimiter)
                return c1 != delimiter;
            if (c1 == delimiter)
                return 0;            
        }
        while (c1 == c2);
        return c1 > c2;        
    }   
};


typedef thrust::tuple<char, size_t> Tuple;

struct count_newlines {
    __host__ __device__ bool operator()(const Tuple& t) 
    {
        return t.get<0>() == '\n';
    }    
};

struct set_address
{
    const char *s;
    const unsigned int *offset;
    char **dest;

    set_address(const char *_s, const unsigned int *_offset, char **_dest):
        s(_s), offset(_offset), dest(_dest) {}
    template <typename IndexType>
    __host__ __device__
    void operator()(const IndexType & i) {
        dest[i] = (char*)s + offset[i];
    }
};


void str_sort(std::vector<unsigned int>& keys, std::vector<unsigned int>& keys_numeric, thrust::device_vector<char>& device_file_buffer,
				thrust::device_vector<char>& device_file_buffer_out, thrust::device_vector<char>& delimiter, size_t read_cnt,
                size_t first_offset, bool& file_to_rewind, bool reverse);
size_t str_merge(std::vector<unsigned int>& keys, std::vector<unsigned int>& keys_numeric, thrust::device_vector<char>& device_file_buffer,
                thrust::device_vector<char>& device_file_buffer_out, thrust::device_vector<char>& delimiter, size_t read_cnt,
                size_t first_offset, bool& file_to_rewind, bool reverse);
uint64_t filesize(const char* filename);				
