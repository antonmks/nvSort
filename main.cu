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
#include "str_sort.h"

using namespace std;

int main(int ac, char **av)
{
    string file_name;
	thrust::device_vector<char> delimiter(1);
	delimiter[0] = ' ';
	string usage = "Usage : nvsort [-t FIELD_SEPARATOR] [-n NUMERIC_SORT] [-k KEYS] FILE";
	vector<unsigned int> keys;
	vector<unsigned int> keys_numeric;
	uint64_t chunk_sz = 800000000; // segment size, 800MB is ok for GTX 1080
	bool reverse = 0;
	map<size_t, vector<string> > files_to_merge;
	thrust::host_vector<char, thrust::cuda::experimental::pinned_allocator<char> > file_buffer(chunk_sz);
	thrust::device_vector<char> device_file_buffer(chunk_sz);
	thrust::device_vector<char> device_file_buffer_out(chunk_sz);	
	
	std::clock_t start1 = std::clock();	
	
	if (ac <= 1) {
        cout << usage << endl;
        exit(1);
    };


    for(auto i = 1; i < ac; i++) {
        if(strcmp(av[i],"-t") == 0) {
            if(i+1 < ac) {
                delimiter[0] = av[i+1][0];
				i++;
            }
            else {
                cout << usage << endl;
                exit(1);
            };
        }
        else if(strcmp(av[i],"-k") == 0) {
			if(i+1 < ac) {
				if(av[i+1][strlen(av[i+1])-1] == 'n') {
					keys_numeric.push_back(1);
					av[i+1][strlen(av[i+1])-1] = '\0';
				}
				else 
					keys_numeric.push_back(0);
				keys.push_back(atoi(av[i+1]));
				i++;
			}	
			else {
                cout << usage << endl;
                exit(1);
            };
		}
        else if(strcmp(av[i],"-r") == 0) {
            reverse = 1;
        }	
		else 	
			file_name = av[i];			
    };
	if(file_name.length() == 0)
	{
      cout << usage << endl;
      exit(1);
    };		

    if(keys.size() == 0) { //sort by entire strings
    	keys.push_back(1);
    	keys_numeric.push_back(0);
    	delimiter[0] = '\n';
    };	
	
	//cout << "Sep " << delimiter[0] << " file " << file_name << endl;
	//for(int i = 0; i < keys.size(); i++)
	//	cout << "Key " << keys[i] << " " << keys_numeric[i] << endl;
	
		
	auto file_size = filesize(file_name.c_str());	
	ifstream f(file_name.c_str(), ios::binary);	

	//Sort phase	
	unsigned int tot_read = 0;
	bool first = 1;

	for(unsigned int k = 0; k <= file_size/chunk_sz + 1 && f; k++) {	
		
		f.read(file_buffer.data(), chunk_sz);
		uint64_t read_cnt = f.gcount();
		if(read_cnt == chunk_sz) {
			int j = 0;
			while(file_buffer[chunk_sz-j-1] != '\n')
				j++;
			read_cnt = read_cnt-j;				
			f.seekg(-j, f.cur);
			//cout << "sort rewind " << j << endl;
		};	
		tot_read = tot_read + read_cnt;
		
		thrust::copy(file_buffer.begin(), file_buffer.begin() + read_cnt, device_file_buffer.begin());		
		str_sort(keys, keys_numeric, device_file_buffer, device_file_buffer_out, delimiter, read_cnt, 0, first, reverse);		
		thrust::copy(device_file_buffer_out.begin(), device_file_buffer_out.begin() + read_cnt, file_buffer.begin());		

		auto file_out = file_name + ".sorted";	
		if(file_size/chunk_sz > 0) {
			file_out = file_out + '.' + to_string(k);
			files_to_merge[read_cnt].push_back(file_out);			
		};		
		fstream sorted_file(file_out.c_str(),ios::out|ios::binary);
		sorted_file.write((char *)file_buffer.data(), read_cnt);		
		sorted_file.close();			

	};
	f.close();
	//std::cout<< "sort phase time: " <<  ( ( std::clock() - start1 ) / (double)CLOCKS_PER_SEC ) <<  '\n';    	

	//merge phase
	unsigned int k = 0;	
	while(files_to_merge.size() > 1) {
		string first_file, second_file;
		auto it = files_to_merge.begin();
		first_file = it->second[0];
		if(it->second.size() > 1) {		
			second_file = it->second[1];
		}
		else {
			it++;
			second_file = it->second[0];
		};
		
		auto file_out = file_name + ".merged." + to_string(k++);	
		//cout << "files " << first_file << " " << second_file << " " << file_out << endl;
		ifstream f1(first_file.c_str(), ios::binary);	
		ifstream f2(second_file.c_str(), ios::binary);	 
		fstream sorted_file(file_out.c_str(),ios::out|ios::binary);       
        size_t total_written = 0;
        size_t cnt1, cnt2, rewind;	
        int j;
        uint64_t read_cnt = 0;
        bool file_to_rewind;          

		while (f1 && f2) {		   
		   
           f1.read(file_buffer.data(), chunk_sz/2);
           read_cnt = f1.gcount();
		   if (read_cnt == 0)
		   	  break;           
           if(read_cnt == chunk_sz/2) {
              j = 0;
              while(file_buffer[read_cnt-j-1] != '\n')
			     j++;
		      cnt1 = read_cnt-j;				
		      f1.seekg(-j, f1.cur); 
		   }
		   else
		   	   cnt1 = read_cnt;
           thrust::copy(file_buffer.begin(), file_buffer.begin() + cnt1, device_file_buffer.begin());		 

	   	   f2.read(file_buffer.data(), chunk_sz/2);
           read_cnt = f2.gcount();
		   if (read_cnt == 0)
		   	  break;                      
           if(read_cnt == chunk_sz/2) {
              j = 0;
              while(file_buffer[read_cnt-j-1] != '\n')
			     j++;
		      cnt2 = read_cnt-j;				
	          f2.seekg(-j, f2.cur);               
		   }
	       else
		      cnt2 = read_cnt;
           thrust::copy(file_buffer.begin(), file_buffer.begin() + cnt2, device_file_buffer.begin() + cnt1);	
           auto sorted_sz = str_merge(keys, keys_numeric, device_file_buffer, device_file_buffer_out, delimiter,
                                    cnt1 + cnt2, cnt1, file_to_rewind, reverse);	
           //cout << "sorted sz " << sorted_sz << endl; 
           if((!f1 && file_to_rewind == 1) || (!f2 && file_to_rewind == 0))   {                
              sorted_sz =  cnt1 + cnt2;    	
           };   
       	   thrust::copy(device_file_buffer_out.begin(), device_file_buffer_out.begin() + sorted_sz, file_buffer.begin());		
       	   sorted_file.write((char *)file_buffer.data(), sorted_sz);		
       	   total_written = total_written + sorted_sz;
       	   rewind = cnt1 + cnt2 - sorted_sz;
       	   //cout << "rewind " << file_to_rewind << " " << rewind << endl;
       	   if(file_to_rewind == 0) {
       	   	  if(rewind > 0) {
       	   		 if(!f1)
       	   	        f1.clear();       	   	
  	             f1.seekg(-rewind, f1.cur);  	             
       	   	  }
       	   }
       	   else {
       	   	  if(rewind > 0) {
       	   	     if(!f2) 
       	   	        f2.clear();
                 f2.seekg(-rewind, f2.cur);       	   
       	   	  }
       	   }
		}; 

		if(f1) {
		   do {	
			   f1.read(file_buffer.data(), chunk_sz/2);
	           read_cnt = f1.gcount();
	           sorted_file.write((char *)file_buffer.data(), read_cnt);		
	           //cout << "wrote " << read_cnt  << " from f1 " << endl;
	          }
	       while(f1);       
		}
		else if(f2) {
		   do {	
			   f2.read(file_buffer.data(), chunk_sz/2);
	           read_cnt = f2.gcount();
	           sorted_file.write((char *)file_buffer.data(), read_cnt);		
	           //cout << "wrote " << read_cnt  << " from f2 " << endl;
              } 
           while(f2);
		};
		total_written = total_written + read_cnt;


		f1.close();
		f2.close();
		sorted_file.close();		
		it = files_to_merge.begin();
		it->second.erase(it->second.begin());	
		if(it->second.size() > 0) {		
			it->second.erase(it->second.begin());
			if(it->second.size() == 0)
               files_to_merge.erase(it);
		}
		else {
			files_to_merge.erase(it++);
			it->second.erase(it->second.begin());
			if(it->second.size() == 0)
               files_to_merge.erase(it);
		};

		files_to_merge[total_written].push_back(file_out);			
		//remove(first_file.c_str());
		//remove(second_file.c_str());
	};
	
	auto file_merged = file_name + ".merged." + to_string(k-1);	
	auto file_final = file_name + ".sorted";	
	rename(file_merged.c_str(), file_final.c_str());

	//std::cout<< "merge phase time: " <<  ( ( std::clock() - start1 ) / (double)CLOCKS_PER_SEC ) <<  '\n';    	
    return 0;

}
