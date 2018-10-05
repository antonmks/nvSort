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
using namespace mgpu;


void str_sort(std::vector<unsigned int>& keys, std::vector<unsigned int>& keys_numeric, thrust::device_vector<char>& device_file_buffer,
		      thrust::device_vector<char>& device_file_buffer_out, thrust::device_vector<char>& delimiter, size_t read_cnt,
		      size_t first_offset, bool& file_to_rewind, bool reverse)
{

    standard_context_t context(0);	    
	thrust::counting_iterator<unsigned int> begin(0);
	char h_delimiter = delimiter[0];
	std::clock_t start1 = std::clock();			
 
	auto begin_keys = thrust::make_zip_iterator(thrust::make_tuple(device_file_buffer.begin(), thrust::counting_iterator<int>(1)));
    auto end_keys = thrust::make_zip_iterator(thrust::make_tuple(device_file_buffer.begin() + read_cnt, thrust::counting_iterator<int>(read_cnt)));
    auto cnt = thrust::count(device_file_buffer.begin(), device_file_buffer.begin() + read_cnt,'\n');
    thrust::device_vector<int> nl_pos(cnt);
    auto res_keys = thrust::make_zip_iterator(thrust::make_tuple(thrust::make_discard_iterator(), nl_pos.begin()));
    thrust::copy_if(begin_keys, end_keys, res_keys, count_newlines());  
	
	//for(int z = 0; z < nl_pos.size(); z++)
	//	cout << "nlpos " << nl_pos[z] << endl;
	thrust::device_vector<unsigned int> key_num(1);	
	if(nl_pos.back() == read_cnt) {
	    nl_pos.erase(nl_pos.end()-1);	    
	};
	nl_pos.insert(nl_pos.begin(), 0);

	//for(int z = 0; z < nl_pos.size(); z++)
	//	cout << "nlpos fin  " << nl_pos[z] << endl;


	thrust::device_vector<unsigned int> perm(cnt);	
	thrust::sequence(perm.begin(), perm.end(), 0, 1);			
	
	for(unsigned int i = 0; i < keys.size(); i++) {

		key_num[0] = keys[keys.size()-1-i];
		if(keys_numeric[keys.size()-1-i] == 1) {
			thrust::device_vector<double> device_key_double(cnt);	
			thrust::device_vector<double> device_key_tmp(cnt);	
					
			gpu_atof at((const char*)thrust::raw_pointer_cast(device_file_buffer.data()), thrust::raw_pointer_cast(device_key_double.data()),               
						(const unsigned int *)thrust::raw_pointer_cast(key_num.data()), (const char*)thrust::raw_pointer_cast(delimiter.data()),
						(const int*)thrust::raw_pointer_cast(nl_pos.data()));
			thrust::for_each(begin, begin + cnt, at);						
			thrust::gather(perm.begin(), perm.end(), device_key_double.begin(), device_key_tmp.begin());
		    if(reverse) {	
			    thrust::stable_sort_by_key(device_key_tmp.begin(), device_key_tmp.end(), perm.begin(), thrust::greater<double>());
			}
			else {
				thrust::stable_sort_by_key(device_key_tmp.begin(), device_key_tmp.end(), perm.begin());	
			}    
		}
		else {
			
			thrust::device_vector<char*> field_pos(cnt);	
			thrust::device_vector<char*> field_pos_tmp(cnt);			
			gpu_find_pos fp((const char*)thrust::raw_pointer_cast(device_file_buffer.data()), (char**)thrust::raw_pointer_cast(field_pos.data()),               
						(const unsigned int *)thrust::raw_pointer_cast(key_num.data()), (const char*)thrust::raw_pointer_cast(delimiter.data()),
						(const int*)thrust::raw_pointer_cast(nl_pos.data()));
			thrust::for_each(begin, begin + cnt, fp);			
			thrust::gather(perm.begin(), perm.end(), field_pos.begin(), field_pos_tmp.begin());
			if(reverse) {
				sort_str_desc f(h_delimiter);	
				mergesort(thrust::raw_pointer_cast(field_pos_tmp.data()), thrust::raw_pointer_cast(perm.data()), perm.size(), f, context); 
			}	
			else {
				sort_str f(h_delimiter);	
				mergesort(thrust::raw_pointer_cast(field_pos_tmp.data()), thrust::raw_pointer_cast(perm.data()), perm.size(), f, context); 
			}
			//cudaDeviceSynchronize();
		};	
	}	

	thrust::device_vector<int> nl_len(cnt);
	thrust::transform(nl_pos.begin()+1, nl_pos.end(), nl_pos.begin(), nl_len.begin(),  thrust::minus<int>());
	nl_len[cnt-1] = read_cnt - nl_pos[cnt-1];
	//cout << "last len " << nl_len[cnt-1] << " " << read_cnt << " " << nl_pos[cnt-1] << endl;
		
	thrust::device_vector<int> nl_len_g(cnt);
    thrust::device_vector<int> nl_len_pos(cnt);
	thrust::gather(perm.begin(), perm.end(), nl_len.begin(), nl_len_g.begin());	
	thrust::exclusive_scan(nl_len_g.begin(), nl_len_g.end(), nl_len_g.begin());	
    thrust::scatter(nl_len_g.begin(), nl_len_g.end(), perm.begin(), nl_len_pos.begin());	
    interval_scatter(thrust::raw_pointer_cast(device_file_buffer.data()), read_cnt, thrust::raw_pointer_cast(nl_pos.data()), nl_pos.size(), thrust::raw_pointer_cast(nl_len_pos.data()), thrust::raw_pointer_cast(device_file_buffer_out.data()), context);        
}	


size_t str_merge(std::vector<unsigned int>& keys, std::vector<unsigned int>& keys_numeric, thrust::device_vector<char>& device_file_buffer,
		         thrust::device_vector<char>& device_file_buffer_out, thrust::device_vector<char>& delimiter,
		         size_t read_cnt, size_t first_offset, bool& file_to_rewind, bool reverse)
{
	
	thrust::counting_iterator<unsigned int> begin(0);
	char h_delimiter = delimiter[0];
	std::clock_t start1 = std::clock();		
    
	auto begin_keys = thrust::make_zip_iterator(thrust::make_tuple(device_file_buffer.begin(), thrust::counting_iterator<int>(1)));
    auto end_keys = thrust::make_zip_iterator(thrust::make_tuple(device_file_buffer.begin() + read_cnt, thrust::counting_iterator<int>(read_cnt)));
    auto cnt = thrust::count(device_file_buffer.begin(), device_file_buffer.begin() + read_cnt,'\n');
    auto first_cnt = thrust::count(device_file_buffer.begin(), device_file_buffer.begin() + first_offset,'\n');
    //cout << "test lines " << cnt << " " << first_cnt << " " << read_cnt << endl;
    
    thrust::device_vector<int> nl_pos(cnt);
    auto res_keys = thrust::make_zip_iterator(thrust::make_tuple(thrust::make_discard_iterator(), nl_pos.begin()));
    thrust::copy_if(begin_keys, end_keys, res_keys, count_newlines());  
	
	thrust::device_vector<unsigned int> key_num(1);	
	if(nl_pos.back() == read_cnt) {
	    nl_pos.erase(nl_pos.end()-1);	    
	};
	nl_pos.insert(nl_pos.begin(), 0);


	thrust::device_vector<unsigned int> perm(cnt);	
    thrust::device_vector<unsigned int> perm_tmp(cnt);  
	thrust::sequence(perm.begin(), perm.end(), 0, 1);		
    thrust::device_vector<char*> nl_pos_char(cnt);    

    set_address ff((const char*)thrust::raw_pointer_cast(device_file_buffer.data()),
                   (const unsigned int*)thrust::raw_pointer_cast(nl_pos.data()), 
                   thrust::raw_pointer_cast(nl_pos_char.data()));
    thrust::for_each(begin, begin + cnt, ff);    

    thrust::device_vector<char*> field_pos_tmp(cnt);      
    thrust::device_vector<unsigned int> d_keys(keys.size());
    thrust::copy(keys.data(), keys.data()+keys.size(), d_keys.begin());
    thrust::device_vector<unsigned int> d_keys_numeric(keys.size());    
    thrust::copy(keys_numeric.data(), keys_numeric.data()+keys.size(), d_keys_numeric.begin());

    if(reverse) {
	    compare_fields_desc f(h_delimiter, thrust::raw_pointer_cast(d_keys.data()), thrust::raw_pointer_cast(d_keys_numeric.data()), keys.size());
	    //merge(nl_pos_char.data(), perm.begin(), first_cnt,
	    //      nl_pos_char.data() + first_cnt,  perm.begin() + first_cnt, cnt-first_cnt,
	    //      field_pos_tmp.data(), perm_tmp.begin(), f, context);	
	}
	else{
	    compare_fields f(h_delimiter, thrust::raw_pointer_cast(d_keys.data()), thrust::raw_pointer_cast(d_keys_numeric.data()), keys.size());
	    //merge(nl_pos_char.data(), perm.begin(), first_cnt,
	    //      nl_pos_char.data() + first_cnt,  perm.begin() + first_cnt, cnt-first_cnt,
	    //      field_pos_tmp.data(), perm_tmp.begin(), f, context);		
	}
	
	thrust::device_vector<int> nl_len(cnt);
	thrust::transform(nl_pos.begin()+1, nl_pos.end(), nl_pos.begin(), nl_len.begin(),  thrust::minus<int>());
	nl_len[cnt-1] = read_cnt - nl_pos[cnt-1];

	thrust::device_vector<int> nl_len_g(cnt);
    thrust::device_vector<int> nl_len_pos(cnt);
	thrust::gather(perm_tmp.begin(), perm_tmp.end(), nl_len.begin(), nl_len_g.begin());	
	thrust::exclusive_scan(nl_len_g.begin(), nl_len_g.end(), nl_len_g.begin());	
    thrust::scatter(nl_len_g.begin(), nl_len_g.end(), perm_tmp.begin(), nl_len_pos.begin());	

	//for(int z = 0; z < 10; z++)
	//	cout << "nl pos  " << nl_pos[z] << " " << nl_len_pos[z] << endl;
    //interval_scatter(thrust::raw_pointer_cast(device_file_buffer.data()), read_cnt, thrust::raw_pointer_cast(nl_pos.data()), nl_pos.size(), 
    //	                                      thrust::raw_pointer_cast(nl_len_pos.data()), thrust::raw_pointer_cast(device_file_buffer_out.data()), context);        
    auto new_pos1 = nl_len_pos[first_cnt-1];
    auto new_pos2 = nl_len_pos[cnt-1];    
    //cout << "pos1 pos 2 " << new_pos1 << " " << new_pos2 << endl;
    if(new_pos1 < new_pos2) 
    {
        file_to_rewind = 1;
        return new_pos1 + nl_len[first_cnt-1];	
    }
    else
    {
   	    file_to_rewind = 0;
        return new_pos2 + nl_len.back();	        
    }    
}	


uint64_t filesize(const char* filename)
{
    ifstream in(filename, ios::binary | ios::ate);
	if(!in) {
		cout << "Could not open file " << filename << endl;
		exit(0);
	};	
    return in.tellg();
}

