#pragma once

#include <sstream>
#include <fstream>
#include <functional>
#include <thread>
#include <map>
#include <vector>
#include <limits>

using namespace std;

template<typename key_Ty, typename value_Ty>
struct map_reduce
{
	using map_cont_Ty = map<key_Ty, value_Ty>;
	using reduce_cont_Ty = multimap<key_Ty, value_Ty>;

	void map_thread(	function<bool(map_cont_Ty&, const string&)> callout,
						map_cont_Ty& res_map,
						const string& file_path, 
						const streampos block_size,
						const size_t block_num)
	{
		streampos offset = block_num * block_size;

		ifstream data_file(file_path, ios::in);
		data_file.seekg(offset, ios::beg);

		string data_record;

		//Skip trimmed string
		if (block_num != 0)
			getline(data_file, data_record);

		while (true)
		{
			getline(data_file, data_record);

			cout << this_thread::get_id() << ":";
			cout << data_record << endl;

			//Call User method
			if(!data_record.empty())
				callout(res_map, data_record);

			if (data_file.tellg() - offset > block_size)
				break;

			if (data_file.eof())
				break;
		}
	}

	vector<map_cont_Ty> do_map(const string& file_path, function<bool(map_cont_Ty&, const string&)> callout, size_t nmap)
	{
		ifstream data_file(file_path, ios::in);

		data_file.seekg(0, ios::end);
		streampos file_size = data_file.tellg();
		data_file.close();

		if (file_size == -1)
			return vector<map_cont_Ty>();
			//Return empty container as err
			//Throw exception unstead?

		streampos block_size = file_size / nmap;

		if (!block_size)
		{
			nmap = 1;
			block_size = file_size;
		}

		//Start map threads
		vector<shared_ptr<thread>> mapper_threads;
		vector<map_cont_Ty> map_result(nmap);

		for (size_t i = 0; i < nmap; i++)
		{
			shared_ptr<thread> t = make_shared<thread>(	&map_reduce::map_thread,
														this,
														callout,
														ref(map_result[i]), 
														file_path, 
														block_size, i);
			mapper_threads.push_back(t);
		}

		//Wait all map threads
		for (size_t i = 0; i < nmap; i++)
			mapper_threads[i]->join();

		return map_result;
	}

	vector<reduce_cont_Ty> do_shuffle(vector<map_cont_Ty>& map_result)
	{
		vector<reduce_cont_Ty> shuffle_result;
		reduce_cont_Ty general_cont;

		for (auto cur_map : map_result)
		{
			for (auto elem : cur_map)
			{
				general_cont.insert({ elem.first, elem.second });
			}
		}

		reduce_cont_Ty tmp_cont;

		auto key = general_cont.begin()->first;

		for(auto g : general_cont)
		{			
			if (key != g.first)
			{
				shuffle_result.push_back(tmp_cont);
				tmp_cont.clear();
			}

			tmp_cont.insert(g);
			key = g.first;
		}

		shuffle_result.push_back(tmp_cont);

		return shuffle_result;
	}

	void reduce_thread(	function<bool(map_cont_Ty& , reduce_cont_Ty&)> callout,
						map_cont_Ty& res_map,
						const vector<reduce_cont_Ty>& shuffle_result, 
						size_t block_start, size_t block_end)
	{
		for (size_t i = block_start; i < block_end; i++)
		{
			reduce_cont_Ty tmp = shuffle_result[i];
			callout(res_map, tmp);
		}
	}

	vector<map_cont_Ty> do_reduce(const vector<reduce_cont_Ty>& shuffle_result, function<bool(map_cont_Ty&, reduce_cont_Ty&)> callout, size_t nreduce)
	{
		//Start map threads
		vector<shared_ptr<thread>> reduce_threads;

		size_t shuffle_count = shuffle_result.size();
		
		if (!shuffle_count)
			return vector<map_cont_Ty>();

		size_t block_size = shuffle_count / nreduce;
		size_t block_count = nreduce;
		size_t last_block_size = shuffle_count - block_count * block_size;
		
		if (!block_size)
		{
			block_size = 1;
			last_block_size = 0;
			block_count = shuffle_count;
		}

		vector<map_cont_Ty> reduce_result(block_count);

		size_t block_start = 0;
		size_t block_end = block_start + block_size;

		for (size_t i = 0; i < block_count; i++)
		{
			//Split last block over the threads by 1 item for every thread
			if (last_block_size)
			{
				block_end++;
				last_block_size--;
			}

			cout << "Reduce from " << block_start << " to " << block_end << endl;

			shared_ptr<thread> t = make_shared<thread>(	&map_reduce::reduce_thread,
														this,
														callout,
														ref(reduce_result[i]),
														shuffle_result, block_start, block_end);
			reduce_threads.push_back(t);

			block_start = block_end;
			block_end = block_start + block_size;
		}

		//Wait all reduce threads
		for (size_t i = 0; i < block_count; i++)
			reduce_threads[i]->join();

		return reduce_result;
	}
};
