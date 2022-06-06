#pragma once

#include <set>

#include "map_reduce.h"

namespace min_prefix
{
	using map_cont_Ty = map<string, size_t>;
	using reduce_cont_Ty = multimap<string, size_t>;

	bool map_callout(map_cont_Ty& res_map, const string& data_record)
	{
		for (size_t i = 1; i <= data_record.length(); i++)
		{
			string prefix = data_record.substr(0, i);
				
			if (res_map.contains(prefix))
				res_map[prefix]++;
			else
				res_map[prefix] = 1;
		}

		return true;
	}

	bool reduce_callout(map_cont_Ty& res_map, reduce_cont_Ty& record_for_reduce)
	{
		for (auto elem : record_for_reduce)
		{
			if (!res_map.contains(elem.first))
				res_map[elem.first] = elem.second;
			else
				res_map[elem.first] += elem.second;
		}

		return true;
	}

	vector<string> find_min_prefix(const string& file_path, size_t nmap, size_t nreduce)
	{
		map_reduce<string, size_t> m;

		vector<map_cont_Ty> map_result = m.do_map(file_path, &min_prefix::map_callout, nmap);
		vector<reduce_cont_Ty> shuffle_result =  m.do_shuffle(map_result);
		vector<map_cont_Ty> reduce_result = m.do_reduce(shuffle_result, &min_prefix::reduce_callout, nreduce);

		vector<string> min_prefix;
		string last_prefix = "";

		//Minimize prefix map
		for (auto reduce_r : reduce_result)
		{
			for (auto elem : reduce_r)
			{
				if (elem.second != 1)
					continue;

				if (!last_prefix.empty())
				{
					//Is a minimal prefix??
					//Or prev prefix is a substring of current?
					if (elem.first.find(last_prefix) != string::npos)
						continue;
				}

				//All prefix sorted yet
				last_prefix = elem.first;
				min_prefix.emplace_back(elem.first);
			}
		}

		return min_prefix;
	}
}
