// task_12_mapreduce.cpp : This file contains the 'main' function. Program execution begins and ends there.
//

#include <iostream>

#include "map_reduce.h"
#include "min_prefix.h"

using namespace std;

int main(int argc, char* argv[])
{
	if (argc < 4)
	{
		cout << "Usage mapreduce [src] [mnum] [rnum]" << endl;
		return 0;
	}

	string file_path = string(argv[1]);
	size_t nmap = atoi(argv[2]);
	size_t nreduce = atoi(argv[3]);

	if (nmap <= 0 || nreduce <= 0)
	{
		cout << "mnum\\rnum invalid" << endl;
		return 0;
	}

	ifstream f(file_path, ios::in);
	if (!f.is_open())
	{
		cout << "Can't open file " << file_path << endl;
		return 0;
	}
	f.close();

	vector<string> min_prefix = min_prefix::find_min_prefix(file_path, nmap, nreduce);

	cout << "Save min prefix" << endl;

	ofstream pr_f("prefix_log.txt", ios::out);

	//Save prefix to file
	for (auto pref : min_prefix)
	{
		cout << pref << endl;
		pr_f << pref << endl;
	}

	pr_f.close();

	return 0;
}