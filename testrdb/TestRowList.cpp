// testrdb.cpp : This file contains the 'main' function. Program execution begins and ends there.
//

#include <iostream>
#include "TestCase.h"
#include "RowList.h"

void TestRowList()
{
	rdb::RowList rows(2);
	std::cout << "after construct RowList\n"; std::string line; std::getline(std::cin, line);
	for (int i = 0; i < 10000; ++i)
	{
		rows.AppendRowBegin();
		rows.AppendColValue(i);
		rows.AppendColValue(i * i);
		if (rows.AppendRowDone(i))
			break;
	}
	std::cout << "after inset rows into RowList\n"; std::getline(std::cin, line);
	for (uint64_t i = 0; i < rows.GetRowCnt(); ++i)
	{
		for (uint32_t j = 0; j < rows.GetColCnt(); ++j)
		{
			auto p = rows.GetColValue(i, j);
			if (p)
				std::cout << (int)*p << "\t";
			else
				std::cout << "<NULL>" << "\t";
		}
		std::cout << std::endl;
	}
}

//static AddTestCase addtestcase(TestRowList);