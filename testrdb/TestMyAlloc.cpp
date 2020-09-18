
#include <iostream>
#include "MyAlloc.h"
#include "TestCase.h"

void TestMyAlloc()
{
	std::cout << "Start TestMyAlloc..." << std::endl;

	rdb::TwoPhaseAllocator tpa(4);
	rdb::CurrentAllocatorSetter cas(&tpa);
	rdb::myvector<int64_t> vec;
	std::cout << "before reserve" << std::endl;
	//vec.reserve(10);
	for (int i = 0; i < 10; ++i)
		vec.push_back(i);

	std::cout << "End TestMyAlloc" << std::endl;
}

static AddTestCase addtestcase(TestMyAlloc);
