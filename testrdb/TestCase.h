
#ifndef TEST_CASE_H
#define TEST_CASE_H

#include <vector>
#include <functional>

using TestCaseFunc = std::function<void()>;

extern std::vector<TestCaseFunc> alltestcases;

struct AddTestCase
{
	AddTestCase(const TestCaseFunc & t)
	{
		alltestcases.push_back(t);
	}
};

#endif // end of TEST_CASE_H
