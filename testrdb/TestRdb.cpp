
#include <iostream>
#include <string>
#include "TestCase.h"

int main(int argc, char * argv[])
{
	for (auto & tf : alltestcases)
	{
		tf();
	}
	std::cout << "press any key to exit...";
	std::string line;
	std::getline(std::cin, line);
	return 0;
}
