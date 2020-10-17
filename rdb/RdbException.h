
#ifndef RDB_EXCEPTION_H
#define RDB_EXCEPTION_H

#include <string>
#include <stdexcept>

namespace rdb
{

	class RdbException : public std::runtime_error
	{
	public:
		RdbException(const std::string & errmsg) : runtime_error(errmsg)
		{}
	};
	
	class RdbBadAllocException : RdbException 
	{
	public:
		RdbBadAllocException(size_t requested_size) 
			: RdbException("can not alloc memory. requested size:" + std::to_string(requested_size))
		{}
	};

} // end of namespace rdb

#endif // end of RDB_EXCEPTION_H
