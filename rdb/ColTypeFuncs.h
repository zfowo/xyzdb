
#ifndef COL_TYPE_FUNCS_H
#define COL_TYPE_FUNCS_H

#include "BaseTypes.h"
#include "RdbException.h"

namespace rdb
{
	// 返回列类型名
	const std::string & ColTypeName(ColType ct);
	// 对于可变大小类型，比如TEXT/BYTES类型，返回-1。
	int ColTypeSize(ColType ct);

	inline bool IsDroppedColType(ColType ct)
	{
		return ct < 0;
	}
	inline bool IsArrayColType(ColType ct)
	{
		assert(ct > 0);
		return (uint32_t)ct & (uint32_t)ArrayColTypeMask;
	}
	inline ColType MakeArrayColType(ColType element_ct)
	{
		assert(element_ct > 0 && !IsArrayColType(element_ct));
		if (element_ct == CT_BLOB)
			throw RdbException("array element type can not be BLOB");
		return (ColType)((uint32_t)element_ct | (uint32_t)ArrayColTypeMask);
	}
	inline ColType GetArrayElementColType(ColType array_ct)
	{
		assert(array_ct > 0 && IsArrayColType(array_ct));
		return (ColType)((uint32_t)array_ct & ~(uint32_t)ArrayColTypeMask);
	}

} // end of namespace rdb

#endif // end of COL_TYPE_FUNCS_H
