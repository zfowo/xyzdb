
#ifndef MISC_UTILS_H
#define MISC_UTILS_H

#include <utility>
#include "MyAlloc.h"
#include "BaseTypes.h"

namespace rdb
{
	// from boost hash_combine
	inline uint64_t HashValueCombine(uint64_t h1, uint64_t h2)
	{
		return h1 ^ (h2 + 0x9E3779B9 + (h1 << 6) + (h1 >> 2));
	}
	inline uint64_t HashBytes(const uint8_t * data, size_t sz)
	{
		uint64_t h = 0;
		for (size_t i = 0; i < sz; ++i)
			h = h * 101 + data[i];
		return h;
	}

	// endian
	inline bool IsLittleEndian()
	{
		uint32_t v = 0x01020304;
		return (0x04 == *(uint8_t *)&v);
	}
	inline int16_t LocalToBigEndian(int16_t n)
	{
		if (IsLittleEndian())
		{
			uint8_t * p = (uint8_t *)&n;
			std::swap(p[0], p[1]);
		}
		return n;
	}
	inline uint16_t LocalToBigEndian(uint16_t n)
	{
		return (uint16_t)LocalToBigEndian((int16_t)n);
	}
	inline int32_t LocalToBigEndian(int32_t n)
	{
		if (IsLittleEndian())
		{
			uint8_t * p = (uint8_t *)&n;
			std::swap(p[0], p[3]);
			std::swap(p[1], p[2]);
		}
		return n;
	}
	inline uint32_t LocalToBigEndian(uint32_t n)
	{
		return (uint32_t)LocalToBigEndian((int32_t)n);
	}
	inline int64_t LocalToBigEndian(int64_t n)
	{
		if (IsLittleEndian())
		{
			uint8_t * p = (uint8_t *)&n;
			std::swap(p[0], p[7]);
			std::swap(p[1], p[6]);
			std::swap(p[2], p[5]);
			std::swap(p[3], p[4]);
		}
		return n;
	}
	inline uint64_t LocalToBigEndian(uint64_t n)
	{
		return (uint64_t)LocalToBigEndian((int64_t)n);
	}
#if defined(HAVE_MYINT128)
	inline void LocalToBigEndian(myint128 & n)
	{
		if (IsLittleEndian())
		{
			uint8_t * p = (uint8_t *)&n;
			std::swap(p[0], p[15]);
			std::swap(p[1], p[14]);
			std::swap(p[2], p[13]);
			std::swap(p[3], p[12]);
			std::swap(p[4], p[11]);
			std::swap(p[5], p[10]);
			std::swap(p[6], p[9]);
			std::swap(p[7], p[8]);
		}
	}
#endif

	inline int16_t BigEndianToLocal(int16_t n) { return LocalToBigEndian(n); }
	inline uint16_t BigEndianToLocal(uint16_t n) { return LocalToBigEndian(n); }
	inline int32_t BigEndianToLocal(int32_t n) { return LocalToBigEndian(n); }
	inline uint32_t BigEndianToLocal(uint32_t n) { return LocalToBigEndian(n); }
	inline int64_t BigEndianToLocal(int64_t n) { return LocalToBigEndian(n); }
	inline uint64_t BigEndianToLocal(uint64_t n) { return LocalToBigEndian(n); }
#if defined(HAVE_MYINT128)
	inline void BigEndianToLocal(myint128 & n) { LocalToBigEndian(n); }
#endif

} // end of namespace rdb

#endif // end of MISC_UTILS_H
