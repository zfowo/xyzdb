
#ifndef MYBITMAP_H
#define MYBITMAP_H

#include <algorithm>
#include "MyAlloc.h"
#include "NoCopyMove.h"
#include "RdbException.h"

namespace rdb
{
	class MyBitmap
	{
	private:
		uint8_t * data;
		int bit_cnt;
		int byte_cnt;
		bool need_free;
	public:
		MyBitmap(int bitcnt)
		{
			this->bit_cnt = bitcnt;
			this->byte_cnt = (this->bit_cnt + 7) / 8;
			this->data = (uint8_t *)myalloc(this->byte_cnt, true);
			if (!this->data)
				throw RdbBadAllocException(this->byte_cnt);
			this->need_free = true;
		}
		MyBitmap(int bitcnt, uint8_t * buf, bool copy)
		{
			this->bit_cnt = bitcnt;
			this->byte_cnt = (this->bit_cnt + 7) / 8;
			if (copy)
			{
				this->data = (uint8_t *)myalloc(this->byte_cnt);
				if (!this->data)
					throw RdbBadAllocException(this->byte_cnt);
				memcpy(this->data, buf, this->byte_cnt);
			}
			else
			{
				this->data = buf;
			}
			this->need_free = copy;
		}
		~MyBitmap()
		{
			if (this->need_free && this->data)
				myfree(this->data);
		}

		NO_COPY_MOVE(MyBitmap);

		int ByteCount() const { return this->byte_cnt; }
		int BitCount() const { return this->bit_cnt; }
		uint8_t * Data() const { return this->data; }

		MyBitmap & Set(int idx)
		{
			assert(idx >= 0 && idx < this->bit_cnt);
			int byteidx = idx / 8;
			int bitidx = idx % 8;
			uint8_t c = this->data[byteidx];

			this->data[byteidx] = (uint8_t)(c | (1 << bitidx));
			return *this;
		}
		MyBitmap & Unset(int idx)
		{
			assert(idx >= 0 && idx < this->bit_cnt);
			int byteidx = idx / 8;
			int bitidx = idx % 8;
			uint8_t c = this->data[byteidx];

			this->data[byteidx] = (uint8_t)(c & ~(1 << bitidx));
			return *this;
		}
		bool IsSet(int idx) const
		{
			assert(idx >= 0 && idx < this->bit_cnt);
			int byteidx = idx / 8;
			int bitidx = idx % 8;
			uint8_t c = this->data[byteidx];

			return c & (1 << bitidx);
		}
		static bool IsSet(uint8_t * data, int idx)
		{
			assert(idx > 0);
			int byteidx = idx / 8;
			int bitidx = idx % 8;
			uint8_t c = data[byteidx];
			return c & (1 << bitidx);
		}

		uint8_t * Release(int * bytecnt = nullptr)
		{
			uint8_t * p = this->data;
			this->data = nullptr;
			if (bytecnt)
				*bytecnt = this->byte_cnt;
			return p;
		}
		mystring ToBytes() const
		{
			return mystring((const char *)this->data, this->byte_cnt);
		}

		std::string ToString() const
		{
			static std::string table[16] = { "0000", "0001", "0010", "0011", "0100", "0101", "0110", "0111",
											 "1000", "1001", "1010", "1011", "1100", "1101", "1110", "1111" };
			std::string s;
			s.reserve(this->byte_cnt * 8 + this->byte_cnt);
			for (int i = 0; i < this->byte_cnt; ++i)
			{
				uint8_t c = this->data[i];
				std::string s2 = table[c >> 4] + table[c & 0x0F];
				std::reverse(s2.begin(), s2.end());
				s.append(s2).append(1, ' ');
			}
			return s;
		}
	};

} // end of namespace rdb

#endif // end of MYBITMAP_H
