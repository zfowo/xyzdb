
#ifndef ROW_LIST_H
#define ROW_LIST_H

#include <assert.h>
#include <stdint.h>
#include <stdexcept>
#include <string>
#include <vector>
#include <functional>
#include <algorithm>

namespace rdb
{
	class RowList
	{
	public:
		RowList(const RowList & other) = delete;
		RowList & operator=(const RowList & other) = delete;

		RowList(uint32_t colcnt, bool nonull = false, uint64_t maxrowcnt = 1000000)
			: col_cnt(colcnt), no_null(nonull), max_row_cnt(maxrowcnt),
			sz_per_col(nonull ? sizeof(uintptr_t) : 2 * sizeof(uintptr_t)),
			sz_per_row(colcnt * sz_per_col + sizeof(uint64_t))
		{
			assert(colcnt > 0 && maxrowcnt > 0);
			this->cur_row_cnt = 0;
			this->col_cnt_in_cur_row = 0;

			this->row_data_buf = (char *)malloc(this->max_row_cnt * this->sz_per_row); // do not memset after malloc
			if (!this->row_data_buf)
				throw std::runtime_error("malloc fail in RowList.");
		}
		~RowList()
		{
			free(this->row_data_buf);
		}

		void AppendColValue(uintptr_t datum)
		{
			assert(this->col_cnt_in_cur_row < this->col_cnt);
			uintptr_t * p = this->GetColAddr();
			int i = 0;
			if (!this->no_null)
				p[i++] = 0;
			p[i] = datum;
			++this->col_cnt_in_cur_row;
		}
		void AppendColValue(const char * data, size_t len)
		{
			assert(this->col_cnt_in_cur_row < this->col_cnt);
			assert(data && len > 0);
			uintptr_t * p = this->GetColAddr();
			int i = 0;
			if (!this->no_null)
				p[i++] = 0;
			p[i] = this->AppendOtherData(data, len);
			++this->col_cnt_in_cur_row;
		}
		void AppendColValueNull()
		{
			assert(this->col_cnt_in_cur_row < this->col_cnt);
			if (this->no_null)
				throw std::runtime_error("BUG: should not call AppendColValueNull while RowList::no_null is true.");

			uintptr_t * p = this->GetColAddr();
			int i = 0;
			if (!this->no_null)
				p[i++] = 1;
			p[i] = 0;
			++this->col_cnt_in_cur_row;
		}
		void AppendRowBegin()
		{
			assert(this->cur_row_cnt < this->max_row_cnt);
			this->col_cnt_in_cur_row = 0;
		}
		// return true if can not append row next
		bool AppendRowDone(uint64_t rowid)
		{
			this->cur_row_cnt++;
			char * rowend = this->row_data_buf + this->cur_row_cnt * this->sz_per_row;
			*(uint64_t *)(rowend - sizeof(uint64_t)) = rowid;
			return this->cur_row_cnt >= this->max_row_cnt;
		}

		std::vector<uint64_t> Sort(const std::function<bool(uint64_t, uint64_t, const RowList *)> & rowcmp) const
		{
			std::vector<uint64_t> rowidxs;
			rowidxs.reserve(this->cur_row_cnt);
			for (uint64_t i = 0; i < this->cur_row_cnt; ++i)
				rowidxs[i] = i;

			std::sort(rowidxs.begin(), rowidxs.end(), [this, &rowcmp](uint64_t idx1, uint64_t idx2) {
				return rowcmp(idx1, idx2, this);
			});
			return rowidxs;
		}

		// return nullptr if col value is null
		uintptr_t * GetColValue(uint64_t rowidx, uint32_t colidx) const
		{
			assert(rowidx < this->cur_row_cnt && colidx < this->col_cnt);
			uintptr_t * p = (uintptr_t *)(this->row_data_buf + rowidx * this->sz_per_row + colidx * this->sz_per_col);
			if (this->no_null)
				return p;
			else
				return p[0] ? nullptr : (p + 1);
		}
		uint64_t GetRowId(uint64_t rowidx) const
		{
			assert(rowidx < this->cur_row_cnt);
			char * rowend = this->row_data_buf + (rowidx + 1) * this->sz_per_row;
			return *(uint64_t *)(rowend - sizeof(uint64_t));
		}
		uint64_t GetRowCnt() const { return this->cur_row_cnt; }
		uint32_t GetColCnt() const { return this->col_cnt; }

	protected:
		uintptr_t AppendOtherData(const char * data, size_t len)
		{
			auto eit = this->other_data_bufs.rbegin();
			if (this->other_data_bufs.empty() || (eit->size() + len > eit->capacity()))
			{
				std::string buf;
				buf.reserve(OTHER_DATA_BUF_SIZE);
				this->other_data_bufs.push_back(std::move(buf));
				eit = this->other_data_bufs.rbegin();
			}
			std::string & s = *eit;
			uintptr_t datum = (uintptr_t)(&s[0] + s.size());
			s.append(data, len);
			return datum;
		}
		uintptr_t * GetColAddr() const
		{
			return (uintptr_t *)(this->row_data_buf + this->cur_row_cnt * this->sz_per_row + this->col_cnt_in_cur_row * this->sz_per_col);
		}

	protected:
		const uint32_t col_cnt;
		const bool no_null;
		const uint64_t max_row_cnt;
		const uint32_t sz_per_col;
		const uint32_t sz_per_row;

		uint64_t cur_row_cnt;
		uint32_t col_cnt_in_cur_row;

		char * row_data_buf;

		std::vector<std::string> other_data_bufs;
		static const int OTHER_DATA_BUF_SIZE = 4 * 1024 * 1024;
	};
} // end of namespace rdb

#endif // end of ROW_LIST_H
