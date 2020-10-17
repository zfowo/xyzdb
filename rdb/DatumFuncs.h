
#ifndef DATUM_FUNCS_H
#define DATUM_FUNCS_H

#include <string>
#include "BaseTypes.h"
#include "MyAlloc.h"
#include "MyBitmap.h"

namespace rdb
{
	// 除了CTFloat/CTDouble，其他CTxxx类型都可以和Datum相互转换
	template <typename CT>
	Datum DatumFromCT(CT v)
	{
		return (Datum)v;
	}
	template <typename CT>
	CT DatumToCT(Datum d)
	{
		return (CT)d;
	}
	// specialization for CTFloat/CTDouble
	template <>
	Datum DatumFromCT(CTFloat v)
	{
		Datum d = 0;
		memcpy(&d, &v, sizeof(v));
		return d;
	}
	template <>
	CTFloat DatumToCT<CTFloat>(Datum d)
	{
		CTFloat v = 0;
		memcpy(&v, &d, sizeof(v));
		return v;
	}
	template <>
	Datum DatumFromCT(CTDouble v)
	{
		Datum d = 0;
		memcpy(&d, &v, sizeof(v));
		return d;
	}
	template <>
	CTDouble DatumToCT<CTDouble>(Datum d)
	{
		CTDouble v = 0;
		memcpy(&v, &d, sizeof(v));
		return v;
	}

} // end of namespace rdb

namespace rdb
{
	bool IsVLAToast(uint8_t flag)
	{
		return (flag & VLAFlagLenMask) == VLAToastFlag;
	}
	bool IsVLALen1(uint8_t flag)
	{
		return (flag & VLAFlagLenMask) == VLALen1Flag;
	}
	bool IsVLALen2(uint8_t flag)
	{
		return (flag & VLAFlagLenMask) == VLALen2Flag;
	}
	bool IsVLALen4(uint8_t flag)
	{
		return (flag & VLAFlagLenMask) == VLALen4Flag;
	}
	uint8_t MakeVLAFlag(size_t data_len, int * len_header_sz = nullptr);
	// 返回VLA中实际数据的长度，以及长度头的大小。
	std::pair<uint32_t, uint32_t> ParseVLAHeader(const Varlena * vla);
	uint32_t VLATotalLen(const Varlena * vla);
	// 获得VLA中实际数据的位置及其大小。
	std::pair<const char *, uint32_t> GetVLAData(const Varlena * vla);
	Varlena * MakeVLA(const char * data, size_t len);

	int varint_to_bytes(uint64_t n, char * buf);
	uint64_t varint_from_bytes(char * buf, int * cnt = nullptr);
	int varint_to_bytes(uint64_t n, mystring & s);

	// 返回月数和微妙数
	std::pair<int64_t, int64_t> ParseCTInterval(CTInterval v);
	CTInterval MakeCTInterval(int64_t month, int64_t micro);

	// 数组的格式：
	// .) uint32_t : 数组中的元素个数，最高位表示是否有NULL值。
	// .) NULL位图。
	// .) 所有非NULL的元素值。
	class CTArrayBuilder
	{
	private:
		ColTypeInfo array_cti;
		ColTypeInfo elem_cti;
		uint32_t elem_cnt;
		bool has_null;
		myvector<bool> nulls;
		mystring data;
	};
	class CTArrayReader
	{
	public:
		CTArrayReader(Varlena * vla_, const ColTypeInfo & arr_cti);
		~CTArrayReader();

		NO_COPY_MOVE(CTArrayReader);

		const ColTypeInfo & ArrayCTI() const { return this->array_cti; }
		const ColTypeInfo & ElemCTI() const { return this->elem_cti; }
		uint32_t ElemCount() const { return this->elem_cnt; }
		bool HasNull() const { return this->null_bmp; }
		MyBitmap * NullBmp() const { return this->null_bmp; }

		void Reset();
		bool Next(Datum & d, bool & isnull, bool copy = false);

	private:
		Varlena * vla;
		ColTypeInfo array_cti;
		ColTypeInfo elem_cti;

		uint32_t elem_cnt;
		MyBitmap * null_bmp;
		const char * elem_data;

		uint32_t next_idx;
		const char * next_elem_data;
	};

} // end of namespace rdb

namespace rdb
{
	using DatumToBytesFuncType = void(*)(Datum, const ColTypeInfo &, mystring &);
	using DatumFromBytesFuncType = Datum(*)(const char *&, const ColTypeInfo &, bool);
	using DatumCopyFuncType = Datum(*)(Datum, const ColTypeInfo &);
	using DatumFreeFuncType = void(*)(Datum, const ColTypeInfo &);
	using DatumSizeFuncType = uint32_t(*)(Datum, const ColTypeInfo &);

	void DatumToBytes(Datum d, const ColTypeInfo & cti, mystring & s);
	Datum DatumFromBytes(const char *& s, const ColTypeInfo & cti, bool copy = false);
	Datum DatumCopy(Datum d, const ColTypeInfo & cti);
	Datum DatumMove(Datum & d, const ColTypeInfo & cti);
	void DatumFree(Datum d, const ColTypeInfo & cti);
	uint32_t DatumSize(Datum d, const ColTypeInfo & cti);


	using DatumCompareFuncType = int(*)(Datum, Datum, const ColTypeInfo &);
	using DatumEqualFuncType = bool(*)(Datum, Datum, const ColTypeInfo &);
	using DatumHashFuncType = uint64_t(*)(Datum, const ColTypeInfo &);

	int DatumCompare(Datum d1, Datum d2, const ColTypeInfo & cti);
	bool DatumEqual(Datum d1, Datum d2, const ColTypeInfo & cti);
	uint64_t DatumHash(Datum d1, const ColTypeInfo & cti);


	using DatumToStringFuncType = void(*)(Datum, const ColTypeInfo &, mystring &);
	using DatumFromStringFuncType = Datum(*)(const char *, size_t, const ColTypeInfo &);

	void DatumToString(Datum d, const ColTypeInfo & cti, mystring & s);
	Datum DatumFromString(const char * s, size_t sz, const ColTypeInfo & cti);

} // end of namespace rdb

#endif // end of DATUM_FUNCS_H
