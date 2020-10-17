
#include "DatumFuncs.h"
#include "RdbException.h"
#include "ColTypeFuncs.h"
#include "MiscUtils.h"

namespace rdb
{
	template <typename FT>
	struct CTAndArrayFuncs
	{
		FT ct_func;
		FT ct_array_func;
	};
	static CTAndArrayFuncs<DatumToBytesFuncType> datum_to_bytes_ft[CT_MAX] = { {nullptr, nullptr} };
	static CTAndArrayFuncs<DatumFromBytesFuncType> datum_from_bytes_ft[CT_MAX] = { {nullptr, nullptr} };
	static CTAndArrayFuncs<DatumCopyFuncType> datum_copy_ft[CT_MAX] = { {nullptr, nullptr} };
	static CTAndArrayFuncs<DatumFreeFuncType> datum_free_ft[CT_MAX] = { {nullptr, nullptr} };
	static CTAndArrayFuncs<DatumSizeFuncType> datum_size_ft[CT_MAX] = { {nullptr, nullptr} };

	static CTAndArrayFuncs<DatumCompareFuncType> datum_compare_ft[CT_MAX] = { { nullptr, nullptr } };
	static CTAndArrayFuncs<DatumEqualFuncType> datum_equal_ft[CT_MAX] = { { nullptr, nullptr } };
	static CTAndArrayFuncs<DatumHashFuncType> datum_hash_ft[CT_MAX] = { { nullptr, nullptr } };

	static CTAndArrayFuncs<DatumToStringFuncType> datum_to_string_ft[CT_MAX] = { {nullptr, nullptr} };
	static CTAndArrayFuncs<DatumFromStringFuncType> datum_from_string_ft[CT_MAX] = { {nullptr, nullptr} };

	struct DatumToBytesFuncRegister
	{
		DatumToBytesFuncRegister(ColType ct, DatumToBytesFuncType f, DatumToBytesFuncType arrf)
		{
			datum_to_bytes_ft[(int)ct].ct_func = f;
			datum_to_bytes_ft[(int)ct].ct_array_func = arrf;
		}
	};
	struct DatumFromBytesFuncRegister
	{
		DatumFromBytesFuncRegister(ColType ct, DatumFromBytesFuncType f, DatumFromBytesFuncType arrf)
		{
			datum_from_bytes_ft[(int)ct].ct_func = f;
			datum_from_bytes_ft[(int)ct].ct_array_func = arrf;
		}
	};
	struct DatumCopyFuncRegister
	{
		DatumCopyFuncRegister(ColType ct, DatumCopyFuncType f, DatumCopyFuncType arrf)
		{
			datum_copy_ft[(int)ct].ct_func = f;
			datum_copy_ft[(int)ct].ct_array_func = arrf;
		}
	};
	struct DatumFreeFuncRegister
	{
		DatumFreeFuncRegister(ColType ct, DatumFreeFuncType f, DatumFreeFuncType arrf)
		{
			datum_free_ft[(int)ct].ct_func = f;
			datum_free_ft[(int)ct].ct_array_func = arrf;
		}
	};
	struct DatumSizeFuncRegister
	{
		DatumSizeFuncRegister(ColType ct, DatumSizeFuncType f, DatumSizeFuncType arrf)
		{
			datum_size_ft[(int)ct].ct_func = f;
			datum_size_ft[(int)ct].ct_array_func = arrf;
		}
	};
#define RegToBytesFunc(ct, f, arrf)   DatumToBytesFuncRegister   ct##_tobytes_reg(ct, f, arrf)
#define RegFromBytesFunc(ct, f, arrf) DatumFromBytesFuncRegister ct##_frombytes_reg(ct, f, arrf)
#define RegCopyFunc(ct, f, arrf)      DatumCopyFuncRegister      ct##_copy_reg(ct, f, arrf)
#define RegFreeFunc(ct, f, arrf)      DatumFreeFuncRegister      ct##_free_reg(ct, f, arrf)
#define RegSizeFunc(ct, f, arrf)      DatumSizeFuncRegister      ct##_size_reg(ct, f, arrf)

	struct DatumCompareFuncRegister
	{
		DatumCompareFuncRegister(ColType ct, DatumCompareFuncType f, DatumCompareFuncType arrf)
		{
			datum_compare_ft[(int)ct].ct_func = f;
			datum_compare_ft[(int)ct].ct_array_func = arrf;
		}
	};
	struct DatumEqualFuncRegister
	{
		DatumEqualFuncRegister(ColType ct, DatumEqualFuncType f, DatumEqualFuncType arrf)
		{
			datum_equal_ft[(int)ct].ct_func = f;
			datum_equal_ft[(int)ct].ct_array_func = arrf;
		}
	};
	struct DatumHashFuncRegister
	{
		DatumHashFuncRegister(ColType ct, DatumHashFuncType f, DatumHashFuncType arrf)
		{
			datum_hash_ft[(int)ct].ct_func = f;
			datum_hash_ft[(int)ct].ct_array_func = arrf;
		}
	};
#define RegCompareFunc(ct, f, arrf) DatumCompareFuncRegister ct##_compare_reg(ct, f, arrf)
#define RegEqualFunc(ct, f, arrf)   DatumEqualFuncRegister ct##_equal_reg(ct, f, arrf)
#define RegHashFunc(ct, f, arrf)    DatumHashFuncRegister ct##_hash_reg(ct,f, arrf)

	struct DatumToStringFuncRegister
	{
		DatumToStringFuncRegister(ColType ct, DatumToStringFuncType f, DatumToStringFuncType arrf)
		{
			datum_to_string_ft[(int)ct].ct_func = f;
			datum_to_string_ft[(int)ct].ct_array_func = arrf;
		}
	};
	struct DatumFromStringFuncRegister
	{
		DatumFromStringFuncRegister(ColType ct, DatumFromStringFuncType f, DatumFromStringFuncType arrf)
		{
			datum_from_string_ft[(int)ct].ct_func = f;
			datum_from_string_ft[(int)ct].ct_array_func = arrf;
		}
	};
#define RegToStringFunc(ct, f, arrf)   DatumToStringRegister ct##_tostring_reg(ct, f, arrf)
#define RegFromStringFunc(ct, f, arrf) DatumFromStringRegister ct##_fromstring_reg(ct, f, arrf)

} // end of namespace rdb

namespace rdb
{
	// 返回VLA中实际数据的长度，以及长度头的大小。
	std::pair<uint32_t, uint32_t> ParseVLAHeader(const Varlena * vla)
	{
		uint8_t lenflag = (uint8_t)(vla->flag & VLAFlagLenMask);
		if (lenflag == VLALen1Flag)
			return std::make_pair((uint32_t)*(uint8_t*)vla->data, (uint32_t)1);
		else if (lenflag == VLALen2Flag)
			return std::make_pair((uint32_t)*(uint16_t *)vla->data, (uint32_t)2);
		else if (lenflag == VLALen4Flag)
			return std::make_pair(*(uint32_t *)vla->data, (uint32_t)4);
		throw RdbException("unknown lenflag:" + std::to_string(lenflag));
	}
	uint32_t VLATotalLen(const Varlena * vla)
	{
		auto h = ParseVLAHeader(vla);
		return VLAFlagSize + h.second + h.first;
	}
	std::pair<const char *, uint32_t> GetVLAData(const Varlena * vla)
	{
		auto h = ParseVLAHeader(vla);
		return std::make_pair(vla->data + h.second, h.first);
	}
	uint8_t MakeVLAFlag(size_t data_len, int * len_header_sz)
	{
		uint8_t flag = 0;
		int sz = 0;
		if (data_len <= 0xFF)
		{
			sz = 1;
			flag |= VLALen1Flag;
		}
		else if (data_len <= 0xFFFF)
		{
			sz = 2;
			flag |= VLALen2Flag;
		}
		else if (data_len <= 0xFFFFFFFF)
		{
			sz = 4;
			flag |= VLALen4Flag;
		}
		else
		{
			throw RdbException("data len is out of VLA limit:" + std::to_string(data_len));
		}
		if (len_header_sz)
			*len_header_sz = sz;
		return flag;
	}
	Varlena * MakeVLA(const char * data, size_t len)
	{
		int len_header_sz = 0;
		uint8_t flag = MakeVLAFlag(len, &len_header_sz);
		size_t vla_len = VLAFlagSize + len_header_sz + len;
		Varlena * vla = (Varlena *)myalloc(vla_len);
		if (!vla)
			throw RdbBadAllocException(vla_len);
		MyPtrHolder<Varlena, myfreer> vlaholder(vla);

		vla->flag = flag;
		if (IsVLALen1(flag))
			*(uint8_t *)vla->data = (uint8_t)len;
		else if (IsVLALen2(flag))
			*(uint16_t *)vla->data = (uint16_t)len;
		else if (IsVLALen4(flag))
			*(uint32_t *)vla->data = (uint32_t)len;
		memcpy(vla->data + len_header_sz, data, len);

		return vlaholder.Release();
	}

	int varint_to_bytes(uint64_t n, char * buf)
	{
		unsigned char * pc = (unsigned char *)buf;
		while (true)
		{
			unsigned char c = n & 0x7F;
			n = n >> 7;
			if (n == 0)
			{
				*pc++ = c;
				return (int)((char *)pc - buf);
			}
			c = c | 0x80;
			*pc++ = c;
		}
		assert(false);
		return 0;
	}
	uint64_t varint_from_bytes(const char * buf, int * cnt = nullptr)
	{
		unsigned char * pc = (unsigned char *)buf;
		uint64_t v = 0, c = 0;
		// 先检查第一个字节
		c = *pc++;
		if (!(c & 0x80))
		{
			if (cnt)
				*cnt = 1;
			return c;
		}
		// 第一个字节的最高位为1
		v = c & 0x7F;
		int shift = 7;
		while (true)
		{
			c = *pc++;
			v |= (c & 0x7F) << shift;
			if ((c & 0x80) == 0)
				break;
			shift += 7;
		}
		if (cnt)
			*cnt = (int)((char *)pc - buf);
		return v;
	}
	int varint_to_bytes(uint64_t n, mystring & s)
	{
		char buf[16];
		int bytecnt = varint_to_bytes(n, buf);
		s.append(buf, bytecnt);
		return bytecnt;
	}

	std::pair<int64_t, int64_t> ParseCTInterval(CTInterval v)
	{
		uint64_t month = (v & IntervalMonthMask) >> IntervalMicroBitNum;
		uint64_t micro = v & IntervalMicroMask;
		bool month_isneg = month & 0x01;
		bool micro_isneg = micro & 0x01;
		month = month >> 1;
		micro = micro >> 1;
		
		int64_t month2 = (int64_t)month;
		if (month_isneg)
			month2 = -month2;
		int64_t micro2 = (int64_t)micro;
		if (micro_isneg)
			micro2 = -micro2;

		return std::make_pair(month2, micro2);
	}
	CTInterval MakeCTInterval(int64_t month, int64_t micro)
	{
		if (month > IntervalMaxMonth || month < -IntervalMaxMonth)
			throw RdbException("interval month is out of range: " + std::to_string(month));
		if (micro > IntervalMaxMicro || micro < -IntervalMaxMicro)
			throw RdbException("interval micro is out of range: " + std::to_string(micro));
		bool month_isneg = month < 0;
		bool micro_isneg = micro < 0;
		if (month_isneg)
			month = -month;
		if (micro_isneg)
			micro = -micro;
		month = month << 1;
		micro = micro << 1;
		if (month_isneg)
			month = month | 0x01;
		if (micro_isneg)
			micro = micro | 0x01;
		return ((uint64_t)month << IntervalMicroBitNum) | (uint64_t)micro;
	}

	// class CTArrayReader
	CTArrayReader::CTArrayReader(Varlena * vla_, const ColTypeInfo & arr_cti)
		: vla(vla_), array_cti(arr_cti), elem_cti(arr_cti)
	{
		assert(vla_);
		assert(IsArrayColType(arr_cti.ct));
		this->elem_cti.ct = GetArrayElementColType(arr_cti.ct);

		auto x = GetVLAData(this->vla);
		this->elem_cnt = *(uint32_t *)x.first;
		if (this->elem_cnt & 0x80000000) // has NULL
		{
			this->elem_cnt = this->elem_cnt & 0x7FFFFFFF;
			this->null_bmp = new MyBitmap((int)this->elem_cnt, (uint8_t *)x.first + sizeof(uint32_t), false);
			this->elem_data = x.first + sizeof(uint32_t) + this->null_bmp->ByteCount();
		}
		else
		{
			this->null_bmp = nullptr;
			this->elem_data = x.first + sizeof(uint32_t);
		}

		this->Reset();
	}
	CTArrayReader::~CTArrayReader()
	{
		if (this->null_bmp)
			delete this->null_bmp;
	}
	void CTArrayReader::Reset()
	{
		this->next_idx = 0;
		this->next_elem_data = this->elem_data;
	}
	bool CTArrayReader::Next(Datum & d, bool & isnull, bool copy)
	{
		if (this->next_idx >= this->elem_cnt)
			return false;

		if (this->null_bmp && this->null_bmp->IsSet((int)this->next_idx))
		{
			d = 0;
			isnull = true;
		}
		else
		{
			d = DatumFromBytes(this->next_elem_data, this->elem_cti, copy);
			isnull = false;
		}
		this->next_idx++;
		return true;
	}

} // end of namespace rdb

// CTToBytes
namespace rdb
{
	template <typename CT>
	void CTToBytes(Datum d, const ColTypeInfo &, mystring & s)
	{
		CT v = DatumToCT<CT>(d);
		s.append((const char *)&v, sizeof(v));
	}
	// 这里不能写成CTToBytes对CTVarint的特化，否则CTToBytes<CTTime64>也会调用它。
	void CTVarintToBytes(Datum d, const ColTypeInfo &, mystring & s)
	{
		CTVarint v = DatumToCT<CTVarint>(d);
		varint_to_bytes(v, s);
	}
	template <>
	void CTToBytes<CTDeciaml128>(Datum d, const ColTypeInfo &, mystring & s)
	{
		CTDecimal128 v = DatumToCT<CTDeciaml128>(d);
		s.append((const char *)v, sizeof(*v));
	}
	template <>
	void CTToBytes<Varlena *>(Datum d, const ColTypeInfo &, mystring & s)
	{
		Varlena * vla = DatumToCT<Varlena *>(d);
		auto h = ParseVLAHeader(vla);
		s.append((const char *)vla, VLAFlagSize + h.second + h.first);
	}

	RegToBytesFunc(CT_INT8, CTToBytes<CTInt8>, CTToBytes<CTArray>);
	RegToBytesFunc(CT_INT16, CTToBytes<CTInt16>, CTToBytes<CTArray>);
	RegToBytesFunc(CT_INT32, CTToBytes<CTInt32>, CTToBytes<CTArray>);
	RegToBytesFunc(CT_INT64, CTToBytes<CTInt64>, CTToBytes<CTArray>);
	RegToBytesFunc(CT_VARINT, CTVarintToBytes, CTToBytes<CTArray>);

	RegToBytesFunc(CT_FLOAT, CTToBytes<CTFloat>, CTToBytes<CTArray>);
	RegToBytesFunc(CT_DOUBLE, CTToBytes<CTDouble>, CTToBytes<CTArray>);

	RegToBytesFunc(CT_DECIMAL64, CTToBytes<CTDecimal64>, CTToBytes<CTArray>);
	RegToBytesFunc(CT_DECIMAL128, CTToBytes<CTDecimal128>, CTToBytes<CTArray>);

	RegToBytesFunc(CT_TEXT, CTToBytes<CTText>, CTToBytes<CTArray>);
	RegToBytesFunc(CT_BYTES, CTToBytes<CTBytes>, CTToBytes<CTArray>);

	RegToBytesFunc(CT_DATE, CTToBytes<CTDate>, CTToBytes<CTArray>);
	RegToBytesFunc(CT_TIME32, CTToBytes<CTTime32>, CTToBytes<CTArray>);
	RegToBytesFunc(CT_TIME64, CTToBytes<CTTime64>, CTToBytes<CTArray>);
	RegToBytesFunc(CT_TIMESTAMP, CTToBytes<CTTimeStamp>, CTToBytes<CTArray>);
	RegToBytesFunc(CT_TIMETZ32, CTToBytes<CTTimeTz32>, CTToBytes<CTArray>);
	RegToBytesFunc(CT_TIMETZ64, CTToBytes<CTTimeTz64>, CTToBytes<CTArray>);
	RegToBytesFunc(CT_TIMESTAMPTZ, CTToBytes<CTTimeStampTz>, CTToBytes<CTArray>);
	RegToBytesFunc(CT_INTERVAL, CTToBytes<CTInterval>, CTToBytes<CTArray>);

	RegToBytesFunc(CT_BLOB, CTToBytes<CTBlob>, CTToBytes<CTArray>);

	RegToBytesFunc(CT_GEOMETRY, CTToBytes<CTGeometry>, CTToBytes<CTArray>);
	RegToBytesFunc(CT_GEOGRAPHY, CTToBytes<CTGeography>, CTToBytes<CTArray>);

	RegToBytesFunc(CT_JSON, CTToBytes<CTJson>, CTToBytes<CTArray>);

	void DatumToBytes(Datum d, const ColTypeInfo & cti, mystring & s)
	{
		assert(cti.ct > 0);
		ColType ct = cti.ct;
		bool isarray = IsArrayColType(ct);
		if (isarray)
			ct = GetArrayElementColType(ct);

		auto & ff = datum_to_bytes_ft[(int)ct];
		if (!ff.ct_func || !ff.ct_array_func)
			throw RdbException("DatumToBytesFunc for col type is null. coltype:" + ColTypeName(ct));
		if (isarray)
			ff.ct_array_func(d, cti, s);
		else
			ff.ct_func(d, cti, s);
	}

} // end of namespace rdb

// CTFromBytes
namespace rdb
{
	template <typename CT>
	Datum CTFromBytes(const char *& s, const ColTypeInfo &, bool)
	{
		CT v = *(CT *)s;
		s += sizeof(v);
		return DatumFromCT<CT>(v);
	}
	Datum CTVarintFromBytes(const char *& s, const ColTypeInfo &, bool)
	{
		int cnt = 0;
		CTVarint v = varint_from_bytes(s, &cnt);
		s += cnt;
		return DatumFromCT<CTVarint>(v);
	}
	template <>
	Datum CTFromBytes<CTDecimal128>(const char *& s, const ColTypeInfo &, bool copy)
	{
		CTDecimal128 v = (CTDecimal128)s;
		if (copy)
		{
			v = (CTDecimal128)myalloc(sizeof(*v));
			if (!v)
				throw RdbBadAllocException(sizeof(*v));
			memcpy(v, s, sizeof(*v));
		}
		s += sizeof(*v);
		return DatumFromCT<CTDecimal128>(v);
	}
	template <>
	Datum CTFromBytes<Varlena *>(const char *& s, const ColTypeInfo &, bool copy)
	{
		Varlena * v = (Varlena *)s;
		uint32_t len = VLATotalLen(v);
		if (copy)
		{
			v = (Varlena *)myalloc(len);
			if (!v)
				throw RdbBadAllocException(len);
			memcpy(v, s, len);
		}
		s += len;
		return DatumFromCT<Varlena *>(v);
	}

	RegFromBytesFunc(CT_INT8, CTFromBytes<CTInt8>, CTFromBytes<CTArray>);
	RegFromBytesFunc(CT_INT16, CTFromBytes<CTInt16>, CTFromBytes<CTArray>);
	RegFromBytesFunc(CT_INT32, CTFromBytes<CTInt32>, CTFromBytes<CTArray>);
	RegFromBytesFunc(CT_INT64, CTFromBytes<CTInt64>, CTFromBytes<CTArray>);
	RegFromBytesFunc(CT_VARINT, CTVarintFromBytes, CTFromBytes<CTArray>);

	RegFromBytesFunc(CT_FLOAT, CTFromBytes<CTFloat>, CTFromBytes<CTArray>);
	RegFromBytesFunc(CT_DOUBLE, CTFromBytes<CTDouble>, CTFromBytes<CTArray>);

	RegFromBytesFunc(CT_DECIMAL64, CTFromBytes<CTDecimal64>, CTFromBytes<CTArray>);
	RegFromBytesFunc(CT_DECIMAL128, CTFromBytes<CTDecimal128>, CTFromBytes<CTArray>);

	RegFromBytesFunc(CT_TEXT, CTFromBytes<CTText>, CTFromBytes<CTArray>);
	RegFromBytesFunc(CT_BYTES, CTFromBytes<CTBytes>, CTFromBytes<CTArray>);

	RegFromBytesFunc(CTDATE, CTFromBytes<CTDate>, CTFromBytes<CTArray>);
	RegFromBytesFunc(CT_TIME32, CTFromBytes<CTTime32>, CTFromBytes<CTArray>);
	RegFromBytesFunc(CT_TIME64, CTFromBytes<CTTime64>, CTFromBytes<CTArray>);
	RegFromBytesFunc(CT_TIMESTAMP, CTFromBytes<CTTimeStamp>, CTFromBytes<CTArray>);
	RegFromBytesFunc(CT_TIMETZ32, CTFromBytes<CTTimeTz32>, CTFromBytes<CTArray>);
	RegFromBytesFunc(CT_TIMETZ64, CTFromBytes<CTTimeTz64>, CTFromBytes<CTArray>);
	RegFromBytesFunc(CT_TIMESTAMPTZ, CTFromBytes<CTTimeStampTz>, CTFromBytes<CTArray>);
	RegFromBytesFunc(CT_INTERVAL, CTFromBytes<CTInterval>, CTFromBytes<CTArray>);

	RegFromBytesFunc(CT_BLOB, CTFromBytes<CTBlob>, CTFromBytes<CTArray>);

	RegFromBytesFunc(CT_GEOMETRY, CTFromBytes<CTGeometry>, CTFromBytes<CTArray>);
	RegFromBytesFunc(CT_GEOGRAPHY, CTFromBytes<CTGeography>, CTFromBytes<CTArray>);

	RegFromBytesFunc(CT_JSON, CTFromBytes<CTJson>, CTFromBytes<CTArray>);

	Datum DatumFromBytes(const char *& s, const ColTypeInfo & cti, bool copy)
	{
		assert(cti.ct > 0);
		ColType ct = cti.ct;
		bool isarray = IsArrayColType(ct);
		if (isarray)
			ct = GetArrayElementColType(ct);

		auto & ff = datum_from_bytes_ft[(int)ct];
		if (!ff.ct_func || !ff.ct_array_func)
			throw RdbException("DatumFromBytesFunc for col type is null. coltype:" + ColTypeName(ct));
		if (isarray)
			return ff.ct_array_func(s, cti, copy);
		else
			return ff.ct_func(s, cti, copy);
	}

} // end of namespace rdb

// CTCopy & CTMove & CTFree & CTSize
namespace rdb
{
	template <typename CT>
	Datum CTCopy(Datum d, const ColTypeInfo &)
	{
		return d;
	}
	template <>
	Datum CTCopy<CTDecimal128>(Datum d, const ColTypeInfo &)
	{
		CTDecimal128 v = DatumToCT<CTDecimal128>(d);
		CTDecimal128 v2 = (CTDecimal128)myalloc(sizeof(*v));
		if (!v2)
			throw RdbBadAllocException(sizeof(*v));
		memcpy(v2, v, sizeof(*v));
		return DatumFromCT<CTDecimal128>(v2);
	}
	template <>
	Datum CTCopy<Varlena *>(Datum d, const ColTypeInfo &)
	{
		Varlena * v = DatumToCT<Varlena *>(d);
		uint32_t sz = VLATotalLen(v);
		Varlena * v2 = (Varlena *)myalloc(sz);
		if (!v2)
			throw RdbBadAllocException(sz);
		memcpy(v2, v, sz);
		return DatumFromCT<Varlena *>(v2);
	}

	RegCopyFunc(CT_INT8, CTCopy<CTInt8>, CTCopy<CTArray>);
	RegCopyFunc(CT_INT16, CTCopy<CTInt16>, CTCopy<CTArray>);
	RegCopyFunc(CT_INT32, CTCopy<CTInt32>, CTCopy<CTArray>);
	RegCopyFunc(CT_INT64, CTCopy<CTInt64>, CTCopy<CTArray>);
	RegCopyFunc(CT_VARINT, CTCopy<CTVarint>, CTCopy<CTArray>);

	RegCopyFunc(CT_FLOAT, CTCopy<CTFloat>, CTCopy<CTArray>);
	RegCopyFunc(CT_DOUBLE, CTCopy<CTDouble>, CTCopy<CTArray>);

	RegCopyFunc(CT_DECIMAL64, CTCopy<CTDecimal64>, CTCopy<CTArray>);
	RegCopyFunc(CT_DECIMAL128, CTCopy<CTDecimal128>, CTCopy<CTArray>);

	RegCopyFunc(CT_TEXT, CTCopy<CTText>, CTCopy<CTArray>);
	RegCopyFunc(CT_BYTES, CTCopy<CTBytes>, CTCopy<CTArray>);

	RegCopyFunc(CT_DATE, CTCopy<CTDate>, CTCopy<CTArray>);
	RegCopyFunc(CT_TIME32, CTCopy<CTTime32>, CTCopy<CTArray>);
	RegCopyFunc(CT_TIME64, CTCopy<CTTime64>, CTCopy<CTArray>);
	RegCopyFunc(CT_TIMESTAMP, CTCopy<CTTimeStamp>, CTCopy<CTArray>);
	RegCopyFunc(CT_TIMETZ32, CTCopy<CTTimeTz32>, CTCopy<CTArray>);
	RegCopyFunc(CT_TIMETZ64, CTCopy<CTTimeTz64>, CTCopy<CTArray>);
	RegCopyFunc(CT_TIMESTAMPTZ, CTCopy<CTTimeStampTz>, CTCopy<CTArray>);
	RegCopyFunc(CT_INTERVAL, CTCopy<CTInterval>, CTCopy<CTArray>);

	RegCopyFunc(CT_BLOB, CTCopy<CTBlob>, CTCopy<CTArray>);

	RegCopyFunc(CT_GEOMETRY, CTCopy<CTGeometry>, CTCopy<CTArray>);
	RegCopyFunc(CT_GEOGRAPHY, CTCopy<CTGeography>, CTCopy<CTArray>);

	RegCopyFunc(CT_JSON, CTCopy<CTJson>, CTCopy<CTArray>);

	Datum DatumCopy(Datum d, const ColTypeInfo & cti)
	{
		assert(cti.ct > 0);
		ColType ct = cti.ct;
		bool isarray = IsArrayColType(ct);
		if (isarray)
			ct = GetArrayElementColType(ct);

		auto & ff = datum_copy_ft[(int)ct];
		if (!ff.ct_func || !ff.ct_array_func)
			throw RdbException("DatumCopyFunc for col type is null. coltype:" + ColTypeName(ct));
		if (isarray)
			return ff.ct_array_func(d, cti);
		else
			return ff.ct_func(d, cti);
	}

	Datum DatumMove(Datum & d, const ColTypeInfo &)
	{
		Datum d2 = d;
		d = (Datum)nullptr;
		return d2;
	}

	template <typename CT>
	void CTFree(Datum d, const ColTypeInfo &)
	{
		return;
	}
	template <>
	void CTFree<CTDecimal128>(Datum d, const ColTypeInfo &)
	{
		CTDecimal128 v = DatumToCT<CTDecimal128>(d);
		myfree(v);
	}
	template <>
	void CTFree<Varlena *>(Datum d, const ColTypeInfo &)
	{
		Varlena * v = DatumToCT<Varlena *>(d);
		myfree(v);
	}

	RegFreeFunc(CT_INT8, CTFree<CTInt8>, CTFree<CTArray>);
	RegFreeFunc(CT_INT16, CTFree<CTInt16>, CTFree<CTArray>);
	RegFreeFunc(CT_INT32, CTFree<CTInt32>, CTFree<CTArray>);
	RegFreeFunc(CT_INT64, CTFree<CTInt64>, CTFree<CTArray>);
	RegFreeFunc(CT_VARINT, CTFree<CTVarint>, CTFree<CTArray>);

	RegFreeFunc(CT_FLOAT, CTFree<CTFloat>, CTFree<CTArray>);
	RegFreeFunc(CT_DOUBLE, CTFree<CTDouble>, CTFree<CTArray>);

	RegFreeFunc(CT_DECIMAL64, CTFree<CTDecimal64>, CTFree<CTArray>);
	RegFreeFunc(CT_DECIMAL128, CTFree<CTDecimal128>, CTFree<CTArray>);

	RegFreeFunc(CT_TEXT, CTFree<CTText>, CTFree<CTArray>);
	RegFreeFunc(CT_BYTES, CTFree<CTBytes>, CTFree<CTArray>);

	RegFreeFunc(CT_DATE, CTFree<CTDate>, CTFree<CTArray>);
	RegFreeFunc(CT_TIME32, CTFree<CTTime32>, CTFree<CTArray>);
	RegFreeFunc(CT_TIME64, CTFree<CTTime64>, CTFree<CTArray>);
	RegFreeFunc(CT_TIMESTAMP, CTFree<CTTimeStamp>, CTFree<CTArray>);
	RegFreeFunc(CT_TIMETZ32, CTFree<CTTimeTz32>, CTFree<CTArray>);
	RegFreeFunc(CT_TIMETZ64, CTFree<CTTimeTz64>, CTFree<CTArray>);
	RegFreeFunc(CT_TIMESTAMPTZ, CTFree<CTTimeStampTz>, CTFree<CTArray>);
	RegFreeFunc(CT_INTERVAL, CTFree<CTInterval>, CTFree<CTArray>);

	RegFreeFunc(CT_BLOB, CTFree<CTBlob>, CTFree<CTArray>);

	RegFreeFunc(CT_GEOMETRY, CTFree<CTGeometry>, CTFree<CTArray>);
	RegFreeFunc(CT_GEOGRAPHY, CTFree<CTGeography>, CTFree<CTArray>);

	RegFreeFunc(CT_JSON, CTFree<CTJson>, CTFree<CTArray>);

	void DatumFree(Datum d, const ColTypeInfo & cti)
	{
		assert(cti.ct > 0);
		ColType ct = cti.ct;
		bool isarray = IsArrayColType(ct);
		if (isarray)
			ct = GetArrayElementColType(ct);

		auto & ff = datum_free_ft[(int)ct];
		if (!ff.ct_func || !ff.ct_array_func)
			throw RdbException("DatumFreeFunc for col type is null. coltype:" + ColTypeName(ct));
		if (isarray)
			ff.ct_array_func(d, cti);
		else
			ff.ct_func(d, cti);
	}

	template <typename CT>
	uint32_t CTSize(Datum, const ColTypeInfo &)
	{
		return sizeof(CT);
	}
	uint32_t CTVarintSize(Datum d, const ColTypeInfo &)
	{
		CTVarint v = DatumToCT<CTVarint>(d);
		char buf[16];
		return (uint32_t)varint_to_bytes(v, buf);
	}
	template <>
	uint32_t CTSize<CTDecimal128>(Datum d, const ColTypeInfo &)
	{
		CTDecimal128 v = DatumToCT<CTDecimal128>(d);
		return (uint32_t)sizeof(*v);
	}
	template <>
	uint32_t CTSize<Varlena *>(Datum d, const ColTypeInfo &)
	{
		Varlena * v = DatumToCT<Varlena *>(d);
		return VLATotalLen(v);
	}

	RegSizeFunc(CT_INT8, CTSize<CTInt8>, CTSize<CTArray>);
	RegSizeFunc(CT_INT16, CTSize<CTInt16>, CTSize<CTArray>);
	RegSizeFunc(CT_INT32, CTSize<CTInt32>, CTSize<CTArray>);
	RegSizeFunc(CT_INT64, CTSize<CTInt64>, CTSize<CTArray>);
	RegSizeFunc(CT_VARINT, CTVarintSize, CTSize<CTArray>);

	RegSizeFunc(CT_FLOAT, CTSize<CTFloat>, CTSize<CTArray>);
	RegSizeFunc(CT_DOUBLE, CTSize<CTDouble>, CTSize<CTArray>);

	RegSizeFunc(CT_DECIMAL64, CTSize<CTDecimal64>, CTSize<CTArray>);
	RegSizeFunc(CT_DECIMAL128, CTSize<CTDecimal128>, CTSize<CTArray>);

	RegSizeFunc(CT_TEXT, CTSize<CTText>, CTSize<CTArray>);
	RegSizeFunc(CT_BYTES, CTSize<CTBytes>, CTSize<CTArray>);

	RegSizeFunc(CT_DATE, CTSize<CTDate>, CTSize<CTArray>);
	RegSizeFunc(CT_TIME32, CTSize<CTTime32>, CTSize<CTArray>);
	RegSizeFunc(CT_TIME64, CTSize<CTTime64>, CTSize<CTArray>);
	RegSizeFunc(CT_TIMESTAMP, CTSize<CTTimeStamp>, CTSize<CTArray>);
	RegSizeFunc(CT_TIMETZ32, CTSize<CTTimeTz32>, CTSize<CTArray>);
	RegSizeFunc(CT_TIMETZ64, CTSize<CTTimeTz64>, CTSize<CTArray>);
	RegSizeFunc(CT_TIMESTAMPTZ, CTSize<CTTimeStampTz>, CTSize<CTArray>);
	RegSizeFunc(CT_INTERVAL, CTSize<CTInterval>, CTSize<CTArray>);

	RegSizeFunc(CT_BLOB, CTSize<CTBlob>, CTSize<CTArray>);

	RegSizeFunc(CT_GEOMETRY, CTSize<CTGeometry>, CTSize<CTArray>);
	RegSizeFunc(CT_GEOGRAPHY, CTSize<CTGeography>, CTSize<CTArray>);

	RegSizeFunc(CT_JSON, CTSize<CTJson>, CTSize<CTArray>);

	uint32_t DatumSize(Datum d, const ColTypeInfo & cti)
	{
		assert(cti.ct > 0);
		ColType ct = cti.ct;
		bool isarray = IsArrayColType(ct);
		if (isarray)
			ct = GetArrayElementColType(ct);

		auto & ff = datum_size_ft[(int)ct];
		if (!ff.ct_func || !ff.ct_array_func)
			throw RdbException("DatumSizeFunc for col type is null. coltype:" + ColTypeName(ct));
		if (isarray)
			return ff.ct_array_func(d, cti);
		else
			return ff.ct_func(d, cti);
	}

} // end of namespace rdb

// CTCompare
namespace rdb
{
	template <typename CT>
	int CTCompare(Datum d1, Datum d2, const ColTypeInfo &)
	{
		CT v1 = DatumToCT<CT>(d1);
		CT v2 = DatumToCT<CT>(d2);
		return (v1 == v2) ? 0 : (v1 < v2 ? -1 : 1);
	}
	template <>
	int CTCompare<CTDecimal128>(Datum d1, Datum d2, const ColTypeInfo &)
	{
		CTDecimal128 v1 = DatumToCT<CTDecimal128>(d1);
		CTDecimal128 v2 = DatumToCT<CTDecimal128>(d2);
#if defined(HAVE_MYINT128)
		myint128 n1 = *(myint128 *)v1->data;
		myint128 n2 = *(myint128 *)v2->data;
		BigEndianToLocal(n1);
		BigEndianToLocal(n2);
		return (n1 == n2) ? 0 : (n1 < n2 ? -1 : 1);
#else
		bool isneg1 = v1->data[0] & 0x80;
		bool isneg2 = v2->data[0] & 0x80;
		if (isneg1 != isneg2)
			return isneg1 ? -1 : 1;

		CTDecimal128Data dd1 = *v1, dd2 = *v2;
		uint64_t * p1 = (uint64_t *)dd1.data;
		uint64_t * p2 = (uint64_t *)dd2.data;
		if (isneg1)
		{
			// 这里无须求反后再加1
			*p1 = ~(*p1);
			*(p1 + 1) = ~(*(p1 + 1));
			*p2 = ~(*p2);
			*(p2 + 1) = ~(*(p2 + 1));
		}
		int res = 0;
		if (*p1 != *p2)
		{
			res = (*p1 < *p2) ? -1 : 1;
		}
		else
		{
			p1++; p2++;
			res = (*p1 == *p2) ? 0 : ((*p1 < *p2) ? -1 : 1);
		}
		if (isneg1)
			res = -res;
		return res;
#endif
	}
	int CTTextCompare(Datum d1, Datum d2, const ColTypeInfo &)
	{

	}
	template <>
	int CTCompare<Varlena *>(Datum d1, Datum d2, const ColTypeInfo &)
	{
		Varlena * v1 = DatumToCT<Varlena *>(d1);
		Varlena * v2 = DatumToCT<Varlena *>(d2);
		auto x1 = GetVLAData(v1);
		auto x2 = GetVLAData(v2);
		uint32_t sz = (x1.second < x2.second) ? x1.second : x2.second;
		int diff = memcmp(x1.first, x2.first, sz);
		if (diff != 0)
			return diff;
		return (x1.second == x2.second) ? 0 : ((x1.second < x2.second) ? -1 : 1);
	}
	int CTIntervalCompare(Datum d1, Datum d2, const ColTypeInfo &)
	{
		CTInterval v1 = DatumToCT<CTInterval>(d1);
		CTInterval v2 = DatumToCT<CTInterval>(d2);
		auto x1 = ParseCTInterval(v1);
		auto x2 = ParseCTInterval(v2);
		// 这里可以直接用减，因为不会溢出
		int64_t month = x1.first - x2.first;
		int64_t micro = x1.second - x2.second;
		// 每个月按30天计算
		int64_t diff = month * 30 * MicroPerDay + micro;
		return (diff == 0) ? 0 : ((diff < 0) ? -1 : 1);
	}
	int CTArrayCompare(Datum d1, Datum d2, const ColTypeInfo & cti)
	{
		CTArray v1 = DatumToCT<CTArray>(d1);
		CTArray v2 = DatumToCT<CTArray>(d2);
		CTArrayReader ar1(v1, cti);
		CTArrayReader ar2(v2, cti);
		Datum elemd1 = 0, elemd2 = 0;
		bool isnull1 = false, isnull2 = false;
		while (ar1.Next(elemd1, isnull1) && ar2.Next(elemd2, isnull2))
		{
			if (isnull1 && isnull2)
				continue;
			else if (isnull1)
				return -1;
			else if (isnull2)
				return 1;

			int ret = DatumCompare(elemd1, elemd2, ar1.ElemCTI());
			if (ret == 0)
				continue;
			return ret;
		}

		uint32_t cnt1 = ar1.ElemCount();
		uint32_t cnt2 = ar2.ElemCount();
		return (cnt1 == cnt2) ? 0 : ((cnt1 < cnt2) ? -1 : 1);
	}

	RegCompareFunc(CT_INT8, CTCompare<CTInt8>, CTArrayCompare);
	RegCompareFunc(CT_INT16, CTCompare<CTInt16>, CTArrayCompare);
	RegCompareFunc(CT_INT32, CTCompare<CTInt32>, CTArrayCompare);
	RegCompareFunc(CT_INT64, CTCompare<CTInt64>, CTArrayCompare);
	RegCompareFunc(CT_VARINT, CTCompare<CTVarint>, CTArrayCompare);

	RegCompareFunc(CT_FLOAT, CTCompare<CTFloat>, CTArrayCompare);
	RegCompareFunc(CT_DOUBLE, CTCompare<CTDouble>, CTArrayCompare);

	RegCompareFunc(CT_DECIMAL64, CTCompare<CTDecimal64>, CTArrayCompare);
	RegCompareFunc(CT_DECIMAL128, CTCompare<CTDecimal128>, CTArrayCompare);

	RegCompareFunc(CT_TEXT, CTTextCompare, CTArrayCompare);
	RegCompareFunc(CT_BYETS, CTCompare<CTBytes>, CTArrayCompare);

	RegCompareFunc(CT_DATE, CTCompare<CTDate>, CTArrayCompare);
	RegCompareFunc(CT_TIME32, CTCompare<CTTime32>, CTArrayCompare);
	RegCompareFunc(CT_TIME64, CTCompare<CTTime64>, CTArrayCompare);
	RegCompareFunc(CT_TIMESTAMP, CTCompare<CTTimeStamp>, CTArrayCompare);
	RegCompareFunc(CT_TIMETZ32, CTCompare<CTTimeTz32>, CTArrayCompare);
	RegCompareFunc(CT_TIMETZ64, CTCompare<CTTimeTz64>, CTArrayCompare);
	RegCompareFunc(CT_TIMESTAMPTZ, CTCompare<CTTimeStampTz>, CTArrayCompare);
	RegCompareFunc(CT_INTERVAL, CTIntervalCompare, CTArrayCompare);

	RegCompareFunc(CT_BLOB, CTCompare<CTBlob>, CTArrayCompare);

	RegCompareFunc(CT_GEOMETRY, CTCompare<CTGeometry>, CTArrayCompare);
	RegCompareFunc(CT_GEOGRAPHY, CTCompare<CTGeography>, CTArrayCompare);

	RegCompareFunc(CT_JSON, CTCompare<CTJson>, CTArrayCompare);

	int DatumCompare(Datum d1, Datum d2, const ColTypeInfo & cti)
	{
		assert(cti.ct > 0);
		ColType ct = cti.ct;
		bool isarray = IsArrayColType(ct);
		if (isarray)
			ct = GetArrayElementColType(ct);

		auto & ff = datum_compare_ft[(int)ct];
		if (!ff.ct_func || !ff.ct_array_func)
			throw RdbException("DatumCompareFunc for col type is null. coltype:" + ColTypeName(ct));
		if (isarray)
			return ff.ct_array_func(d1, d2, cti);
		else
			return ff.ct_func(d1, d2, cti);
	}

} // end of namespace rdb

// CTEqual & CTHash
// Compare/Euqal/Hash的语义要一致：Compare(d1,d2)==0 <-> Equal(d1,d2)==true -> Hash(d1)==Hash(d2)
// 对于float/double，不能用memcmp来实现equal比较，因为NaN != NaN，+0和-0是相等的但是有不同的字节表示。
namespace rdb
{
	template <typename CT>
	bool CTEqual(Datum d1, Datum d2, const ColTypeInfo &)
	{
		CT v1 = DatumToCT<CT>(d1);
		CT v2 = DatumTOCT<CT>(d2);
		return v1 == v2;
	}
	template <>
	bool CTEqual<CTDecimal128>(Datum d1, Datum d2, const ColTypeInfo &)
	{
		CTDecimal128 v1 = DatumToCT<CTDecimal128>(d1);
		CTDecimal128 v2 = DatumToCT<CTDecimal128>(d2);
		return memcmp(v1->data, v2->data, sizeof(*v1)) == 0;
	}
	// TEXT类型的equal比较不需要collate，因此可以直接用memcmp
	template <>
	bool CTEqual<Varlena *>(Datum d1, Datum d2, const ColTypeInfo &)
	{
		Varlena * v1 = DatumToCT<Varlena *>(d1);
		Varlena * v2 = DatumToCT<Varlena *>(d2);
		auto x1 = GetVLAData(v1);
		auto x2 = GetVLAData(v2);
		if (x1.second != x2.second)
			return false;
		return memcmp(x1.first, x2.first, x1.second) == 0;
	}
	bool CTIntervalEqual(Datum d1, Datum d2, const ColTypeInfo &)
	{
		CTInterval v1 = DatumToCT<CTInterval>(d1);
		CTInterval v2 = DatumToCT<CTInterval>(d2);
		auto x1 = ParseCTInterval(v1);
		auto x2 = ParseCTInterval(v2);
		// 这里可以直接运算，因为不会溢出
		// 每个月按30天计算
		int64_t total_micro1 = x1.first * 30 * MicroPerDay + x1.second;
		int64_t total_micro2 = x2.first * 30 * MicroPerDay + x2.second;
		return total_micro1 == total_micro2;
	}
	// 对于那些可以通过memcmp来实现equal的数组类型，可以用CTEqual<CTArray>，否则需要用CTArrayEqual
	bool CTArrayEqual(Datum d1, Datum d2, const ColTypeInfo & cti)
	{
		CTArray v1 = DatumToCT<CTArray>(d1);
		CTArray v2 = DatumToCT<CTArray>(d2);
		CTArrayReader ar1(v1, cti);
		CTArrayReader ar2(v2, cti);
		if (ar1.ElemCount() != ar2.ElemCount())
			return false;
		if (ar1.HasNull() ^ ar2.HasNull())
			return false;
		if (ar1.HasNull())
		{
			auto bmp1 = ar1.NullBmp();
			auto bmp2 = ar2.NullBmp();
			if (memcmp(bmp1->Data(), bmp2->Data(), bmp1->ByteCount()) != 0)
				return false;
		}
		// 两个数组有相同的元素个数，NULL位图也相同
		Datum elemd1 = 0, elemd2 = 0;
		bool isnull1 = false, isnull2 = false;
		while (ar1.Next(elemd1, isnull1) && ar2.Next(elemd2, isnull2))
		{
			assert(isnull1 == isnull2);
			if (!DatumEqual(elemd1, elemd2, ar1.ElemCTI()))
				return false;
		}
		return true;
	}

	RegEqualFunc(CT_INT8, CTEqual<CTInt8>, CTEqual<CTArray>);
	RegEqualFunc(CT_INT16, CTEqual<CTInt16>, CTEqual<CTArray>);
	RegEqualFunc(CT_INT32, CTEqual<CTInt32>, CTEqual<CTArray>);
	RegEqualFunc(CT_INT64, CTEqual<CTInt64>, CTEqual<CTArray>);
	RegEqualFunc(CT_VARINT, CTEqual<CTVarint>, CTEqual<CTArray>);

	RegEqualFunc(CT_FLOAT, CTEqual<CTFloat>, CTArrayEqual);
	RegEqualFunc(CT_DOUBLE, CTEqual<CTDouble>, CTArrayEqual);

	RegEqualFunc(CT_DECIMAL64, CTEqual<CTDecimal64>, CTEqual<CTArray>);
	RegEqualFunc(CT_DECIMAL128, CTEqual<CTDecimal128>, CTEqual<CTArray>);

	RegEqualFunc(CT_TEXT, CTEqual<CTText>, CTEqual<CTArray>);
	RegEqualFunc(CT_BYTES, CTEqual<CTBytes>, CTEqual<CTArray>);

	RegEqualFunc(CT_DATE, CTEqual<CTDate>, CTEqual<CTArray>);
	RegEqualFunc(CT_TIME32, CTEqual<CTTime32>, CTEqual<CTArray>);
	RegEqualFunc(CT_TIME64, CTEqual<CTTime64>, CTEqual<CTArray>);
	RegEqualFunc(CT_TIMESTAMP, CTEqual<CTTimeStamp>, CTEqual<CTArray>);
	RegEqualFunc(CT_TIMETZ32, CTEqual<CTTimeTz32>, CTEqual<CTArray>);
	RegEqualFunc(CT_TIMETZ64, CTEqual<CTTimeTz64>, CTEqual<CTArray>);
	RegEqualFunc(CT_TIMESTAMPTZ, CTEqual<CTTimeStampTz>, CTEqual<CTArray>);
	RegEqualFunc(CT_INTERVAL, CTIntervalEqual, CTArrayEqual);

	RegEqualFunc(CT_BLOB, CTEqual<CTBlob>, CTEqual<CTArray>);

	RegEqualFunc(CT_GEOMETRY, CTEqual<CTGeometry>, CTEqual<CTArray>);
	RegEqualFunc(CT_GEOGRAPHY, CTEqual<CTGeography>, CTEqual<CTArray>);

	RegEqualFunc(CT_JSON, CTEqual<CTJson>, CTEqual<CTArray>);

	bool DatumEqual(Datum d1, Datum d2, const ColTypeInfo & cti)
	{
		assert(cti.ct > 0);
		ColType ct = cti.ct;
		bool isarray = IsArrayColType(ct);
		if (isarray)
			ct = GetArrayElementColType(ct);

		auto & ff = datum_equal_ft[(int)ct];
		if (!ff.ct_func || !ff.ct_array_func)
			throw RdbException("DatumEqualFunc for col type is null. coltype:" + ColTypeName(ct));
		if (isarray)
			return ff.ct_array_func(d1, d2, cti);
		else
			return ff.ct_func(d1, d2, cti);
	}


	template <typename CT>
	uint64_t CTHash(Datum d, const ColTypeInfo &)
	{
		CT v = DatumToCT<CT>(d);
		return std::hash<CT>()(v);
	}
	template <>
	uint64_t CTHash<CTDecimal128>(Datum d, const ColTypeInfo &)
	{
		CTDecimal128 v = DatumToCT<CTDecimal128>(d);
		uint64_t * p = (uint64_t *)v->data;
		return HashValueCombine(std::hash<uint64_t>()(p[0]), std::hash<uint64_t>()(p[1]));
	}
	template <>
	uint64_t CTHash<Varlena *>(Datum d, const ColTypeInfo &)
	{
		Varlena * v = DatumToCT<Varlena *>(d);
		auto x = GetVLAData(v);
		return HashBytes((const uint8_t *)x.first, x.second);
	}
	uint64_t CTIntervalHash(Datum d, const ColTypeInfo &)
	{
		CTInterval v = DatumToCT<CTInterval>(d);
		auto x = ParseCTInterval(v);
		int64_t total_micro = x.first * 30 * MicroPerDay + x.second;
		return std::hash<int64_t>()(total_micro);
	}
	// 先hash非NULL值，然后再结合位图的hash值，否则{1/2/NULL}和{1/NULL/2}会得到相同的hash值。
	uint64_t CTArrayHash(Datum d, const ColTypeInfo & cti)
	{
		CTArray v = DatumToCT<CTArray>(d);
		CTArrayReader ar(v, cti);
		Datum d = 0;
		bool isnull = false;
		uint64_t h = 0;
		while (ar.Next(d, isnull))
		{
			if (isnull)
				continue;
			h = HashValueCombine(h, DatumHash(d, ar.ElemCTI()));
		}
		auto bmp = ar.NullBmp();
		if (bmp)
		{
			auto h1 = HashBytes(bmp->Data(), bmp->ByteCount());
			h = HashValueCombine(h, h1);
		}
		return h;
	}

	RegHashFunc(CT_INT8, CTHash<CTInt8>, CTHash<CTArray>);
	RegHashFunc(CT_INT16, CTHash<CTInt16>, CTHash<CTArray>);
	RegHashFunc(CT_INT32, CTHash<CTInt32>, CTHash<CTArray>);
	RegHashFunc(CT_INT64, CTHash<CTInt64>, CTHash<CTArray>);
	RegHashFunc(CT_VARINT, CTHash<CTVarint>, CTHash<CTArray>);

	RegHashFunc(CT_FLOAT, CTHash<CTFloat>, CTArrayHash);
	RegHashFunc(CT_DOUBLE, CTHash<CTDouble>, CTArrayHash);

	RegHashFunc(CT_DECIMAL64, CTHash<CTDecimal64>, CTHash<CTArray>);
	RegHashFunc(CT_DECIMAL128, CTHash<CTDecimal128>, CTHash<CTArray>);

	RegHashFunc(CT_TEXT, CTHash<CTText>, CTHash<CTArray>);
	RegHashFunc(CT_BYTES, CTHash<CTBytes>, CTHash<CTArray>);

	RegHashFunc(CT_DATE, CTHash<CTDate>, CTHash<CTArray>);
	RegHashFunc(CT_TIME32, CTHash<CTTime32>, CTHash<CTArray>);
	RegHashFunc(CT_TIME64, CTHash<CTTime64>, CTHash<CTArray>);
	RegHashFunc(CT_TIMESTAMP, CTHash<CTTimeStamp>, CTHash<CTArray>);
	RegHashFunc(CT_TIMETZ32, CTHash<CTTimeTz32>, CTHash<CTArray>);
	RegHashFunc(CT_TIMETZ64, CTHash<CTTimeTz64>, CTHash<CTArray>);
	RegHashFunc(CT_TIMESTAMPTZ, CTHash<CTTimeStampTz>, CTHash<CTArray>);
	RegHashFunc(CT_INTERVAL, CTIntervalHash, CTArrayHash);

	RegHashFunc(CT_BLOB, CTHash<CTBlob>, CTHash<CTArray>);

	RegHashFunc(CT_GEOMETRY, CTHash<CTGeometry>, CTHash<CTArray>);
	RegHashFunc(CT_GEOGRAPHY, CTHash<CTGeography>, CTHash<CTArray>);

	RegHashFunc(CT_JSON, CTHash<CTJson>, CTHash<CTArray>);

	uint64_t DatumHash(Datum d, const ColTypeInfo & cti)
	{
		assert(cti.ct > 0);
		ColType ct = cti.ct;
		bool isarray = IsArrayColType(ct);
		if (isarray)
			ct = GetArrayElementColType(ct);

		auto & ff = datum_hash_ft[(int)ct];
		if (!ff.ct_func || !ff.ct_array_func)
			throw RdbException("DatumHashFunc for col type is null. coltype:" + ColTypeName(ct));
		if (isarray)
			return ff.ct_array_func(d, cti);
		else
			return ff.ct_func(d, cti);
	}

} // end of namespace rdb

namespace rdb
{

}
