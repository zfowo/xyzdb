
#ifndef BASE_TYPES_H
#define BASE_TYPES_H

#include <stdint.h>
#include <string>
#include <vector>
// 
// 基本类型的定义，只依赖于C++标准库。
// 

namespace rdb
{
	// 
	// 列值有2种格式：外部格式和内部格式。外部格式比如文本格式；而内部格式是保存在内存/磁盘上的格式，
	// 内部格式由各自的具体类型表示，比如int8_t/.../varint/CTDecimal64等，有时内部处理时需要一种
	// 统一的类型来表示这个内部格式的值，这就是Datum类型。
	// 
	// 列值用uintptr_t表示，分为2类：
	//   1)可以直接用uintptr_t表示的。
	//   2)uintptr_t保存的是个地址值，其指向实际的列值数据。
	// 要解析列值，还需要列类型以及列类型修饰符。
	// 
	using Datum = uintptr_t;
	using ColTypeMod = int32_t;
	enum ColType : int16_t
	{
		CT_INT8 = 0x01, 
		CT_INT16 = 0x02, 
		CT_INT32 = 0x03, 
		CT_INT64 = 0x04, 
		CT_VARINT = 0x05, 

		CT_FLOAT = 0x06, 
		CT_DOUBLE = 0x07, 

		CT_DECIMAL64 = 0x08, 
		CT_DECIMAL128 = 0x09, 

		CT_TEXT = 0x0A, 
		CT_BYTES = 0x0B, 

		CT_DATE = 0x0C, 
		CT_TIME32 = 0x0D, 
		CT_TIME64 = 0x0E, 
		CT_TIMESTAMP = 0x0F, 
		CT_TIMETZ32 = 0x10, 
		CT_TIMETZ64 = 0x11, 
		CT_TIMESTAMPTZ = 0x12, 
		CT_INTERVAL = 0x13, 

		CT_BLOB = 0x14, 

		CT_GEOMETRY = 0x15, 
		CT_GEOGRAPHY = 0x16, 

		CT_JSON = 0x17, 

		CT_MAX, 
	};
	struct ColTypeInfo
	{
		ColType ct;
		ColTypeMod ctmod;
		ColTypeInfo(ColType ct_, ColTypeMod ctmod_ = 0) : ct(ct_), ctmod(ctmod_)
		{}
	};
	using RowTypeInfo = std::vector<ColTypeInfo *>;
	// 需要RowTypeInfo才能解析RowDatum。
	struct RowDatum
	{
		Datum * col_datums;
		bool  * col_nulls;
		bool has_null;
	};
	
	using CTInt8 = int8_t;
	using CTInt16 = int16_t;
	using CTInt32 = int32_t;
	using CTInt64 = int64_t;
	using CTVarint = uint64_t;

	using CTFloat = float;
	using CTDouble = double;

	using CTDecimal64 = int64_t;
	struct CTDecimal128
	{
		uint8_t data[16];
	};
	
	struct Varlena
	{
		uint8_t flag;
		char data[0];
	};
	const uint8_t FlagLenMask = 0x03;
	const uint8_t ToastFalg = 0x00;
	const uint8_t Len1Flag = 0x01;
	const uint8_t Len2Flag = 0x02;
	const uint8_t Len4Flag = 0x03;
	using CTText = Varlena*;
	using CTBytes = Varlena*;
	
	// date是距离epoch的天数。
	// time32/timetz32是距离00:00:00的毫秒数。
	// time64/timetz64是距离00:00:00的微秒数。
	// timestamp/timestamptz是距离epoch的微秒数。
	// interval用64位表示，高20位表示月数，低44位表示微秒数，各部分的最高位是符号位。
	using CTDate = int32_t;
	using CTTime32 = uint32_t;
	using CTTime64 = uint64_t;
	using CTTimeStamp = int64_t;
	using CTTimeTz32 = uint32_t;
	using CTTimeTz64 = uint64_t;
	using CTTimeStampTz = int64_t;
	using CTInterval = uint64_t;
	
	using CTBlob = uint64_t;
	
	using CTGeometry = Varlena*;
	using CTGeography = Varlena*;

	using CTJson = Varlena*;
	
	// 
	// 这些id都从1开始计数。
	// 表定义中的每个列都有一个内部id。列分区也有一个内部id。
	// 表的每条行记录有一个行id，行修改后其id保持不变，这样就无需修改索引数据。
	// 行id是分区范围的，如果是表范围的话，当启动初始化最大行id的时候需要读取整个表的数据。
	// 
	// 表id和索引id是db范围的，而表分区id是表范围的。
	// 索引共用表分区id。另外索引不是全局，而是各个表分区上的索引。
	// 
	// 表id和索引id以及表分区id不能复用，比如删除表t1(包含分区id:100/101/102)后再创建表t2，
	// 那么表t2不能复用分区id:100/101/102，因为删除表t1后其数据可能还没有删除(compact有滞后)。
	// 
	// 表id和索引id的维护有2种方法：
	//   1) 用merge方式实现的计数器。不过并发的时候会频繁导致锁超时，因为修改同一个key。
	//   2) 记录每次生成的id，然后由后台线程清理老的生成记录(最大的id生成记录需要保留)。
	// 表分区id直接用一个key来记录最近生成的id即可，因为同一个表上不允许并发添加分区。
	// 
	// 表id的高2位用于表示表是否有表分区和列分区。对于没有分区的表，key里面就无需包含分区id。
	// 索引id的高1位用于表示表是否包含表分区。索引的key里不包含列分区id。
	// 
	using ColId = int16_t;
	using ColPartId = int16_t;
	using RowId = uint64_t;
	using TableId = uint32_t;
	using TablePartId = uint32_t;
	using IndexId = uint32_t;
	
	// 表分区类型。
	// PT_NONE表示不是分区表，内部存储的key中也没有分区id。
	enum PartType
	{
		PT_NONE, 
		PT_ROUNDROBIN, 
		PT_HASH, 
		PT_RANGE, 
		PT_LIST, 
	};

	enum TableType
	{
		TT_ROWSTORE, 
		TT_COLSTORE, 
		TT_MEMSTORE, 
	};
	enum IndexType
	{
		IT_BTREE, 
		IT_PK, 
		IT_UNIQUE, 
		IT_INVERTED,  // 倒排索引
		IT_INVERTED2, // 支持后缀匹配的倒排索引
		IT_RANGE, // 范围索引，类似于pg里的BRIN索引，适用于只插入的表。
		IT_GIST, 
	};

	enum ScanType
	{
		ST_SEQ, 
		ST_INDEX, 
	};
	enum JoinType
	{
		JT_NESTLOOP, 
		JT_HASH, 
		JT_MERGE, 
	};

} // end of namespace rdb

#endif // end of BASE_TYPES_H
