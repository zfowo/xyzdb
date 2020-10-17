
#include "ColTypeFuncs.h"

namespace rdb
{
	static std::string col_type_names[CT_MAX];
	static int col_type_sizes[CT_MAX];

	struct ColTypeNameSizeRegister
	{
		ColTypeNameSizeRegister(ColType ct, const std::string & ctname, int ctsz)
		{
			col_type_names[(int)ct] = ctname.substr(3);
			col_type_sizes[(int)ct] = ctsz;
		}
	};
#define RegColTypeNameSize(ct, sz)  ColTypeNameSizeRegister reg_##ct##_name_sz(ct, #ct, sz)

	RegColTypeNameSize(CT_INT8, (int)sizeof(CTInt8));
	RegColTypeNameSize(CT_INT16, (int)sizeof(CTInt16));
	RegColTypeNameSize(CT_INT32, (int)sizeof(CTInt32));
	RegColTypeNameSize(CT_INT64, (int)sizeof(CTInt64));
	RegColTypeNameSize(CT_VARINT, -1);

	RegColTypeNameSize(CT_FLOAT, (int)sizeof(CTFloat));
	RegColTypeNameSize(CT_DOUBLE, (int)sizeof(CTDouble));

	RegColTypeNameSize(CT_DECIMAL64, (int)sizeof(CTDecimal64));
	RegColTypeNameSize(CT_DECIMAL128, (int)sizeof(CTDecimal128Data));

	RegColTypeNameSize(CT_TEXT, -1);
	RegColTypeNameSize(CT_BYTES, -1);

	RegColTypeNameSize(CT_DATE, (int)sizeof(CTDate));
	RegColTypeNameSize(CT_TIME32, (int)sizeof(CTTime32));
	RegColTypeNameSize(CT_TIME64, (int)sizeof(CTTime64));
	RegColTypeNameSize(CT_TIMESTAMP, (int)sizeof(CTTimeStamp));
	RegColTypeNameSize(CT_TIMETZ32, (int)sizeof(CTTimeTz32));
	RegColTypeNameSize(CT_TIMETZ64, (int)sizeof(CTTimeTz64));
	RegColTypeNameSize(CT_TIMESTAMPTZ, (int)sizeof(CTTimeStampTz));
	RegColTypeNameSize(CT_INTERVAL, (int)sizeof(CTInterval));

	RegColTypeNameSize(CT_BLOB, (int)sizeof(CTBlob));

	RegColTypeNameSize(CT_GEOMETRY, -1);
	RegColTypeNameSize(CT_GEOGRAPHY, -1);

	RegColTypeNameSize(CT_JSON, -1);

	const std::string & ColTypeName(ColType ct)
	{
		return col_type_names[(int)ct];
	}

	int ColTypeSize(ColType ct)
	{
		return col_type_sizes[(int)ct];
	}

} // end of namespace rdb
