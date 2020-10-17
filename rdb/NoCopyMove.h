
#ifndef NO_COPY_MOVE_H
#define NO_COPY_MOVE_H

#define NO_COPY(ClassName)  \
	ClassName(const ClassName & other) = delete; \
	ClassName & operator=(const ClassName & other) = delete

#define NO_MOVE(ClassName)  \
	ClassName(ClassName && other) = delete; \
	ClassName & operator=(ClassName && other) = delete

#define NO_COPY_MOVE(ClassName)  \
	NO_COPY(ClassName); \
	NO_MOVE(ClassName)

#define COPY_DEFAULT(ClassName)  \
	ClassName(const ClassName & other) = default; \
	ClassName & operator=(const ClassName & other) = default

#define MOVE_DEFAULT(ClassName)  \
	ClassName(ClassName && other) = default; \
	ClassName & operator=(ClassName && other) = default

#define MY_MOVE(ClassName, PtrMem)  \
	ClassName(ClassName && other) \
	{ \
		this->PtrMem = other.PtrMem; \
		other.PtrMem = nullptr; \
	} \
	ClassName & operator=(ClassName && other) \
	{ \
		this->PtrMem = other.PtrMem; \
		other.PtrMem = nullptr; \
		return *this; \
	}

#endif // end of NO_COPY_MOVE_H
