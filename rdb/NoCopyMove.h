
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

#endif // end of NO_COPY_MOVE_H
