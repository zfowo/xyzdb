
#include <iostream>
#include "MyAlloc.h"

#define LOG_MYALLOC

namespace rdb
{
	thread_local Allocator * allocer = nullptr;

	void * myalloc(size_t size, bool zero)
	{
		assert(size > 0);
		void * p = nullptr;
		if (!allocer)
			p = malloc(size);
		else
			p = allocer->Alloc(size);
		if (p && zero)
			memset(p, '\0', size);
#ifdef LOG_MYALLOC
		std::cout << "myalloc: size:" << size << " zero:" << zero << " ptr:" << (void *)p << std::endl;
#endif
		return p;
	}

	void * myrealloc(void * ptr, size_t old_size, size_t new_size)
	{
#ifdef LOG_MYALLOC
		std::cout << "myrealloc: new_size:" << new_size << " old_size:" << old_size << std::endl;
#endif
		assert(ptr && new_size > 0);
		if (!allocer)
			return realloc(ptr, new_size);

		if (new_size <= old_size)
			return ptr;

		void * new_ptr = allocer->Alloc(new_size);
		if (!new_ptr)
			return nullptr;
		if (old_size > 0)
			memcpy(new_ptr, ptr, old_size);
		allocer->Free(ptr);
		return new_ptr;
	}

	void  myfree(void * ptr)
	{
#ifdef LOG_MYALLOC
		std::cout << "myfree: ptr:" << ptr << std::endl;
#endif
		if (!ptr)
			return;

		if (!allocer)
			free(ptr);
		else
			allocer->Free(ptr);
	}

	// class TwoPhaseAllocator
	TwoPhaseAllocator::TwoPhaseAllocator(size_t max_mem_mb)
	{
		max_mem_mb = (max_mem_mb + 3) & ~(size_t)3; // 4MB的倍数
		this->heap_size = max_mem_mb << 20;
		this->heap = (char *)malloc(this->heap_size);
		this->next = this->heap;
		if (!this->heap)
			throw std::runtime_error("can not malloc " + std::to_string(max_mem_mb) + "MB memory.");
	}
	TwoPhaseAllocator::~TwoPhaseAllocator()
	{
		std::cout << "~TwoPhaseAllocator: " << std::endl
		          << "    heap: " << (void *)this->heap << std::endl
		          << "    heap_size: " << (this->heap_size >> 20) << "MB" << std::endl
		          << "    alloc_num: " << this->alloc_num << std::endl
			      << "    free_num: " << this->free_num  << std::endl
			      << "    alloc_total_size: " << this->alloc_total_size << std::endl;
		free(this->heap);
	}
	void * TwoPhaseAllocator::Alloc(size_t size)
	{
		size = (size + 7) & ~(size_t)7;
		if (this->next - this->heap + size > this->heap_size)
			return nullptr;

		this->alloc_num++;
		this->alloc_total_size += size;
		char * ptr = this->next;
		this->next += size;
		return ptr;
	}
	void TwoPhaseAllocator::Free(void * ptr)
	{
		this->free_num++;
	}

	// class CurrentAllocatorSetter
	CurrentAllocatorSetter::CurrentAllocatorSetter(Allocator * new_allocator, bool del_new_alloc_)
	{
		this->old_allocator = allocer;
		this->new_allocator = new_allocator;
		this->del_new_alloc = del_new_alloc_;
		allocer = this->new_allocator;
	}
	CurrentAllocatorSetter::~CurrentAllocatorSetter()
	{
		allocer = this->old_allocator;
		if (this->del_new_alloc && this->new_allocator)
			delete this->new_allocator;
	}

} // end of namespace rdb


UseMyAlloc usemyalloc;

void * operator new(size_t size, const UseMyAlloc & /*uma*/)
{
	void * ptr = rdb::myalloc(size);
	if (!ptr)
		throw std::bad_alloc();
	return ptr;
}
void   operator delete(void * ptr, const UseMyAlloc & /*uma*/)
{
	rdb::myfree(ptr);
}
void * operator new(size_t size, const UseMyAlloc & /*uma*/, const std::nothrow_t & /*nt*/)
{
	return rdb::myalloc(size);
}
void   operator delete(void * ptr, const UseMyAlloc & /*uma*/, const std::nothrow_t & /*nt*/)
{
	rdb::myfree(ptr);
}

