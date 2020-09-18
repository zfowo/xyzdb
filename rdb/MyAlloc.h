
#ifndef MY_ALLOC_H
#define MY_ALLOC_H

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdexcept>
#include <new>
#include <mutex>

#include "NoCopyMove.h"

namespace rdb
{
	class Allocator;
	extern thread_local Allocator * allocer;

	// myalloc不抛出bad_alloc异常。
	void * myalloc(size_t size, bool zero = false);
	// old_size是需要从ptr拷贝的数据的大小，不是ptr指向的内存的实际大小。
	void * myrealloc(void * ptr, size_t old_size, size_t new_size);
	void   myfree(void * ptr);

	// Allocater不抛出bad_alloc异常。
	class Allocator
	{
	public:
		Allocator() : alloc_num(0), free_num(0), alloc_total_size(0) {}
		virtual ~Allocator() {}

		NO_COPY_MOVE(Allocator);

		virtual void * Alloc(size_t size) = 0;
		virtual void   Free(void * ptr) = 0;
	protected:
		uint64_t alloc_num;
		uint64_t free_num;
		uint64_t alloc_total_size;
	};

	class ThrAllocator
	{
	public:
		ThrAllocator(Allocator * alloc_) : alloc(alloc_) {}

		NO_COPY_MOVE(ThrAllocator);

		void * Alloc(size_t size) 
		{
			std::lock_guard<std::mutex> lg(this->mtx);
			return this->alloc->Alloc(size); 
		}
		void   Free(void * ptr) 
		{
			std::lock_guard<std::mutex> lg(this->mtx);
			this->Free(ptr); 
		}
	private:
		Allocator * alloc;
		std::mutex mtx;
	};

	// 适用于内存操作分为两个阶段的情况: 分配阶段和释放阶段。
	// 比如生成一个大的链表->使用这个链表->释放链表。
	class TwoPhaseAllocator : public Allocator
	{
	public:
		TwoPhaseAllocator(size_t max_mem_mb = 1024);
		virtual ~TwoPhaseAllocator();

		NO_COPY_MOVE(TwoPhaseAllocator);

		virtual void * Alloc(size_t size) override;
		virtual void   Free(void * ptr) override;
	private:
		char * heap;
		char * next;
		size_t heap_size;
	};

	class SizedAllocator : public Allocator
	{
	public:
		SizedAllocator();
		virtual ~SizedAllocator();

		NO_COPY_MOVE(SizedAllocator);

		virtual void * Alloc(size_t size) override;
		virtual void   Free(void * ptr) override;
	};

	class CurrentAllocatorSetter
	{
	public:
		CurrentAllocatorSetter(Allocator * new_allocator, bool del_new_alloc_ = false);
		~CurrentAllocatorSetter();

		NO_COPY_MOVE(CurrentAllocatorSetter);

		Allocator * GetNewAllocator() const { return this->new_allocator; }
		Allocator * GetOldAllocator() const { return this->old_allocator; }
	private:
		Allocator * old_allocator;
		Allocator * new_allocator;
		bool del_new_alloc;
	};

	// for C++
	template <typename T>
	struct CppAllocator
	{
		using value_type = T;

		CppAllocator() = default;
		template <typename U> CppAllocator(const CppAllocator<U> &) {}

		T * allocate(size_t n)
		{
			void * p = myalloc(n * sizeof(T));
			if (!p)
				throw std::bad_alloc();
			return (T*)p;
		}
		void deallocate(T * p, size_t n)
		{
			(void)n;
			myfree(p);
		}
	};
	template <typename T1, typename T2>
	bool operator==(const CppAllocator<T1> &, const CppAllocator<T2> &) { return true; }
	template <typename T1, typename T2>
	bool operator!=(const CppAllocator<T1> &, const CppAllocator<T2> &) { return false; }
} // end of namespace rdb


// 下面这些需要定义在全局namespace里。
struct UseMyAlloc {};
extern UseMyAlloc usemyalloc;
void * operator new(size_t size, const UseMyAlloc & uma);
void   operator delete(void * ptr, const UseMyAlloc & uma);
void * operator new(size_t size, const UseMyAlloc & uma, const std::nothrow_t & nt);
void   operator delete(void * ptr, const UseMyAlloc & uma, const std::nothrow_t & nt);
// 正常情况下不会调用前面operator delete的重载版本，重载版本只在类的构造函数抛出异常
// 的时候才会被调用，因此通过new(usemyalloc) T()创建的对象，需要用mydelete来释放。
template <typename T>
void mydelete(T * p)
{
	p->~T();
	operator delete(p, usemyalloc);
}
// new类数组的时候，需要记录数组的元素个数，具体实现是编译器相关的，
// 一种比较普遍的实现方式是：多分配8个字节用于记录数组的元素个数，
// 这样new表达式返回的地址值和operator new[]返回的地址值可能是不相同的，
// 而且正常情况下不会调用operator delte[]的重载版本，因此无法实现UseMyAlloc的重载版本。
// 所以需要数组的时候使用MyVector。
//inline void * operator new[](size_t size, const UseMyAlloc & uma) { return operator new(size, uma); }
//inline void operator delete[](void * ptr, const UseMyAlloc & uma) { operator delete(ptr, uma); }

// 
// c++标准库中的类
// 
#include <memory>
#include <string>
#include <vector>
#include <map>

namespace rdb
{
	template <typename T>
	using mybasicstring = std::basic_string<T, std::char_traits<T>, CppAllocator<T>>;
	using mystring = mybasicstring<char>;
	using mywstring = mybasicstring<wchar_t>;
	using myu16string = mybasicstring<char16_t>;
	using myu32string = mybasicstring<char32_t>;

	template <typename T>
	using myvector = std::vector<T, CppAllocator<T>>;

	template <typename Key, typename T, typename Compare = std::less<Key>>
	using mymap = std::map<Key, T, Compare, CppAllocator<std::pair<const Key, T>>>;

	template <typename T>
	struct mydeleter
	{
		void operator()(T * p)
		{
			mydelete(p);
		}
	};

} // end of namespace rdb

#endif // end of MY_ALLOC_H
