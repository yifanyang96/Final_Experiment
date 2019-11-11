/*=============================================================================
# Filename: Util.h
# Author: Bookug Lobert 
# Mail: 1181955272@qq.com
# Last Modified: 2015-10-16 10:43
# Description: 
1. firstly written by liyouhuan, modified by zengli
2. common macros, functions, classes, etc
# Notice: we only talk about sub-graph isomorphism in the essay, however, in
# this system, the homomorphism is supported.(which means that multiple variables
in the sparql query can point to the same node in data graph)
=============================================================================*/

#ifndef _UTIL_UTIL_H
#define _UTIL_UTIL_H

//basic macros and types are defined here, including common headers 

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <limits.h>
#include <unistd.h>
#include <dirent.h>
#include <string.h>
#include <assert.h>
#include <bitset>
#include <sys/time.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/mman.h>

#include <string>
#include <fstream>
#include <iostream>
#include <sstream>
#include <map>
#include <set>
#include <vector>
#include <queue>
#include <thread> 
#include <mutex> 
#include <algorithm>

typedef unsigned TYPE_TRIPLE_NUM;
typedef unsigned TYPE_ENTITY_LITERAL_ID;
typedef int TYPE_PREDICATE_ID;
static const unsigned INVALID = UINT_MAX;
static const TYPE_ENTITY_LITERAL_ID INVALID_ENTITY_LITERAL_ID = UINT_MAX;
static const TYPE_PREDICATE_ID INVALID_PREDICATE_ID = -1;

#define THREAD_ON 1			

#define xfree(x) free(x); x = NULL;

#define K 3

#define N 3

typedef struct TYPE_ID_TUPLE
{
	TYPE_ENTITY_LITERAL_ID subid;
	TYPE_ENTITY_LITERAL_ID preid;
	TYPE_ENTITY_LITERAL_ID objid;
}ID_TUPLE;

class Util
{
public:
	static const unsigned MB = 1048576;
	static const unsigned GB = 1073741824;
	static const char EDGE_OUT= 'o';
	static const unsigned LITERAL_FIRST_ID = 2 * 1000*1000*1000;
	static const unsigned TRANSFER_SIZE = 1 << 20;
	static const unsigned long long MAX_BUFFER_SIZE = 1 << 30;
	static const unsigned STORAGE_BLOCK_SIZE = 1 << 12;	//fixed size of disk-block in B+ tree storage
	static const TYPE_TRIPLE_NUM TRIPLE_NUM_MAX = INVALID;
	static std::string tmp_path;
	static std::string profile;

	Util();
	static char* l_trim(char *szOutput, const char *szInput);
	static char* a_trim(char *szOutput, const char * szInput);
	static bool configure();  //read init.conf and set the parameters for this system
	static int memoryLeft();
	static std::string getThreadID();
	static long get_cur_time();
	static std::string int2string(long n);
	static void Csync(FILE* _fp);

	static std::string getSystemOutput(std::string cmd);
	static int compare(const char* _str1, unsigned _len1, const char* _str2, unsigned _len2); //QUERY(how to use default args)
	static bool is_entity_ele(TYPE_ENTITY_LITERAL_ID id);
	static bool is_literal_ele(TYPE_ENTITY_LITERAL_ID id);
	static bool isEntity(const std::string& _str);
	static int cmp_unsigned(const void* _i1, const void* _i2);
	static void sort(unsigned*& _id_list, unsigned _list_len);
	static unsigned removeDuplicate(unsigned*, unsigned);
	static std::string getExactPath(const char* path);

	static bool dir_exist(const std::string _dir);
	static bool create_dir(const std:: string _dir);
	static bool create_file(const std::string _file);
	static void empty_file(const char* _fname);

	static bool spo_cmp_idtuple(const ID_TUPLE& a, const ID_TUPLE& b);
	static bool ops_cmp_idtuple(const ID_TUPLE& a, const ID_TUPLE& b);
	static bool pso_cmp_idtuple(const ID_TUPLE& a, const ID_TUPLE& b);
	static bool equal(const ID_TUPLE& a, const ID_TUPLE& b);
	static std::map<std::string, std::string> global_config;

	static std::string node2string(const char* _raw_str);
	static unsigned bsearch_int_uporder(unsigned _key, const unsigned* _array, unsigned _array_num);
	static void split(const std::string& s, std::vector<std::string>& sv, const char* delim = " ");
};

class BlockInfo
{
public:
	unsigned num;			
	BlockInfo* next;
	BlockInfo()
	{
		num = 0;
		next = NULL;
	}
	BlockInfo(unsigned _num)
	{
		num = _num;
		next = NULL;
	}
	BlockInfo(unsigned _num, BlockInfo* _bp)
	{
		num = _num;
		next = _bp;
	}
};

class Buffer
{
public:
	unsigned size;
	std::string* buffer;
	
	Buffer(unsigned _size)
	{
		this->size = _size;
		this->buffer = new std::string[this->size];
		//for(unsigned i = 0; i < this->size; ++i)
		//{
			//this->buffer[i] = "";
		//}
	}
	
	bool set(unsigned _pos, const std::string& _ele)
	{
		if(_pos >= this->size)
		{
			return false;
		}
		//BETTER:check if this position is used, and decide abort or update?
		this->buffer[_pos] = _ele;
		return true;
	}

	bool del(unsigned _pos)
	{
		if(_pos >= this->size)
		{
			return false;
		}
		this->buffer[_pos] = "";
		return true;
	}

	std::string& get(unsigned _pos) const
	{
		return this->buffer[_pos];
	}

	~Buffer()
	{
		//for(unsigned i = 0; i < size; ++i)
		//{
			//delete[] buffer[i];
		//}
		delete[] buffer;
	}
};

#endif //_UTIL_UTIL_H