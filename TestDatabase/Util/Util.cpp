/*=============================================================================
# Filename: Util.cpp
# Author: Bookug Lobert
# Mail: 1181955272@qq.com
# Last Modified: 2015-10-16 10:43
# Description:
1. firstly written by liyouhuan, modified by zengli
2. achieve functions in Util.h
=============================================================================*/

#include "Util.h"

using namespace std;

string Util::tmp_path = ".tmp/";

int
Util::memoryLeft()
{
    FILE* fp = fopen("/proc/meminfo", "r");
    if(fp == NULL)
        return -1;

    char str[20], tail[3];
    unsigned num, avail = 0, free = 0, buffer = 0, cache = 0;		//WARN:unsigned,memory cant be too large!
    while (fscanf(fp, "%s%u%s", str, &num, tail) != EOF)
    {
        if(strcmp(str, "MemAvailable:") == 0)
        	avail = num;
        if(strcmp(str, "MemFree:") == 0)
        	free = num;
        if(strcmp(str, "Buffers:") == 0)
        	buffer = num;
        if(strcmp(str, "Cached:") == 0)
        	cache = num;
    }

    if (avail == 0)
    	avail = free + buffer + cache;

    fclose(fp);
    return avail / Util::MB;
}

string 
Util::getThreadID()
{
	//thread::id, neither int or long
	auto myid = this_thread::get_id();
	stringstream ss;
	ss << myid;
	return ss.str();
}

long
Util::get_cur_time()
{
    timeval tv;
    gettimeofday(&tv, NULL);
    return (tv.tv_sec*1000 + tv.tv_usec/1000);
}

string
Util::int2string(long n)
{
    string s;
    stringstream ss;
    ss<<n;
    ss>>s;
    return s;
}

void 
Util::Csync(FILE* _fp)
{
	//NOTICE: fclose will also do fflush() operation, but not others
	if(_fp == NULL)
	{
		return; 
	}
	//this will update the buffer from user mode to kernel mode
	fflush(_fp);
	//change to Unix fd and use fsync to sync to disk: fileno(stdin)=0
	int fd = fileno(_fp);
	fsync(fd);
	//FILE * fp = fdopen (1, "w+");   //file descriptor to file pointer 
	//NOTICE: disk scheduler also has a small buffer, but there is no matter even if the power is off
	//(UPS for each server to enable the synchronization between scheduler and disk)
}

string
Util::getSystemOutput(string cmd)
{
    string ans = "";
    string file = Util::tmp_path;
    file += "ans.txt";
    cmd += " > ";
    cmd += file;
    //cerr << cmd << endl;
    int ret = system(cmd.c_str());
    cmd = "rm -rf " + file;
    if(ret < 0)
    {
        fprintf(stderr, "system call failed!\n");
        system(cmd.c_str());
        return NULL;
    }

    ifstream fin(file.c_str());
    if(!fin)
    {
        cerr << "getSystemOutput: Fail to open : " << file << endl;
        return NULL;
    }

	string temp;
	getline(fin, temp);
	while(!fin.eof())
	{
		//cout<<"system line"<<endl;
		if(ans == "")
			ans = temp;
		else
			ans = ans + "\n" + temp;
		getline(fin, temp);
	}
    fin.close();
    //FILE *fp = NULL;
    //if((fp = fopen(file.c_str(), "r")) == NULL)
    //{
    //fprintf(stderr, "unbale to open file: %s\n", file.c_str());
    //}
    //else
    //{
    //char *ans = (char *)malloc(100);
    //fgets(path, 100, fp);
    //char *find = strchr(path, '\n');
    //if(find != NULL)
    //*find = '\0';
    //fclose(fp);
    //}
    system(cmd.c_str());
	//cerr<<"ans: "<<ans<<endl;
    return ans;
}

int
Util::compare(const char* _str1, unsigned _len1, const char* _str2, unsigned _len2)
{
    int ifswap = 1;		//1 indicate: not swapped
    if(_len1 > _len2)
    {
        const char* str = _str1;
        _str1 = _str2;
        _str2 = str;
        unsigned len = _len1;
        _len1 = _len2;
        _len2 = len;
        ifswap = -1;
    }
    unsigned i;
    //DEBUG: if char can be negative, which cause problem when comparing(128+)
    //
    //NOTICE:little-endian-storage, when string buffer poniter is changed to
    //unsigned long long*, the first char is the lowest byte!
    /*
       unsigned long long *p1 = (unsigned long long*)_str1, *p2 = (unsigned long long*)_str2;
       unsigned limit = _len1/8;
       for(i = 0; i < limit; ++i, ++p1, ++p2)
       {
       if((*p1 ^ *p2) == 0)	continue;
       else
       {
       if(*p1 < *p2)	return -1 * ifswap;
       else			return 1 * ifswap;
       }
       }
       for(i = 8 * limit; i < _len1; ++i)
       {
       if(_str1[i] < _str2[i])	return -1 * ifswap;
       else if(_str1[i] > _str2[i])	return 1 * ifswap;
       else continue;
       }
       if(i == _len2)	return 0;
       else	return -1 * ifswap;
       */
    for(i = 0; i < _len1; ++i)
    {   //ASCII: 0~127 but c: 0~255(-1) all transfered to unsigned char when comparing
        if((unsigned char)_str1[i] < (unsigned char)_str2[i])
            return -1 * ifswap;
        else if((unsigned char)_str1[i] > (unsigned char)_str2[i])
            return 1 * ifswap;
        else;
    }
    if(i == _len2)
        return 0;
    else
        return -1 * ifswap;
}

bool 
Util::is_entity_ele(TYPE_ENTITY_LITERAL_ID id) 
{
	return id < Util::LITERAL_FIRST_ID;
}

bool
Util::is_literal_ele(TYPE_ENTITY_LITERAL_ID _id)
{
    return _id >= Util::LITERAL_FIRST_ID;
}

bool 
Util::isEntity(const std::string& _str)
{
	if(_str[0] == '<')
	{
		return true;
	}
	else
	{
		return false;
	}
}

int
Util::cmp_unsigned(const void* _i1, const void* _i2)
{
	unsigned t1 = *(unsigned*)_i1;
	unsigned t2 = *(unsigned*)_i2;
	if(t1 > t2)
	{
		return 1;
	}
	else if(t1 == t2)
	{
		return 0;
	}
	else //t1 < t2
	{
		return -1;
	}
}

void
Util::sort(unsigned*& _id_list, unsigned _list_len)
{
#ifndef PARALLEL_SORT
	qsort(_id_list, _list_len, sizeof(unsigned), Util::cmp_unsigned);
#else
    omp_set_num_threads(thread_num);
    __gnu_parallel::sort(_id_list, _id_list + _list_len, Util::parallel_cmp_unsigned);
#endif
}

unsigned
Util::removeDuplicate(unsigned* _list, unsigned _len)
{
	if (_list == NULL || _len == 0) {
		return 0;
	}
	unsigned valid = 0, limit = _len - 1;
	for(unsigned i = 0; i < limit; ++i)
	{
		if(_list[i] != _list[i+1])
		{
			_list[valid++] = _list[i];
		}
	}
	_list[valid++] = _list[limit];

	return valid;
}

bool
Util::dir_exist(const string _dir)
{
	DIR* dirptr = opendir(_dir.c_str());
	if(dirptr != NULL)
	{
		closedir(dirptr);
		return true;
	}

	return false;
}

string
Util::getExactPath(const char *str)
{
    string cmd = "realpath ";
    cmd += string(str);

    return getSystemOutput(cmd);
}

bool
Util::create_dir(const  string _dir)
{
    if(! Util::dir_exist(_dir))
    {
        mkdir(_dir.c_str(), 0755);
        return true;
    }

    return false;
}

bool
Util::create_file(const string _file) {
	if (creat(_file.c_str(), 0755) > 0) {
		return true;
	}
	return false;
}

void
Util::empty_file(const char* _fname)
{
	FILE * fp;
	//NOTICE: if exist, then overwrite and create a empty file
	fp = fopen(_fname, "w"); 
	if(fp == NULL)
	{
		printf("do empty file %s failed\n", _fname);
	}
	else 
	{
		fclose(fp);
	}
}

bool 
Util::spo_cmp_idtuple(const ID_TUPLE& a, const ID_TUPLE& b)
{
	if(a.subid != b.subid)
	{
		return a.subid < b.subid;
	}

	if(a.preid != b.preid)
	{
		return a.preid < b.preid;
	}

	if(a.objid != b.objid)
	{
		return a.objid < b.objid;
	}

	//all are equal, no need to sort this two
	return false;
}

bool 
Util::ops_cmp_idtuple(const ID_TUPLE& a, const ID_TUPLE& b)
{
	if(a.objid != b.objid)
	{
		return a.objid < b.objid;
	}

	if(a.preid != b.preid)
	{
		return a.preid < b.preid;
	}

	if(a.subid != b.subid)
	{
		return a.subid < b.subid;
	}

	//all are equal, no need to sort this two
	return false;
}

bool 
Util::pso_cmp_idtuple(const ID_TUPLE& a, const ID_TUPLE& b)
{
	if(a.preid != b.preid)
	{
		return a.preid < b.preid;
	}

	if(a.subid != b.subid)
	{
		return a.subid < b.subid;
	}

	if(a.objid != b.objid)
	{
		return a.objid < b.objid;
	}

	//all are equal, no need to sort this two
	return false;
}

bool 
Util::equal(const ID_TUPLE& a, const ID_TUPLE& b)
{
	if(a.subid == b.subid && a.preid == b.preid && a.objid == b.objid)
	{
		return true;
	}
	return false;
}