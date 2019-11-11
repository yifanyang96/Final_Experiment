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

string Util::profile = "init.conf";
string Util::tmp_path = ".tmp/";
map<string, string> Util::global_config;

Util::Util()
{
    Util::configure();
#ifdef DEBUG_KVSTORE
    if(this->debug_kvstore == NULL)
    {
        string s = this->debug_path + "kv.log";
        this->debug_kvstore = fopen(s.c_str(), "w+");
        if(this->debug_kvstore == NULL)
        {
            cerr << "open error: kv.log\n";
            this->debug_kvstore = stderr;
        }
    }
#endif
#ifdef DEBUG_DATABASE
    if(this->debug_database == NULL)
    {
        string s = this->debug_path + "db.log";
        this->debug_database = fopen(s.c_str(), "w+");
        if(this->debug_database == NULL)
        {
            cerr << "open error: db.log\n";
            this->debug_database = stderr;
        }
    }
#endif
#ifdef DEBUG_VSTREE
    if(this->debug_vstree == NULL)
    {
        string s = this->debug_path + "vs.log";
        this->debug_vstree = fopen(s.c_str(), "w+");
        if(this->debug_vstree == NULL)
        {
            cerr << "open error: vs.log\n";
            this->debug_vstree = stderr;
        }
    }
#endif
}

char*
Util::l_trim(char* szOutput, const char* szInput)
{
    assert(szInput != NULL);
    assert(szOutput != NULL);
    assert(szOutput != szInput);
    for   (; *szInput != '\0' && isspace(*szInput); ++szInput);
    return strcpy(szOutput, szInput);
}

char*
Util::a_trim(char * szOutput, const char * szInput)
{
    char *p = NULL;
    assert(szInput != NULL);
    assert(szOutput != NULL);
    l_trim(szOutput, szInput);
    for   (p = szOutput + strlen(szOutput) - 1; p >= szOutput && isspace(*p); --p);
    *(++p) = '\0';
    return szOutput;
}

bool
Util::configure()
{
    const unsigned len = 505;
    char *buf, *c;
    char buf_i[len], buf_o[len];
    FILE *fp = NULL;
	char keyname[len];
	char keyval[len];

	//initialize the settings
	Util::global_config["gstore_mode"] = "single";
	//NOTICE+BETTER+TODO:use macro is better to avoid too many judging on this variable(add a DEBUG macro at the outer)
	Util::global_config["debug_level"] = "simple";
	Util::global_config["db_home"] = ".";
	Util::global_config["db_suffix"] = ".db";
	Util::global_config["buffer_maxium"] = "100";
	Util::global_config["thread_maxium"] = "1000";
	//TODO:to be recoverable
	Util::global_config["operation_logs"] = "true";

#ifdef DEBUG
	fprintf(stderr, "profile: %s\n", profile.c_str());
#endif
    if((fp = fopen(profile.c_str(), "r")) == NULL)  //NOTICE: this is not a binary file
    {
#ifdef DEBUG
        fprintf(stderr, "openfile [%s] error [%s]\n", profile.c_str(), strerror(errno));
#endif
        return false;
    }
    fseek(fp, 0, SEEK_SET);

    while(!feof(fp) && fgets(buf_i, len, fp) != NULL)
    {
		//fprintf(stderr, "buffer: %s\n", buf_i);
        Util::l_trim(buf_o, buf_i);
        if(strlen(buf_o) <= 0)
            continue;
        buf = NULL;
        buf = buf_o;
		if(buf[0] == '#')
		{
			continue;
		}
		else if(buf[0] == '[') 
		{
			continue;
		} 
		if((c = (char*)strchr(buf, '=')) == NULL)
			continue;
		memset(keyname, 0, sizeof(keyname));
		sscanf(buf, "%[^=|^ |^\t]", keyname);
#ifdef DEBUG
				//fprintf(stderr, "keyname: %s\n", keyname);
#endif
		sscanf(++c, "%[^\n]", keyval);
		char *keyval_o = (char *)calloc(strlen(keyval) + 1, sizeof(char));
		if(keyval_o != NULL) 
		{
			Util::a_trim(keyval_o, keyval);
#ifdef DEBUG
			//fprintf(stderr, "keyval: %s\n", keyval_o);
#endif
			if(keyval_o && strlen(keyval_o) > 0)
			{
				//strcpy(keyval, keyval_o);
				global_config[string(keyname)] = string(keyval_o);
			}
			xfree(keyval_o);
		}
	}

    fclose(fp);
	//display all settings here
	cout<<"the current settings are as below: "<<endl;
	cout<<"key : value"<<endl;
	cout<<"------------------------------------------------------------"<<endl;
	for(map<string, string>::iterator it = global_config.begin(); it != global_config.end(); ++it)
	{
		cout<<it->first<<" : "<<it->second<<endl;
	}
	cout<<endl;

	return true;
	//return Util::config_setting() && Util::config_debug() && Util::config_advanced();
}

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

string
Util::node2string(const char* _raw_str) {
	string _output;
	unsigned _first_quote = 0;
	unsigned _last_quote = 0;
	bool _has_quote = false;
	for (unsigned i = 0; _raw_str[i] != '\0'; i++) {
		if (_raw_str[i] == '\"') {
			if (!_has_quote) {
				_first_quote = i;
				_last_quote = i;
				_has_quote = true;
			}
			else {
				_last_quote = i;
			}
		}
	}
	if (_first_quote==_last_quote) {
		_output += _raw_str;
		return _output;
	}
	for (unsigned i = 0; i <= _first_quote; i++) {
		_output += _raw_str[i];
	}
	for (unsigned i = _first_quote + 1; i < _last_quote; i++) {
		switch (_raw_str[i]) {
		case '\n':
			_output += "\\n";
			break;
		case '\r':
			_output += "\\r";
			break;
		case '\t':
			_output += "\\t";
			break;
		case '\"':
			_output += "\\\"";
			break;
		case '\\':
			_output += "\\\\";
			break;
		default:
			_output += _raw_str[i];
		}
	}
	for (unsigned i = _last_quote; _raw_str[i] != 0; i++) {
		_output += _raw_str[i];
	}
	return _output;
}

unsigned
Util::bsearch_int_uporder(unsigned _key, const unsigned* _array, unsigned _array_num)
{
    if (_array_num == 0)
    {
        //return -1;
		return INVALID;
    }
    if (_array == NULL)
    {
        //return -1;
		return INVALID;
    }

    unsigned _first = _array[0];
    unsigned _last = _array[_array_num - 1];

    if (_last == _key)
    {
        return _array_num - 1;
    }

    if (_last < _key || _first > _key)
    {
        //return -1;
		return INVALID;
    }

    unsigned low = 0;
    unsigned high = _array_num - 1;

    unsigned mid;
    while (low <= high)
    {
        mid = (high - low) / 2 + low;
        if (_array[mid] == _key)
        {
            return mid;
        }
        if (_array[mid] > _key)
        {
            high = mid - 1;
        }
        else
        {
            low = mid + 1;
        }
    }

    //return -1;
	return INVALID;
}

void
Util::split(const string& s, vector<string>& sv, const char* delim) {
    sv.clear();
	char* buffer = new char[s.size() + 1];
    copy(s.begin(), s.end(), buffer);
    char* p = strtok(buffer, delim);
    do {
        sv.push_back(p);
    } while ((p = strtok(NULL, delim)));   // 6.
}