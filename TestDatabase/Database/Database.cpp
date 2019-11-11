#include "Database.h"

using namespace std;

Database::Database()
{
	this->name = "";
	this->store_path = "";
	string store_path = ".";
	this->signature_binary_file = "signature.binary";
	this->six_tuples_file = "six_tuples";
	this->db_info_file = "db_info_file.dat";
	this->id_tuples_file = "id_tuples";
	this->update_log = "update.log";
	this->update_log_since_backup = "update_since_backup.log";

	string kv_store_path = store_path + "/kv_store";
	this->kvstore = new KVstore(kv_store_path);

	string stringindex_store_path = store_path + "/stringindex_store";
	this->stringindex = new StringIndex(stringindex_store_path);
	this->stringindex->SetTrie(this->kvstore->getTrie());
	//this->encode_mode = Database::STRING_MODE;
	this->encode_mode = Database::ID_MODE;
	this->is_active = false;
	this->sub_num = 0;
	this->pre_num = 0;
	this->literal_num = 0;
	this->entity_num = 0;
	this->triples_num = 0;

	this->pre2num = NULL;
	this->pre2sub = NULL;
	this->pre2obj = NULL;
	this->entity_buffer = NULL;
	this->entity_buffer_size = 0;
	this->literal_buffer = NULL;
	this->literal_buffer_size = 0;

	this->if_loaded = false;

	//this->trie = NULL;

	//this->resetIDinfo();
	this->initIDinfo();

	pthread_rwlock_init(&(this->update_lock), NULL);
}

Database::Database(string _name)
{
	this->name = _name;
	size_t found = this->name.find_last_not_of('/');
	if (found != string::npos) 
	{
		this->name.erase(found + 1);
	}
	this->store_path = Util::global_config["db_home"] + "/" + this->name + Util::global_config["db_suffix"];

	this->signature_binary_file = "signature.binary";
	this->six_tuples_file = "six_tuples";
	this->db_info_file = "db_info_file.dat";
	this->id_tuples_file = "id_tuples";
	this->update_log = "update.log";
	this->update_log_since_backup = "update_since_backup.log";

	string kv_store_path = store_path + "/kv_store";
	cout << "Start creating kvstore..." << endl;
	this->kvstore = new KVstore(kv_store_path);
	string stringindex_store_path = store_path + "/stringindex_store";
	cout << "Start creating StringIndex..." << endl;
	this->stringindex = new StringIndex(stringindex_store_path);
	this->stringindex->SetTrie(this->kvstore->getTrie());
	//this->encode_mode = Database::STRING_MODE;
	this->encode_mode = Database::ID_MODE;
	this->is_active = false;
	this->sub_num = 0;
	this->pre_num = 0;
	this->literal_num = 0;
	this->entity_num = 0;
	this->triples_num = 0;

	this->if_loaded = false;

	this->pre2num = NULL;
	this->pre2sub = NULL;
	this->pre2obj = NULL;
	this->entity_buffer = NULL;
	this->entity_buffer_size = 0;
	this->literal_buffer = NULL;
	this->literal_buffer_size = 0;

	//this->trie = NULL;

	//this->resetIDinfo();
	this->initIDinfo();

	pthread_rwlock_init(&(this->update_lock), NULL);
	cout << "Finish creating db..." << endl;
}

void
Database::setPreMap()
{
	//this->maxNumPID = this->minNumPID = -1;a
	this->maxNumPID = this->minNumPID = INVALID_PREDICATE_ID;
	//int max = 0, min = this->triples_num + 1;
	TYPE_TRIPLE_NUM max = 0, min = this->triples_num + 1;

	this->pre2num = new TYPE_TRIPLE_NUM[this->limitID_predicate];
	this->pre2sub = new TYPE_TRIPLE_NUM[this->limitID_predicate];
	this->pre2obj = new TYPE_TRIPLE_NUM[this->limitID_predicate];
	TYPE_PREDICATE_ID valid = 0, i, t;

	for (i = 0; i < this->limitID_predicate; ++i)
	{
		if (valid == this->pre_num)
		{
			t = 0;
		}
		else
		{
			t = this->kvstore->getPredicateDegree(i);
		}
		this->pre2num[i] = t;

		//NOTICE:only when pre2num[i]>0 then i is a valid predicate id
		if (t > 0)
		{
			valid++;
			if (t > max)
			{
				this->maxNumPID = i;
			}
			if (t < min)
			{
				this->minNumPID = i;
			}

			unsigned *list = NULL;
			unsigned len = 0;
			this->kvstore->getsubIDlistBypreID(i,list,len,true);
			this->pre2sub[i] = len;
			free(list);
			this->kvstore->getobjIDlistBypreID(i,list,len,true);
			this->pre2obj[i] = len;
			free(list);
		}
		else
		{
			this->pre2sub[i] = 0;
			this->pre2obj[i] = 0;
		}
	}
	/*
	for(int i = 0;i < this->pre_num;i++)
	{
		cout <<"pre2num["<<i<<"]: "<<this->pre2num[i]<<endl;
		cout <<"pre2sub["<<i<<"]: "<<this->pre2sub[i]<<endl;
		cout <<"pre2obj["<<i<<"]: "<<this->pre2obj[i]<<endl;
	}
	*/

}

void
Database::build_CacheOfPre2values()
{
	cout << "now add cache of preID2values..." << endl;
	while (!candidate_preID.empty())
	{
		this->kvstore->AddIntoPreCache(candidate_preID.top().key);
		candidate_preID.pop();
	}
}

void
Database::build_CacheOfObj2values()
{
	cout << "now add cache of objID2values..." << endl;
	while (!important_objID.empty())
	{
		this->kvstore->AddIntoObjCache(important_objID.top().key);
		this->kvstore->AddIntoEntityUBCache(important_objID.top().key);
		this->kvstore->AddIntoEntityHashCache(important_objID.top().key);
		important_objID.pop();
	}
}

void
Database::build_CacheOfSub2values()
{
	cout << "now add cache of subID2values..." << endl;
	while (!important_subID.empty())
	{
		this->kvstore->AddIntoSubCache(important_subID.top().key);
		this->kvstore->AddIntoEntityUBCache(important_subID.top().key);
		this->kvstore->AddIntoEntityHashCache(important_subID.top().key);
		important_subID.pop();
	}
}

void
Database::get_important_subID()
{
	while(!important_subID.empty()) important_subID.pop();
	unsigned now_total_size = 0;
	const string invalid = "";
	const unsigned max_total_size = 2000000000;//2G
	std::priority_queue <KEY_SIZE_VALUE, deque<KEY_SIZE_VALUE>, greater<KEY_SIZE_VALUE> > rubbish;
	while(!rubbish.empty()) rubbish.pop();
	// a sub who has largest degree with important pre is important subs
	for(TYPE_ENTITY_LITERAL_ID i = 0; i < limitID_entity; ++i)
	{
		unsigned _value = 0;
		unsigned _size = 0;
		if (this->kvstore->getEntityByID(i) == invalid) continue;	
		_size = this->kvstore->getSubListSize(i);
		if (!VList::isLongList(_size) || _size >= max_total_size) continue; // only long list need to be stored in cache

		for(unsigned j = 0; j < important_preID.size(); ++j)
		{
			_value += this->kvstore->getSubjectPredicateDegree(i, j);
		}
		if (_size + now_total_size < max_total_size)
		{
			important_subID.push(KEY_SIZE_VALUE(i, _size, _value));
			now_total_size += _size;
		}
		else
		{
			if (important_subID.empty()) continue;
			if (_value > important_subID.top().value)
			{
				while (now_total_size + _size >= max_total_size)
				{
					if (important_subID.top().value >= _value) break;
					rubbish.push(important_subID.top());
					now_total_size -= important_subID.top().size;
					important_subID.pop();
				}
				if (now_total_size + _size < max_total_size)
				{
					now_total_size += _size;
					important_subID.push(KEY_SIZE_VALUE(i, _size, _value));
				}
				while (!rubbish.empty())
				{
					if (now_total_size + rubbish.top().size < max_total_size)
					{
						now_total_size += rubbish.top().size;
						important_subID.push(rubbish.top());
					}
					rubbish.pop();
				}
			}
		}
	}
	cout << "finish getting important subID, the cache size is " << now_total_size << endl;
}

void
Database::get_important_objID()
{
	while(!important_objID.empty()) important_objID.pop();
	unsigned now_total_size = 0;
	const unsigned max_total_size = 2000000000;//2G
	const string invalid = "";
	std::priority_queue <KEY_SIZE_VALUE, deque<KEY_SIZE_VALUE>, greater<KEY_SIZE_VALUE> > rubbish;
	while(!rubbish.empty()) rubbish.pop();
	// a sub who has largest degree with important pre is important subs
	for(TYPE_ENTITY_LITERAL_ID i = 0; i < limitID_literal; ++i)
	{
		unsigned _value = 0;
		unsigned _size;
		string _tmp;
		if (i < limitID_entity) _tmp = this->kvstore->getEntityByID(i);
		else _tmp = this->kvstore->getLiteralByID(i);
		if (_tmp == invalid) continue;

		_size = this->kvstore->getObjListSize(i);
		if (!VList::isLongList(_size) || _size >= max_total_size) continue; // only long list need to be stored in cache
		
		for(unsigned j = 0; j < important_preID.size(); ++j)
		{
			_value += this->kvstore->getObjectPredicateDegree(i, j);
		}
		
		if (_size + now_total_size < max_total_size)
		{
			important_objID.push(KEY_SIZE_VALUE(i, _size, _value));
			now_total_size += _size;
		}
		else
		{
			if (important_objID.empty()) continue;
			if (_value > important_objID.top().value)
			{
				while (now_total_size + _size >= max_total_size)
				{
					if (important_objID.top().value >= _value) break;
					rubbish.push(important_objID.top());
					now_total_size -= important_objID.top().size;
					important_objID.pop();
				}
				if (now_total_size + _size < max_total_size)
				{
					now_total_size += _size;
					important_objID.push(KEY_SIZE_VALUE(i, _size, _value));
				}
				while (!rubbish.empty())
				{
					if (now_total_size + rubbish.top().size < max_total_size)
					{
						now_total_size += rubbish.top().size;
						important_objID.push(rubbish.top());
					}
					rubbish.pop();
				}
			}
		}
	}
	cout << endl;
	cout << "finish getting important objID, the cache size is " << now_total_size << endl;
}

void
Database::get_important_preID()
{
	important_preID.clear();
	unsigned max_degree = 0;
	for(TYPE_PREDICATE_ID i = 0; i < limitID_predicate; ++i)
		if (pre2num[i] > max_degree)
			max_degree = pre2num[i];
	unsigned limit_degree = max_degree / 2;
	for(TYPE_PREDICATE_ID i = 0; i < limitID_predicate; ++i)
		if (pre2num[i] > limit_degree)
			important_preID.push_back(i);
}

void
Database::load_important_obj2values()
{
	cout << "get important objID..." << endl;
	this->get_important_objID();

	this->build_CacheOfObj2values();
}
void
Database::load_important_sub2values()
{
	cout << "get important subID..." << endl;
	this->get_important_subID();

	this->build_CacheOfSub2values();
}

void 
Database::load_candidate_pre2values()
{
	cout << "get candidate preID..." << endl;
	this->get_candidate_preID();

	this->build_CacheOfPre2values();
}

void
Database::get_candidate_preID()
{
	//cout << "now add cache of preID2values..." << endl;
	/*for(int i = 0; i < important_preID.size(); ++i)
	{
		unsigned _size = this->kvstore->getPreListSize(important_preID[i]);
		if (now_size + _size >= max_total_size) continue;
		now_size += _size;
		this->kvstore->AddIntoPreCache(important_preID[i]);
	}*/
	unsigned now_total_size = 0;
	const unsigned max_total_size = 2000000000;//2G
//	std::priority_queue <KEY_SIZE_VALUE> candidate_preID;
	std::priority_queue <KEY_SIZE_VALUE, deque<KEY_SIZE_VALUE>, greater<KEY_SIZE_VALUE> > rubbish;
	while(!rubbish.empty()) rubbish.pop();
	while(!candidate_preID.empty()) candidate_preID.pop();
	for(TYPE_PREDICATE_ID i = 0; i < limitID_predicate; ++i)
	{
		unsigned _value = 0;
		unsigned _size;
		
		_size = this->kvstore->getPreListSize(i);
		
		if (!VList::isLongList(_size) || _size >= max_total_size) continue; // only long list need to be stored in cache

		_value = pre2num[i];
		if (_value == 0) continue;

		if (_size + now_total_size < max_total_size)
		{
			candidate_preID.push(KEY_SIZE_VALUE(i, _size, _value));
			now_total_size += _size;
		}
		else
		{
			if (candidate_preID.empty()) continue;
			if (_value > candidate_preID.top().value)
			{
				while (now_total_size + _size >= max_total_size)
				{
					if (candidate_preID.top().value >= _value) break;
					rubbish.push(candidate_preID.top());
					now_total_size -= candidate_preID.top().size;
					candidate_preID.pop();
				}
				if (now_total_size + _size < max_total_size)
				{
					now_total_size += _size;
					candidate_preID.push(KEY_SIZE_VALUE(i, _size, _value));
				}
				while (!rubbish.empty())
				{
					if (now_total_size + rubbish.top().size < max_total_size)
					{
						now_total_size += rubbish.top().size;
						candidate_preID.push(rubbish.top());
					}
					rubbish.pop();
				}
			}
		}
	}
	cout << "finish getting candidate preID, the size is " << now_total_size << endl;
}

void
Database::load_cache()
{
	// get important pre ID
	// a pre whose degree is more than 50% of max pre degree is important pre
	cout << "get important pre ID" << endl;
	this->get_important_preID();
	cout << "total preID num is " << pre_num << endl;
	cout << "important pre ID is: ";
	for(int i = 0; i < important_preID.size(); ++i)
		cout << important_preID[i] << ' ';
	cout << endl;
	this->load_candidate_pre2values();
	this->load_important_sub2values();
	this->load_important_obj2values();
	
	long t0 = Util::get_cur_time();
	vector<StringIndexFile*> indexfile = this->stringindex->get_three_StringIndexFile();

	StringIndexFile* 	entity = indexfile[0];
	StringIndexFile* 	literal = indexfile[1];
	StringIndexFile* 	predicate = indexfile[2];

	struct stat statbuf;
	int fd;
	char tmp;
	long end;
	
	stat((entity->get_loc() + "value").c_str(), &statbuf);
	fd = open((entity->get_loc() + "value").c_str(), O_RDONLY);
	entity->mmapLength = (statbuf.st_size/4096 + 1)*4096;
	entity->Mmap = (char*)mmap(NULL, entity->mmapLength, PROT_READ, MAP_POPULATE|MAP_SHARED, fd, 0);
	close(fd);
	end = entity->mmapLength - 4096;
	for (long off = 0; off < end; off += 4096)
	{	
		tmp = entity->Mmap[off];
	}
	stat((literal->get_loc() + "value").c_str(), &statbuf);
	fd = open((literal->get_loc() + "value").c_str(), O_RDONLY);
	literal->mmapLength = (statbuf.st_size / 4096 + 1) * 4096;
	literal->Mmap = (char*)mmap(NULL, literal->mmapLength, PROT_READ, MAP_POPULATE | MAP_SHARED , fd, 0);
	close(fd);
	end = literal->mmapLength - 4096;
	for (long off = 0; off < end; off += 4096)
	{
		tmp = literal->Mmap[off];
	}
	stat((predicate->get_loc() + "value").c_str(), &statbuf);
	fd = open((predicate->get_loc() + "value").c_str(), O_RDONLY);
	predicate->mmapLength = (statbuf.st_size / 4096 + 1) * 4096;
	predicate->Mmap = (char*)mmap(NULL, predicate->mmapLength, PROT_READ, MAP_POPULATE | MAP_SHARED, fd, 0);
	close(fd);
	end = predicate->mmapLength - 4096;
	for (long off = 0; off < end; off += 4096)
	{
		tmp = predicate->Mmap[off];
	}
	cout << "Value File Preload used " << Util::get_cur_time() - t0 << " ms" << endl;
}

string
Database::getStorePath()
{
	return this->store_path;
}

string
Database::getIDTuplesFile()
{
	return this->getStorePath() + "/" + this->id_tuples_file;
}

string
Database::getSixTuplesFile()
{
	return this->getStorePath() + "/" + this->six_tuples_file;
}

void
Database::initIDinfo()
{
	//NOTICE:keep that limit-1 the maxium using ID
	this->free_id_file_entity = this->getStorePath() + "/freeEntityID.dat";
	this->limitID_entity = 0;
	this->freelist_entity = NULL;

	this->free_id_file_literal = this->getStorePath() + "/freeLiteralID.dat";
	this->limitID_literal = 0;
	this->freelist_literal = NULL;

	this->free_id_file_predicate = this->getStorePath() + "/freePredicateID.dat";
	this->limitID_predicate = 0;
	this->freelist_predicate = NULL;
}

void
Database::resetIDinfo()
{
	this->initIDinfo();
}

void
Database::readIDinfo()
{
	this->initIDinfo();

	FILE* fp = NULL;
	//int t = -1;
	TYPE_ENTITY_LITERAL_ID t = INVALID_ENTITY_LITERAL_ID;
	BlockInfo* bp = NULL;

	fp = fopen(this->free_id_file_entity.c_str(), "r");
	if (fp == NULL)
	{
		cout << "read entity id info error" << endl;
		return;
	}
	//QUERY:this will reverse the original order, if change?
	//Notice that if we cannot ensure that IDs are uporder and continuous, we can
	//not keep an array for IDs like _entity_bitset
	BlockInfo *tmp = NULL, *cur = NULL;
	fread(&(this->limitID_entity), sizeof(int), 1, fp);
	fread(&t, sizeof(int), 1, fp);
	while (!feof(fp))
	{
		//if(t == 14912)
		//{
		//cout<<"Database::readIDinfo() - get 14912"<<endl;
		//}
		//bp = new BlockInfo(t, this->freelist_entity);
		//this->freelist_entity = bp;
		tmp = new BlockInfo(t);
		if (cur == NULL)
		{
			this->freelist_entity = cur = tmp;
		}
		else
		{
			cur->next = tmp;
			cur = tmp;
		}
		fread(&t, sizeof(int), 1, fp);
	}
	fclose(fp);
	fp = NULL;


	fp = fopen(this->free_id_file_literal.c_str(), "r");
	if (fp == NULL)
	{
		cout << "read literal id info error" << endl;
		return;
	}

	fread(&(this->limitID_literal), sizeof(int), 1, fp);
	fread(&t, sizeof(int), 1, fp);
	while (!feof(fp))
	{
		bp = new BlockInfo(t, this->freelist_literal);
		this->freelist_literal = bp;
		fread(&t, sizeof(int), 1, fp);
	}
	fclose(fp);
	fp = NULL;

	fp = fopen(this->free_id_file_predicate.c_str(), "r");
	if (fp == NULL)
	{
		cout << "read predicate id info error" << endl;
		return;
	}
	fread(&(this->limitID_predicate), sizeof(int), 1, fp);
	fread(&t, sizeof(int), 1, fp);
	while (!feof(fp))
	{
		bp = new BlockInfo(t, this->freelist_predicate);
		this->freelist_predicate = bp;
		fread(&t, sizeof(int), 1, fp);
	}
	fclose(fp);
	fp = NULL;
}

string
Database::getDBInfoFile()
{
	return this->getStorePath() + "/" + this->db_info_file;
}

bool
Database::loadDBInfoFile()
{
	FILE* filePtr = fopen(this->getDBInfoFile().c_str(), "rb");

	if (filePtr == NULL)
	{
		cout << "error, can not open db info file. @Database::loadDBInfoFile" << endl;
		return false;
	}

	fseek(filePtr, 0, SEEK_SET);

	fread(&this->triples_num, sizeof(int), 1, filePtr);
	fread(&this->entity_num, sizeof(int), 1, filePtr);
	fread(&this->sub_num, sizeof(int), 1, filePtr);
	fread(&this->pre_num, sizeof(int), 1, filePtr);
	fread(&this->literal_num, sizeof(int), 1, filePtr);
	fread(&this->encode_mode, sizeof(int), 1, filePtr);
	fclose(filePtr);

	//Util::triple_num = this->triples_num;
	//Util::pre_num = this->pre_num;
	//Util::entity_num = this->entity_num;
	//Util::literal_num = this->literal_num;

	return true;
}

void 
Database::load_entity2id(int _mode)
{
	this->kvstore->open_entity2id(_mode);
}

void 
Database::load_id2entity(int _mode)
{
	this->kvstore->open_id2entity(_mode);
}

void 
Database::load_literal2id(int _mode)
{
	this->kvstore->open_literal2id(_mode);
}

void 
Database::load_id2literal(int _mode)
{
	this->kvstore->open_id2literal(_mode);
}

void 
Database::load_predicate2id(int _mode)
{
	this->kvstore->open_predicate2id(_mode);
}

void 
Database::load_id2predicate(int _mode)
{
	this->kvstore->open_id2predicate(_mode);
}

void 
Database::load_sub2values(int _mode)
{
	this->kvstore->open_subID2values(_mode);
}

void 
Database::load_obj2values(int _mode)
{
	this->kvstore->open_objID2values(_mode);
}

void 
Database::load_pre2values(int _mode)
{
	this->kvstore->open_preID2values(_mode);
}

void 
Database::load_entity2UBvalues(int _mode)
{
	this->kvstore->open_entityID2UBvalues(_mode);
}

void 
Database::load_entity2Hashvalues(int _mode)
{
	this->kvstore->open_entityID2Hashvalues(_mode);
}

void 
Database::check()
{
cout<<"triple num: "<<this->triples_num<<endl;
cout<<"pre num: "<<this->pre_num<<endl;
cout<<"entity num: "<<this->entity_num<<endl;
cout<<"literal num: "<<this->literal_num<<endl;
}

bool
Database::build(const string& _rdf_file)
{
	//NOTICE: it is not necessary to use multiple threads here, because some process may rely on others
	//In addition, the memory is a bootleneck and it is dangerous to build serveral indices at a time
	//For example, if we build id2string indices using different threads, they 
	//all have to scan the dataset and only use a part of the data, which may be costly
	//Besides, build process is already very fast, able to build freebase in 12h

	//manage the id for a new database
	this->resetIDinfo();

	string ret = Util::getExactPath(_rdf_file.c_str());
	long tv_build_begin = Util::get_cur_time();

	//string store_path = this->name;
	Util::create_dir(this->store_path);

	string kv_store_path = store_path + "/kv_store";
	Util::create_dir(kv_store_path);

	//string vstree_store_path = store_path + "/vs_store";
	//Util::create_dir(vstree_store_path);

	string stringindex_store_path = store_path + "/stringindex_store";
	Util::create_dir(stringindex_store_path);

	string update_log_path = this->store_path + '/' + this->update_log;
	Util::create_file(update_log_path);
	string update_log_since_backup = this->store_path + '/' + this->update_log_since_backup;
	Util::create_file(update_log_since_backup);

	cout << "begin encode RDF from : " << ret << " ..." << endl;

	//BETTER+TODO:now require that dataset size < memory
	//to support really larger datasets, divide and insert into B+ tree and VStree
	//(read the value, add list and set; update the signature, remove and reinsert)
	//the query process is nearly the same
	//QUERY:build each index in one thread to speed up, but the sorting case?
	//one sorting order for several indexes

	// to be switched to new encodeRDF method.
	//    this->encodeRDF(ret);
	if (!this->encodeRDF_new(ret))	//<-- this->kvstore->id2* trees are closed
	{
		return false;
	}
	cout << "finish encode." << endl;

	//cout<<"test kv"<<this->kvstore->getIDByPredicate("<contain>")<<endl;

	//this->kvstore->flush();
	delete this->kvstore;
	this->kvstore = NULL;
	//sync();
	//cout << "sync kvstore" << endl;
	//this->kvstore->release();
	//cout<<"release kvstore"<<endl;

	//long before_vstree = Util::get_cur_time();
	//(this->kvstore)->open();
	//string _entry_file = this->getSignatureBFile();

	//cout << "begin build VS-Tree on " << ret << "..." << endl;
	//NOTICE: we can use larger buffer for vstree in building process, because it does not compete with others
	//we only need to build vstree in this phase(no need for id tuples anymore)
	//TODO: acquire this arg from memory manager
	//unsigned vstree_cache_size = 4 * LRUCache::DEFAULT_CAPACITY;
	//BETTER: we should set the parameter according to current memory usage
	//(this->vstree)->buildTree(_entry_file, vstree_cache_size);

	long tv_build_end = Util::get_cur_time();

	//cout << "after build vstree, used " << (tv_build_end - before_vstree) << "ms." << endl;
	cout << "after build, used " << (tv_build_end - tv_build_begin) << "ms." << endl;
	cout << "finish build VS-Tree." << endl;

	cout << "finish sub2id pre2id obj2id" << endl;
	cout << "tripleNum is " << this->triples_num << endl;
	cout << "entityNum is " << this->entity_num << endl;
	cout << "preNum is " << this->pre_num << endl;
	cout << "literalNum is " << this->literal_num << endl;

	//this->vstree->saveTree();
	//delete this->vstree;
	//this->vstree = NULL;
	//sync();
	//cout << "sync vstree" << endl;

	//TODO: use fopen w+ to remove signature.binary file
	//string cmd = "rm -rf " + _entry_file;
	//system(cmd.c_str());
	//cout << "signature file removed" << endl;

	return true;
}

bool
Database::load()
{
	if(this->if_loaded)
	{
		return true;
	}

	//TODO: acquire this arg from memory manager
	//BETTER: get return value from subthread(using ref or file as hub)
	unsigned vstree_cache = LRUCache::DEFAULT_CAPACITY;
	bool flag;
#ifndef THREAD_ON
	//flag = (this->vstree)->loadTree(vstree_cache);
	//if (!flag)
	//{
		//cout << "load tree error. @Database::load()" << endl;
		//return false;
	//}
	(this->kvstore)->open();
#else
	//thread vstree_thread(&Database::load_vstree, this, vstree_cache);
	int kv_mode = KVstore::READ_WRITE_MODE;
	thread entity2id_thread(&Database::load_entity2id, this, kv_mode);
	thread id2entity_thread(&Database::load_id2entity, this, kv_mode);
	thread literal2id_thread(&Database::load_literal2id, this, kv_mode);
	thread id2literal_thread(&Database::load_id2literal, this, kv_mode);
	thread predicate2id_thread(&Database::load_predicate2id, this, kv_mode);
#ifndef ONLY_READ
	thread id2predicate_thread(&Database::load_id2predicate, this, kv_mode);
#endif
	thread sub2values_thread(&Database::load_sub2values, this, kv_mode);
	thread obj2values_thread(&Database::load_obj2values, this, kv_mode);
	thread pre2values_thread(&Database::load_pre2values, this, kv_mode);
	thread entity2UBvalues_thread(&Database::load_entity2UBvalues, this, kv_mode);
	thread entity2Hashvalues_thread(&Database::load_entity2Hashvalues, this, kv_mode);
#endif

	//this is very fast
	flag = this->loadDBInfoFile();
	if (!flag)
	{
		cout << "load database info error. @Database::load()" << endl;
		return false;
	}

	if(!(this->kvstore)->load_trie(kv_mode))
		return false;

	this->stringindex->SetTrie(this->kvstore->getTrie());
	//NOTICE: we should also run some heavy work in the main thread
	this->stringindex->load();
	this->readIDinfo();

#ifdef THREAD_ON
	pre2values_thread.join();
#endif
	this->setPreMap();

#ifdef THREAD_ON
	id2entity_thread.join();
	id2literal_thread.join();
#endif

	//BETTER: if we set string buffer using string index instead of B+Tree, then we can
	//avoid to load id2entity and id2literal in ONLY_READ mode

	//generate the string buffer for entity and literal, no need for predicate
	//NOTICE:the total string size should not exceed 20G, assume that most strings length < 500
	//too many empty between entity and literal, so divide them

	//this->setStringBuffer();

	//NOTICE: we should build string buffer from kvstore, not string index
	//Because when searching in string index, it will first check if in buffer(but the buffer is being built)

#ifndef ONLY_READ
#ifdef THREAD_ON
	id2predicate_thread.join();
#endif
#endif

#ifdef THREAD_ON
	entity2id_thread.join();
	literal2id_thread.join();
	predicate2id_thread.join();
	sub2values_thread.join();
	obj2values_thread.join();
	entity2UBvalues_thread.join();
	entity2Hashvalues_thread.join();
	//wait for vstree thread
	//vstree_thread.join();
#endif
	//load cache of sub2values and obj2values
	this->load_cache();
	
	//warm up always as finishing build(), to utilize the system buffer
	//this->warmUp();
	//DEBUG:the warmUp() calls query(), which will also output results, this is not we want

	// Load trie

	/*if (trie != NULL)
		delete trie;
	trie = new Trie;

	string dictionary_path = store_path + "/dictionary.dc";
	if (!trie->LoadTrie(dictionary_path))
	{
		return false;
	}*/

	this->if_loaded = true;

	cout << "finish load" << endl;

	//BETTER: for only-read application(like endpoint), 3 id2values trees can be closed now
	//and we should load all trees on only READ mode

	//HELP: just for checking infos(like kvstore)
	check();

#ifdef ONLY_READ
	this->kvstore->close_id2entity();
	this->kvstore->close_id2literal();
#endif

	return true;
}


TYPE_ENTITY_LITERAL_ID
Database::allocEntityID()
{
	//int t;
	TYPE_ENTITY_LITERAL_ID t = INVALID_ENTITY_LITERAL_ID;

	if (this->freelist_entity == NULL)
	{
		t = this->limitID_entity++;
		if (this->limitID_entity >= Util::LITERAL_FIRST_ID)
		{
			cout << "fail to alloc id for entity" << endl;
			//return -1;
			return INVALID;
		}
	}
	else
	{
		t = this->freelist_entity->num;
		BlockInfo* op = this->freelist_entity;
		this->freelist_entity = this->freelist_entity->next;
		delete op;
	}

	this->entity_num++;
	return t;
}

TYPE_ENTITY_LITERAL_ID
Database::allocLiteralID()
{
	//int t;
	TYPE_ENTITY_LITERAL_ID t = INVALID_ENTITY_LITERAL_ID;

	if (this->freelist_literal == NULL)
	{
		t = this->limitID_literal++;
		if (this->limitID_literal >= Util::LITERAL_FIRST_ID)
		{
			cout << "fail to alloc id for literal" << endl;
			//return -1;
			return INVALID;
		}
	}
	else
	{
		t = this->freelist_literal->num;
		BlockInfo* op = this->freelist_literal;
		this->freelist_literal = this->freelist_literal->next;
		delete op;
	}

	this->literal_num++;
	return t + Util::LITERAL_FIRST_ID;
}

TYPE_PREDICATE_ID
Database::allocPredicateID()
{
	//int t;
	TYPE_PREDICATE_ID t = INVALID_PREDICATE_ID;

	if (this->freelist_predicate == NULL)
	{
		t = this->limitID_predicate++;
		if (this->limitID_predicate >= Util::LITERAL_FIRST_ID)
		{
			cout << "fail to alloc id for predicate" << endl;
			//WARN:if pid is changed to unsigned type, this must be changed
			return -1;
		}
	}
	else
	{
		t = this->freelist_predicate->num;
		BlockInfo* op = this->freelist_predicate;
		this->freelist_predicate = this->freelist_predicate->next;
		delete op;
	}

	this->pre_num++;
	return t;
}

void 
Database::readIDTuples(ID_TUPLE*& _p_id_tuples)
{
	_p_id_tuples = NULL;
	string fname = this->getIDTuplesFile();
	FILE* fp = fopen(fname.c_str(), "rb");
	if(fp == NULL)
	{
		cout<<"error in Database::readIDTuples() -- unable to open file "<<fname<<endl;
		return;
	}

	//NOTICE: avoid to break the unsigned limit, size_t is used in Linux C
	//size_t means long unsigned int in 64-bit machine
	//unsigned long total_num = this->triples_num * 3;
	//_p_id_tuples = new TYPE_ENTITY_LITERAL_ID[total_num];
	_p_id_tuples = new ID_TUPLE[this->triples_num];
	fread(_p_id_tuples, sizeof(ID_TUPLE), this->triples_num, fp);

	fclose(fp);
	//NOTICE: choose to empty the file or not
	Util::empty_file(fname.c_str());

	//return NULL;
}

void
Database::build_s2xx(ID_TUPLE* _p_id_tuples)
{
	//NOTICE: STL sort() is generally fatser than C qsort, especially when qsort is very slow
	//STL sort() not only use qsort algorithm, it can also choose heap-sort method
#ifndef PARALLEL_SORT
	sort(_p_id_tuples, _p_id_tuples + this->triples_num, Util::spo_cmp_idtuple);
#else
	omp_set_num_threads(thread_num);
	__gnu_parallel::sort(_p_id_tuples, _p_id_tuples + this->triples_num, Util::spo_cmp_idtuple);
#endif
	//qsort(_p_id_tuples, this->triples_num, sizeof(int*), Util::_spo_cmp);

	//remove duplicates from the id tables
	TYPE_TRIPLE_NUM j = 1;
	for(TYPE_TRIPLE_NUM i = 1; i < this->triples_num; ++i)
	{
		if(!Util::equal(_p_id_tuples[i], _p_id_tuples[i-1]))
		{
			_p_id_tuples[j] = _p_id_tuples[i];
			++j;
		}
	}
	this->triples_num = j;
	
	this->kvstore->build_subID2values(_p_id_tuples, this->triples_num, this->entity_num);
	this->kvstore->build_subID2UBvalues(_p_id_tuples, this->triples_num, this->entity_num);
	this->kvstore->build_subID2Hashvalues(_p_id_tuples, this->triples_num, this->entity_num);
	//save all entity_signature into binary file
	//string sig_binary_file = this->getSignatureBFile();
	//FILE* sig_fp = fopen(sig_binary_file.c_str(), "wb");
	//if (sig_fp == NULL) 
	//{
		//cout << "Failed to open : " << sig_binary_file << endl;
		//return;
	//}

	//NOTICE:in build process, all IDs are continuous growing
	//EntityBitSet tmp_bitset;
	//tmp_bitset.reset();
	//for(TYPE_ENTITY_LITERAL_ID i = 0; i < this->entity_num; ++i)
	//{
		//SigEntry* sig = new SigEntry(EntitySig(tmp_bitset), -1);
		//fwrite(sig, sizeof(SigEntry), 1, sig_fp);
		//delete sig;
	//}

	//TYPE_ENTITY_LITERAL_ID prev_entity_id = INVALID_ENTITY_LITERAL_ID;
	//int prev_entity_id = -1;
	
	//NOTICE: i*3 + j maybe break the unsigned limit
	//for (unsigned long i = 0; i < this->triples_num; ++i)
	//for (TYPE_TRIPLE_NUM i = 0; i < this->triples_num; ++i)
	//{
		//TYPE_ENTITY_LITERAL_ID subid = _p_id_tuples[i].subid;
		//TYPE_PREDICATE_ID preid = _p_id_tuples[i].preid;
		//TYPE_ENTITY_LITERAL_ID objid = _p_id_tuples[i].objid;
		////TYPE_ENTITY_LITERAL_ID subid = _p_id_tuples[i*3+0];
		////TYPE_PREDICATE_ID preid = _p_id_tuples[i*3+1];
		////TYPE_ENTITY_LITERAL_ID objid = _p_id_tuples[i*3+2];
		//if(subid != prev_entity_id)
		//{
			//if(prev_entity_id != INVALID_ENTITY_LITERAL_ID)
			////if(prev_entity_id != -1)
			//{
//#ifdef DEBUG
				////if(prev_entity_id == 13)
				////{
					////cout<<"yy: "<<Signature::BitSet2str(tmp_bitset)<<endl;
				////}
//#endif
				////NOTICE: we must do twice, we need to locate on the same entry to deal, so we must place in order
				//SigEntry* sig = new SigEntry(EntitySig(tmp_bitset), prev_entity_id);
				////write the sig entry
				//fseek(sig_fp, sizeof(SigEntry) * prev_entity_id, SEEK_SET);
				//fwrite(sig, sizeof(SigEntry), 1, sig_fp);
				////_all_bitset |= *_entity_bitset[i];
				//delete sig;
			//}
			//prev_entity_id = subid;
			//tmp_bitset.reset();
			//Signature::encodeEdge2Entity(tmp_bitset, preid, objid, Util::EDGE_OUT);
			////Signature::encodePredicate2Entity(preid, _tmp_bitset, Util::EDGE_OUT);
			////Signature::encodeStr2Entity(objid, _tmp_bitset);
		//}
		//else
		//{
			//Signature::encodeEdge2Entity(tmp_bitset, preid, objid, Util::EDGE_OUT);
		//}
	//}

	////NOTICE: remember to write the last entity's signature
	//if(prev_entity_id != INVALID_ENTITY_LITERAL_ID)
	////if(prev_entity_id != -1)
	//{
		//SigEntry* sig = new SigEntry(EntitySig(tmp_bitset), prev_entity_id);
		////write the sig entry
		//fseek(sig_fp, sizeof(SigEntry) * prev_entity_id, SEEK_SET);
		//fwrite(sig, sizeof(SigEntry), 1, sig_fp);
		////_all_bitset |= *_entity_bitset[i];
		//delete sig;
	//}

	//fclose(sig_fp);
}

void
Database::build_o2xx(ID_TUPLE* _p_id_tuples)
{
#ifndef PARALLEL_SORT
	sort(_p_id_tuples, _p_id_tuples + this->triples_num, Util::ops_cmp_idtuple);
#else
	omp_set_num_threads(thread_num);
	__gnu_parallel::sort(_p_id_tuples, _p_id_tuples + this->triples_num, Util::ops_cmp_idtuple);
#endif
	//qsort(_p_id_tuples, this->triples_num, sizeof(int*), Util::_ops_cmp);
	this->kvstore->build_objID2values(_p_id_tuples, this->triples_num, this->entity_num, this->literal_num);
	this->kvstore->build_objID2UBvalues(_p_id_tuples, this->triples_num);
	this->kvstore->build_objID2Hashvalues(_p_id_tuples, this->triples_num);
	//save all entity_signature into binary file
	//string sig_binary_file = this->getSignatureBFile();
	////NOTICE: this is different from build_s2xx, the file already exists
	//FILE* sig_fp = fopen(sig_binary_file.c_str(), "rb+");
	//if (sig_fp == NULL) 
	//{
		//cout << "Failed to open : " << sig_binary_file << endl;
		//return;
	//}

	////NOTICE:in build process, all IDs are continuous growing
	////TODO:use unsigned for type and -1 should be changed
	//TYPE_ENTITY_LITERAL_ID prev_entity_id = INVALID_ENTITY_LITERAL_ID;
	////int prev_entity_id = -1;
	//EntityBitSet tmp_bitset;

	////NOTICE: i*3 + j maybe break the unsigned limit
	////for (unsigned long i = 0; i < this->triples_num; ++i)
	//for (TYPE_TRIPLE_NUM i = 0; i < this->triples_num; ++i)
	//{
		//TYPE_ENTITY_LITERAL_ID subid = _p_id_tuples[i].subid;
		//TYPE_PREDICATE_ID preid = _p_id_tuples[i].preid;
		//TYPE_ENTITY_LITERAL_ID objid = _p_id_tuples[i].objid;
		////TYPE_ENTITY_LITERAL_ID subid = _p_id_tuples[i*3+0];
		////TYPE_PREDICATE_ID preid = _p_id_tuples[i*3+1];
		////TYPE_ENTITY_LITERAL_ID objid = _p_id_tuples[i*3+2];


		//if(Util::is_literal_ele(objid))
		//{
			//continue;
		//}

		//if(objid != prev_entity_id)
		//{
			////if(prev_entity_id != -1)
			//if(prev_entity_id != INVALID_ENTITY_LITERAL_ID)
			//{
				////NOTICE: we must do twice, we need to locate on the same entry to deal, so we must place in order
				//fseek(sig_fp, sizeof(SigEntry) * prev_entity_id, SEEK_SET);
				//SigEntry* old_sig = new SigEntry();
				//fread(old_sig, sizeof(SigEntry), 1, sig_fp);
//#ifdef DEBUG
				////cout<<"to write a signature: "<<prev_entity_id<<endl;
				////cout<<prev_entity_id<<endl;
				////if(prev_entity_id == 13)
				////{
					////cout<<"yy: "<<Signature::BitSet2str(tmp_bitset)<<endl;
				////}
//#endif
				//tmp_bitset |= old_sig->getEntitySig().entityBitSet;
				//delete old_sig;
				
//#ifdef DEBUG
				////if(prev_entity_id == 13)
				////{
					////cout<<"yy: "<<Signature::BitSet2str(tmp_bitset)<<endl;
				////}
//#endif
				
					////write the sig entry
				//SigEntry* sig = new SigEntry(EntitySig(tmp_bitset), prev_entity_id);
				//fseek(sig_fp, sizeof(SigEntry) * prev_entity_id, SEEK_SET);
				//fwrite(sig, sizeof(SigEntry), 1, sig_fp);
				////_all_bitset |= *_entity_bitset[i];
				//delete sig;
			//}

//#ifdef DEBUG
			////cout<<"now is a new signature: "<<objid<<endl;
//#endif

			//prev_entity_id = objid;
			//tmp_bitset.reset();
			////cout<<"bitset reset"<<endl;
			//Signature::encodeEdge2Entity(tmp_bitset, preid, subid, Util::EDGE_IN);
			////cout<<"edge encoded"<<endl;
			////Signature::encodePredicate2Entity(preid, _tmp_bitset, Util::EDGE_IN);
			////Signature::encodeStr2Entity(subid, _tmp_bitset);
		//}
		//else
		//{
			////cout<<"same signature: "<<objid<<" "<<preid<<" "<<subid<<endl;
			//Signature::encodeEdge2Entity(tmp_bitset, preid, subid, Util::EDGE_IN);
			////cout<<"edge encoded"<<endl;
		//}
	//}
	////cout<<"loop ended!"<<endl;

	////NOTICE: remember to write the last entity's signature
	//if(prev_entity_id != INVALID_ENTITY_LITERAL_ID)
	////if(prev_entity_id != -1)
	//{
		////cout<<"to write the last signature"<<endl;

		//fseek(sig_fp, sizeof(SigEntry) * prev_entity_id, SEEK_SET);
		//SigEntry* old_sig = new SigEntry();
		//fread(old_sig, sizeof(SigEntry), 1, sig_fp);
		//tmp_bitset |= old_sig->getEntitySig().entityBitSet;
		//delete old_sig;
		////write the sig entry
		//SigEntry* sig = new SigEntry(EntitySig(tmp_bitset), prev_entity_id);
		//fseek(sig_fp, sizeof(SigEntry) * prev_entity_id, SEEK_SET);
		//fwrite(sig, sizeof(SigEntry), 1, sig_fp);
		////_all_bitset |= *_entity_bitset[i];
		//delete sig;
	//}

	//fclose(sig_fp);
}

void
Database::build_p2xx(ID_TUPLE* _p_id_tuples)
{
#ifndef PARALLEL_SORT
	sort(_p_id_tuples, _p_id_tuples + this->triples_num, Util::pso_cmp_idtuple);
#else
	omp_set_num_threads(thread_num);
	__gnu_parallel::sort(_p_id_tuples, _p_id_tuples + this->triples_num, Util::pso_cmp_idtuple);
#endif
	//qsort(_p_id_tuples, this->triples_num, sizeof(int*), Util::_pso_cmp);
	this->kvstore->build_preID2values(_p_id_tuples, this->triples_num, this->pre_num);
}

//NOTICE:in here and there in the insert/delete, we may get the maxium tuples num first
//and so we can avoid the cost of memcpy(scan quickly or use wc -l)
//However, if use compressed RDF format, how can we do it fi not using parser?
//CONSIDER: just an estimated value is ok or use vector!!!(but vector also copy when enlarge)
//and read file line numbers are also costly!
bool
Database::sub2id_pre2id_obj2id_RDFintoSignature(const string _rdf_file)
{
	//NOTICE: if we keep the id_tuples always in memory, i.e. [unsigned*] each unsigned* is [3]
	//then for freebase, there is 2.5B triples. the mmeory cost of this array is 25*10^8*3*4 + 25*10^8*8 = 50G
	//
	//So I choose not to store the id_tuples in memory in this function, but to store them in file and read again after this function
	//Notice that the most memory-costly part of building process is this function, setup 6 trees together
	//later we can read the id_tuples and stored as [num][3], only cost 25*10^8*3*4 = 30G, and later we only build one tree at a time

	string fname = this->getIDTuplesFile();
	FILE* fp = fopen(fname.c_str(), "wb");
	if(fp == NULL)
	{
		cout<<"error in Database::sub2id_pre2id_obj2id() -- unable to open file to write "<<fname<<endl;
		return false;
	}
	ID_TUPLE tmp_id_tuple;
	//NOTICE: avoid to break the unsigned limit, size_t is used in Linux C
	//size_t means long unsigned int in 64-bit machine
	//fread(_p_id_tuples, sizeof(TYPE_ENTITY_LITERAL_ID), total_num, fp);

	TYPE_TRIPLE_NUM _id_tuples_size;
	{
		//initial
		//_id_tuples_max = 10 * 1000 * 1000;
		//_p_id_tuples = new TYPE_ENTITY_LITERAL_ID*[_id_tuples_max];
		//_id_tuples_size = 0;
		this->sub_num = 0;
		this->pre_num = 0;
		this->entity_num = 0;
		this->literal_num = 0;
		this->triples_num = 0;
		(this->kvstore)->open_entity2id(KVstore::CREATE_MODE);
		(this->kvstore)->open_id2entity(KVstore::CREATE_MODE);
		(this->kvstore)->open_predicate2id(KVstore::CREATE_MODE);
		(this->kvstore)->open_id2predicate(KVstore::CREATE_MODE);
		(this->kvstore)->open_literal2id(KVstore::CREATE_MODE);
		(this->kvstore)->open_id2literal(KVstore::CREATE_MODE);
		(this->kvstore)->load_trie(KVstore::CREATE_MODE);
	}

	//Util::logging("finish initial sub2id_pre2id_obj2id");
	cout << "finish initial sub2id_pre2id_obj2id" << endl;

	//BETTER?:close the stdio buffer sync??

	ifstream _fin(_rdf_file.c_str());
	if (!_fin)
	{
		cout << "sub2id&pre2id&obj2id: Fail to open : " << _rdf_file << endl;
		//exit(0);
		return false;
	}

	string _six_tuples_file = this->getSixTuplesFile();
	ofstream _six_tuples_fout(_six_tuples_file.c_str());
	if (!_six_tuples_fout)
	{
		cout << "sub2id&pre2id&obj2id: Fail to open: " << _six_tuples_file << endl;
		//exit(0);
		return false;
	}

	TripleWithObjType* triple_array = new TripleWithObjType[RDFParser::TRIPLE_NUM_PER_GROUP];

	//don't know the number of entity
	//pre allocate entitybitset_max EntityBitSet for storing signature, double the space until the _entity_bitset is used up.
	//
	//int entitybitset_max = 10000000;  //set larger to avoid the copy cost
	//int entitybitset_max = 10000;
	//EntityBitSet** _entity_bitset = new EntityBitSet*[entitybitset_max];
	//for (int i = 0; i < entitybitset_max; i++)
	//{
		//_entity_bitset[i] = new EntityBitSet();
		//_entity_bitset[i]->reset();
	//}
	//EntityBitSet _tmp_bitset;

	{
		cout << "begin build Prefix" << endl;
		long begin = Util::get_cur_time();
		ifstream _fin0(_rdf_file.c_str());
		//parse a file
		RDFParser _parser0(_fin0);

		// Initialize trie

		Trie *trie = kvstore->getTrie();
		while (true)
		{
			int parse_triple_num = 0;
			_parser0.parseFile(triple_array, parse_triple_num);
			if (parse_triple_num == 0)
			{
				break;
			}

			//Process the Triple one by one
			for (int i = 0; i < parse_triple_num; i++)
			{
				string t = triple_array[i].getSubject();
				trie->Addstring(t);
				t = triple_array[i].getPredicate();
				trie->Addstring(t);
				t = triple_array[i].getObject();
				trie->Addstring(t);
			}
		}
        cout<<"Add triples to Trie to prepare for BuildPrefix"<<endl;
		trie->BuildPrefix();
		cout << "BuildPrefix done. used" <<Util::get_cur_time() - begin<< endl;
	}

	RDFParser _parser(_fin);
	//Util::logging("==> while(true)");
	
	while (true)
	{
		int parse_triple_num = 0;

		_parser.parseFile(triple_array, parse_triple_num);

		{
			stringstream _ss;
			_ss << "finish rdfparser" << this->triples_num << endl;
			//Util::logging(_ss.str());
			cout << _ss.str() << endl;
		}
		cout << "after info in sub2id_" << endl;

		if (parse_triple_num == 0)
		{
			break;
		}

		//Process the Triple one by one
		for (int i = 0; i < parse_triple_num; i++)
		{
			//BETTER: assume that no duplicate triples in RDF for building
			//should judge first? using exist_triple() 
			//or sub triples_num in build_subID2values(judge if two neighbor triples are same)
			this->triples_num++;

			//if the _id_tuples exceeds, double the space
			//if (_id_tuples_size == _id_tuples_max)
			//{
				//TYPE_TRIPLE_NUM _new_tuples_len = _id_tuples_max * 2;
				//TYPE_ENTITY_LITERAL_ID** _new_id_tuples = new TYPE_ENTITY_LITERAL_ID*[_new_tuples_len];
				//memcpy(_new_id_tuples, _p_id_tuples, sizeof(TYPE_ENTITY_LITERAL_ID*) * _id_tuples_max);
				//delete[] _p_id_tuples;
				//_p_id_tuples = _new_id_tuples;
				//_id_tuples_max = _new_tuples_len;
			//}

			//BETTER: use 3 threads to deal with sub, obj, pre separately
			//However, the cost of new /delete threads may be high
			//We need a thread pool!

			// For subject
			// (all subject is entity, some object is entity, the other is literal)
			string _sub = triple_array[i].getSubject();
			TYPE_ENTITY_LITERAL_ID _sub_id = (this->kvstore)->getIDByEntity(_sub);
			if (_sub_id == INVALID_ENTITY_LITERAL_ID)
			//if (_sub_id == -1)
			{
				//_sub_id = this->entity_num;
				_sub_id = this->allocEntityID();
				this->sub_num++;
				//this->entity_num++;
				(this->kvstore)->setIDByEntity(_sub, _sub_id);
				(this->kvstore)->setEntityByID(_sub_id, _sub);
			}
			//  For predicate
			string _pre = triple_array[i].getPredicate();
			TYPE_PREDICATE_ID _pre_id = (this->kvstore)->getIDByPredicate(_pre);
			if (_pre_id == INVALID_PREDICATE_ID)
			//if (_pre_id == -1)
			{
				//_pre_id = this->pre_num;
				_pre_id = this->allocPredicateID();
				//this->pre_num++;
				(this->kvstore)->setIDByPredicate(_pre, _pre_id);
				(this->kvstore)->setPredicateByID(_pre_id, _pre);
			}

			//  For object
			string _obj = triple_array[i].getObject();
			//int _obj_id = -1;
			TYPE_ENTITY_LITERAL_ID _obj_id = INVALID_ENTITY_LITERAL_ID;
			// obj is entity
			if (triple_array[i].isObjEntity())
			{
				_obj_id = (this->kvstore)->getIDByEntity(_obj);
				if (_obj_id == INVALID_ENTITY_LITERAL_ID)
				//if (_obj_id == -1)
				{
					//_obj_id = this->entity_num;
					_obj_id = this->allocEntityID();
					//this->entity_num++;
					(this->kvstore)->setIDByEntity(_obj, _obj_id);
					(this->kvstore)->setEntityByID(_obj_id, _obj);
				}
			}
			//obj is literal
			if (triple_array[i].isObjLiteral())
			{
				_obj_id = (this->kvstore)->getIDByLiteral(_obj);
				if (_obj_id == INVALID_ENTITY_LITERAL_ID)
				//if (_obj_id == -1)
				{
					//_obj_id = Util::LITERAL_FIRST_ID + (this->literal_num);
					_obj_id = this->allocLiteralID();
					//this->literal_num++;
					(this->kvstore)->setIDByLiteral(_obj, _obj_id);
					(this->kvstore)->setLiteralByID(_obj_id, _obj);
					//#ifdef DEBUG
					//if(_obj == "\"Bob\"")
					//{
					//cout << "this is id for Bob: " << _obj_id << endl;
					//}
					//cout<<"literal should be bob: " << kvstore->getLiteralByID(_obj_id)<<endl;
					//cout<<"id for bob: "<<kvstore->getIDByLiteral("\"Bob\"")<<endl;
					//#endif
				}
			}

			//NOTICE: we assume that there is no duplicates in the dataset
			//if not, this->triple_num will be not right, and _p_id_tuples will save useless triples
			//However, we can not use exist_triple to detect duplicates here, because it is too time-costly

			//  For id_tuples
			//_p_id_tuples[_id_tuples_size] = new TYPE_ENTITY_LITERAL_ID[3];
			//_p_id_tuples[_id_tuples_size][0] = _sub_id;
			//_p_id_tuples[_id_tuples_size][1] = _pre_id;
			//_p_id_tuples[_id_tuples_size][2] = _obj_id;
			//_id_tuples_size++;
			tmp_id_tuple.subid = _sub_id;
			tmp_id_tuple.preid = _pre_id;
			tmp_id_tuple.objid = _obj_id;
			fwrite(&tmp_id_tuple, sizeof(ID_TUPLE), 1, fp);
			//fwrite(&_sub_id, sizeof(TYPE_ENTITY_LITERAL_ID), 1, fp);
			//fwrite(&_pre_id, sizeof(TYPE_ENTITY_LITERAL_ID), 1, fp);
			//fwrite(&_obj_id, sizeof(TYPE_ENTITY_LITERAL_ID), 1, fp);

#ifdef DEBUG_PRECISE
			////  save six tuples
				//_six_tuples_fout << _sub_id << '\t'
					//<< _pre_id << '\t'
					//<< _obj_id << '\t'
					//<< _sub << '\t'
					//<< _pre << '\t'
					//<< _obj << endl;
#endif

			//NOTICE: the memory cost maybe too larger if combine teh below process here
			//we can do below after this function or after all B+trees are built and closed
			//and we can decide the length of signature according to entity num then
			//1. after all b+trees: empty id_tuples and only open id2string, reload rdf file and encode(using string for entity/literal)
			//
			//2. after this function or after all B+trees: close others and only use id_tuples to encode(no need to read file again, which is too costly)
			//not encoded with string but all IDs(not using encode for string regex matching, then this is ok!)
			//Because we encode with ID, then Signature has to be changed(and dynamic sig length)
			//use same encode strategy for entity/literal/predicate, and adjust the rate of the 3 parts according to real case
			//What is more, if the system memory is enough(precisely, the memory you want to assign to gstore - to vstree/entity_sig_array), 
			//we can also set the sig length larger(which should be included in config file)

			//_entity_bitset is used up, double the space
			//if (this->entity_num >= entitybitset_max)
			//{
				////cout<<"to double entity bitset num"<<endl;
				//EntityBitSet** _new_entity_bitset = new EntityBitSet*[entitybitset_max * 2];
				////BETTER?:if use pointer for array, avoid the copy cost of entitybitset, but consumes more mmeory
				////if the triple size is 1-2 billion, then the memory cost will be very large!!!
				//memcpy(_new_entity_bitset, _entity_bitset, sizeof(EntityBitSet*) * entitybitset_max);
				//delete[] _entity_bitset;
				//_entity_bitset = _new_entity_bitset;

				//int tmp = entitybitset_max * 2;
				//for (int i = entitybitset_max; i < tmp; i++)
				//{
					//_entity_bitset[i] = new EntityBitSet();
					//_entity_bitset[i]->reset();
				//}

				//entitybitset_max = tmp;
			//}

			//{
				//_tmp_bitset.reset();
				//Signature::encodePredicate2Entity(_pre_id, _tmp_bitset, Util::EDGE_OUT);
				//Signature::encodeStr2Entity(_obj.c_str(), _tmp_bitset);
				//*_entity_bitset[_sub_id] |= _tmp_bitset;
			//}

			//if (triple_array[i].isObjEntity())
			//{
				//_tmp_bitset.reset();
				//Signature::encodePredicate2Entity(_pre_id, _tmp_bitset, Util::EDGE_IN);
				//Signature::encodeStr2Entity(_sub.c_str(), _tmp_bitset);
				////cout<<"objid: "<<_obj_id <<endl;
				////when 15999 error
				////WARN:id allocated can be very large while the num is not so much
				////This means that maybe the space for the _obj_id is not allocated now
				////NOTICE:we prepare the free id list in uporder and contiguous, so in build process
				////this can work well
				//*_entity_bitset[_obj_id] |= _tmp_bitset;
			//}
		}
	}

	//cout<<"==> end while(true)"<<endl;

	delete[] triple_array;
	triple_array = NULL;
	_fin.close();
	_six_tuples_fout.close();
	fclose(fp);


		//for (int i = 0; i < entitybitset_max; i++)
		//{
			//delete _entity_bitset[i];
		//}
		//delete[] _entity_bitset;
	
	//{
		//stringstream _ss;
		//_ss << "finish sub2id pre2id obj2id" << endl;
		//_ss << "tripleNum is " << this->triples_num << endl;
		//_ss << "entityNum is " << this->entity_num << endl;
		//_ss << "preNum is " << this->pre_num << endl;
		//_ss << "literalNum is " << this->literal_num << endl;
		//Util::logging(_ss.str());
		//cout << _ss.str() << endl;
	//}

	return true;
}

bool
Database::saveDBInfoFile()
{
	FILE* filePtr = fopen(this->getDBInfoFile().c_str(), "wb");

	if (filePtr == NULL)
	{
		cout << "error, can not create db info file. @Database::saveDBInfoFile" << endl;
		return false;
	}

	fseek(filePtr, 0, SEEK_SET);

	fwrite(&this->triples_num, sizeof(int), 1, filePtr);
	fwrite(&this->entity_num, sizeof(int), 1, filePtr);
	fwrite(&this->sub_num, sizeof(int), 1, filePtr);
	fwrite(&this->pre_num, sizeof(int), 1, filePtr);
	fwrite(&this->literal_num, sizeof(int), 1, filePtr);
	fwrite(&this->encode_mode, sizeof(int), 1, filePtr);

	Util::Csync(filePtr);
	fclose(filePtr);

	//Util::triple_num = this->triples_num;
	//Util::pre_num = this->pre_num;
	//Util::entity_num = this->entity_num;
	//Util::literal_num = this->literal_num;

	return true;
}

bool
Database::encodeRDF_new(const string _rdf_file)
{
#ifdef DEBUG
	//cout<< "now to log!!!" << endl;
	Util::logging("In encodeRDF_new");
	//cout<< "end log!!!" << endl;
#endif

	//TYPE_ENTITY_LITERAL_ID** _p_id_tuples = NULL;
	ID_TUPLE* _p_id_tuples = NULL;
	TYPE_TRIPLE_NUM _id_tuples_max = 0;

	long t1 = Util::get_cur_time();

	//NOTICE: in encode process, we should not divide ID of entity and literal totally apart, i.e. entity is a system 
	//while literal is another system
	//The reason is that if we divide entity and literal, then in triple_array and final_result we can not decide a given
	//ID is entity or not
	//(one way is to add a more structure to tell us which is entity, but this is costly)

	//map sub2id, pre2id, entity/literal in obj2id, store in kvstore, encode RDF data into signature
	if (!this->sub2id_pre2id_obj2id_RDFintoSignature(_rdf_file))
	{
		return false;
	}

	//TODO+BETTER:after encode, we can know the exact entity num, so we can decide if our system can run this dataset 
	//based on the current available memory(need a memory manager globally)
	//If unbale to run, should exit and give a prompt
	//User can modify the config file to run anyway, but gStore will not ensure correctness
	//What is more, in load process, we also need to decide if gStore can run it
	//
	//TODO+BETTER: a global ID manager module, should be based on type template
	//this can be used in vstree, storage and Database

	long t2 = Util::get_cur_time();
	cout << "after encode, used " << (t2 - t1) << "ms." << endl;

	//build stringindex before this->kvstore->id2* trees are closed
	this->stringindex->setNum(StringIndexFile::Entity, this->entity_num);
	this->stringindex->setNum(StringIndexFile::Literal, this->literal_num);
	this->stringindex->setNum(StringIndexFile::Predicate, this->pre_num);
	this->stringindex->save(*this->kvstore);
	//NOTICE: the string index can be parallized with readIDTuples and others
	//However, we should read and build otehr indices only after the 6 trees and string index closed
	//(to save memory)

	long t3 = Util::get_cur_time();
	cout << "after stringindex, used " << (t3 - t2) << "ms." << endl;

	//cout<<"special id: "<<this->kvstore->getIDByEntity("<point7>")<<endl;

	//NOTICE:close these trees now to save memory
	this->kvstore->close_entity2id();
	this->kvstore->close_id2entity();
	this->kvstore->close_literal2id();
	this->kvstore->close_id2literal();
	this->kvstore->close_predicate2id();
	this->kvstore->close_id2predicate();

	long t4 = Util::get_cur_time();
	cout << "id2string and string2id closed, used " << (t4 - t3) << "ms." << endl;

	//after closing the 6 trees, read the id tuples again, and remove the file     given num, a dimension,return a pointer
	//NOTICE: the file can also be used for debugging, and a program can start just from the id tuples file
	//(if copy the 6 id2string trees, no need to parse each time)
	this->readIDTuples(_p_id_tuples);

	//NOTICE: we can also build the signature when we are reading triples, and 
	//update to the corresponding position in the signature file
	//However, this may be costly due to frequent read/write

	long t5 = Util::get_cur_time();
	cout << "id tuples read, used " << (t5 - t4) << "ms." << endl;

	//TODO: how to set the buffer of trees is a big question, fully utilize the availiable memory

	//this->kvstore->build_subID2values(_p_id_tuples, this->triples_num);
	//this->kvstore->open_entityID2UBvalues(KVstore::CREATE_MODE, this->entity_num);
	this->build_s2xx(_p_id_tuples);

	long t6 = Util::get_cur_time();
	cout << "after s2xx, used " << (t6 - t5) << "ms." << endl;

	//this->kvstore->build_objID2values(_p_id_tuples, this->triples_num);
	this->build_o2xx(_p_id_tuples);
	//this->kvstore->close_entityID2UBvalues();

	long t7 = Util::get_cur_time();
	cout << "after o2xx, used " << (t7 - t6) << "ms." << endl;

	//this->kvstore->build_preID2values(_p_id_tuples, this->triples_num);
	this->build_p2xx(_p_id_tuples);

	long t8 = Util::get_cur_time();
	cout << "after p2xx, used " << (t8 - t7) << "ms." << endl;
	// this->kvstore->build_entityID2UBvalues(_p_id_tuples, this->triples_num, this->entity_num);
	// long t9 = Util::get_cur_time();
	// cout << "after entity2UB intialization, used " << (t9 - t8) << "ms." << endl;
	//WARN:we must free the memory for id_tuples array
	delete[] _p_id_tuples;

	//for (TYPE_TRIPLE_NUM i = 0; i < this->triples_num; ++i)
	//{
		//delete[] _p_id_tuples[i];
	//}
	//delete[] _p_id_tuples;

	//NOTICE: we should build vstree after id tuples are freed(to save memory)

	bool flag = this->saveDBInfoFile();
	if (!flag)
	{
		return false;
	}

	long t9 = Util::get_cur_time();
	cout << "db info saved, used " << (t9 - t8) << "ms." << endl;

	//Util::logging("finish encodeRDF_new");

	return true;
}

bool
Database::exist_triple(TYPE_ENTITY_LITERAL_ID _sub_id, TYPE_PREDICATE_ID _pre_id, TYPE_ENTITY_LITERAL_ID _obj_id)
{
	unsigned* _objidlist = NULL;
	unsigned _list_len = 0;
	(this->kvstore)->getobjIDlistBysubIDpreID(_sub_id, _pre_id, _objidlist, _list_len);

	bool is_exist = false;
	//	for(unsigned i = 0; i < _list_len; i ++)
	//	{
	//		if(_objidlist[i] == _obj_id)
	//		{
	//		    is_exist = true;
	//			break;
	//		}
	//	}
	if (Util::bsearch_int_uporder(_obj_id, _objidlist, _list_len) != INVALID)
	//if (Util::bsearch_int_uporder(_obj_id, _objidlist, _list_len) != -1)
	{
		is_exist = true;
	}
	delete[] _objidlist;

	return is_exist;
}

bool Database::exist_triple(const TripleWithObjType& _triple) {
	int sub_id = this->kvstore->getIDByEntity(_triple.getSubject());
	if (sub_id == -1) {
		return false;
	}

	int pre_id = this->kvstore->getIDByPredicate(_triple.getPredicate());
	if (pre_id == -1) {
		return false;
	}

	int obj_id = -1;
	if (_triple.isObjEntity()) {
		obj_id = this->kvstore->getIDByEntity(_triple.getObject());
	}
	else if (_triple.isObjLiteral()) {
		obj_id = this->kvstore->getIDByLiteral(_triple.getObject());
	}
	if (obj_id == -1) {
		return false;
	}

	return exist_triple(sub_id, pre_id, obj_id);
}

bool
Database::insertTriple(const TripleWithObjType& _triple, vector<unsigned>* _vertices, vector<unsigned>* _predicates)
{
	//cout<<endl<<"the new triple is:"<<endl;
	//cout<<_triple.subject<<endl;
	//cout<<_triple.predicate<<endl;
	//cout<<_triple.object<<endl;

	//int sid1 = this->kvstore->getIDByEntity("<http://www.Department7.University0.edu/UndergraduateStudent394>");
	//int sid2 = this->kvstore->getIDByEntity("<http://www.Department7.University0.edu/UndergraduateStudent395>");
	//int oid1 = this->kvstore->getIDByEntity("<ub:UndergraduateStudent>");
	//int oid2 = this->kvstore->getIDByEntity("<UndergraduateStudent394@Department7.University0.edu>");
	//cout<<sid1<<" "<<sid2<<" "<<oid1<<" "<<oid2<<endl;

	long tv_kv_store_begin = Util::get_cur_time();

	TYPE_ENTITY_LITERAL_ID _sub_id = (this->kvstore)->getIDByEntity(_triple.subject);
	bool _is_new_sub = false;
	//if sub does not exist
	if (_sub_id == INVALID_ENTITY_LITERAL_ID)
	//if (_sub_id == -1)
	{
		_is_new_sub = true;
		//_sub_id = this->entity_num++;
		_sub_id = this->allocEntityID();
		//cout<<"this is a new sub id: "<<_sub_id<<endl;
		this->sub_num++;
		(this->kvstore)->setIDByEntity(_triple.subject, _sub_id);
		(this->kvstore)->setEntityByID(_sub_id, _triple.subject);

		//update the string buffer
		//if (_sub_id < this->entity_buffer_size)
		//{
			//this->entity_buffer->set(_sub_id, _triple.subject);
		//}

		if (_vertices != NULL)
			_vertices->push_back(_sub_id);
	}

	TYPE_PREDICATE_ID _pre_id = (this->kvstore)->getIDByPredicate(_triple.predicate);
	bool _is_new_pre = false;
	//if pre does not exist
	if (_pre_id == INVALID_PREDICATE_ID)
	//if (_pre_id == -1)
	{
		_is_new_pre = true;
		//_pre_id = this->pre_num++;
		_pre_id = this->allocPredicateID();
		(this->kvstore)->setIDByPredicate(_triple.predicate, _pre_id);
		(this->kvstore)->setPredicateByID(_pre_id, _triple.predicate);

		if (_predicates != NULL)
			_predicates->push_back(_pre_id);
	}

	//object is either entity or literal
	TYPE_ENTITY_LITERAL_ID _obj_id = INVALID_ENTITY_LITERAL_ID;
	//int _obj_id = -1;
	bool _is_new_obj = false;
	bool is_obj_entity = _triple.isObjEntity();
	if (is_obj_entity)
	{
		_obj_id = (this->kvstore)->getIDByEntity(_triple.object);

		//if (_obj_id == -1)
		if (_obj_id == INVALID_ENTITY_LITERAL_ID)
		{
			_is_new_obj = true;
			//_obj_id = this->entity_num++;
			_obj_id = this->allocEntityID();
			(this->kvstore)->setIDByEntity(_triple.object, _obj_id);
			(this->kvstore)->setEntityByID(_obj_id, _triple.object);

			//update the string buffer
			//if (_obj_id < this->entity_buffer_size)
			//{
				//this->entity_buffer->set(_obj_id, _triple.object);
			//}

			if (_vertices != NULL)
				_vertices->push_back(_obj_id);
		}
	}
	else
	{
		_obj_id = (this->kvstore)->getIDByLiteral(_triple.object);
		//cout<<"check: "<<_obj_id<<" "<<INVALID_ENTITY_LITERAL_ID<<endl;

		//if (_obj_id == -1)
		if (_obj_id == INVALID_ENTITY_LITERAL_ID)
		{
			_is_new_obj = true;
			//_obj_id = Util::LITERAL_FIRST_ID + this->literal_num;
			_obj_id = this->allocLiteralID();
			//cout<<"this is a new obj id: "<<_obj_id<<endl;
			(this->kvstore)->setIDByLiteral(_triple.object, _obj_id);
			(this->kvstore)->setLiteralByID(_obj_id, _triple.object);
			//cout<<this->kvstore->getLiteralByID(_obj_id)<<endl;
			//cout<<this->kvstore->getIDByLiteral(_triple.object)<<endl;

			//update the string buffer
			//TYPE_ENTITY_LITERAL_ID tid = _obj_id - Util::LITERAL_FIRST_ID;
			//if (tid < this->literal_buffer_size)
			//{
				//this->literal_buffer->set(tid, _triple.object);
			//}

			if (_vertices != NULL)
				_vertices->push_back(_obj_id);
		}
	}

	//if this is not a new triple, return directly
	bool _triple_exist = false;
	if (!_is_new_sub && !_is_new_pre && !_is_new_obj)
	{
		_triple_exist = this->exist_triple(_sub_id, _pre_id, _obj_id);
	}

	//debug
	//  {
	//      stringstream _ss;
	//      _ss << this->literal_num << endl;
	//      _ss <<"ids: " << _sub_id << " " << _pre_id << " " << _obj_id << " " << _triple_exist << endl;
	//      Util::logging(_ss.str());
	//  }

	if (_triple_exist)
	{
		cout << "this triple already exist" << endl;
		return false;
	}
	else
	{
		this->triples_num++;
	}
	//cout<<"the triple spo ids: "<<_sub_id<<" "<<_pre_id<<" "<<_obj_id<<endl;

	//update sp2o op2s s2po o2ps s2o o2s etc.
	unsigned updateLen = (this->kvstore)->updateTupleslist_insert(_sub_id, _pre_id, _obj_id);

	//int* list = NULL;
	//int len = 0;
	//int root = this->kvstore->getIDByEntity("<root>");
	//int contain = this->kvstore->getIDByPredicate("<contain>");
	//this->kvstore->getobjIDlistBysubIDpreID(root, contain, list, len);
	//cout<<root<<" "<<contain<<endl;
	//if(len == 0)
	//{
	//cout<<"no result"<<endl;
	//}
	//for(int i = 0; i < len; ++i)
	//{
	//cout << this->kvstore->getEntityByID(list[i])<<" "<<list[i]<<endl;
	//}

	return true;
	//return updateLen;
}

unsigned
Database::insert(const TripleWithObjType* _triples, TYPE_TRIPLE_NUM _triple_num, bool _is_restore)
{
	vector<TYPE_ENTITY_LITERAL_ID> vertices, predicates;
	TYPE_TRIPLE_NUM valid_num = 0;

	if (!_is_restore) {
		string path = this->getStorePath() + '/' + this->update_log;
		string path_all = this->getStorePath() + '/' + this->update_log_since_backup;
		ofstream out;
		ofstream out_all;
		out.open(path.c_str(), ios::out | ios::app);
		out_all.open(path_all.c_str(), ios::out | ios::app);
		if (!out || !out_all) {
			cerr << "Failed to open update log. Insertion aborted." << endl;
			return 0;
		}
		for (int i = 0; i < _triple_num; i++) {
			if (exist_triple(_triples[i])) {
				continue;
			}
			stringstream ss;
			ss << "I\t" << Util::node2string(_triples[i].getSubject().c_str()) << '\t';
			ss << Util::node2string(_triples[i].getPredicate().c_str()) << '\t';
			ss << Util::node2string(_triples[i].getObject().c_str()) << '.' << endl;
			out << ss.str();
			out_all << ss.str();
		}
		out.close();
		out_all.close();
	}

#ifdef USE_GROUP_INSERT
	//NOTICE:this is called by insert(file) or query()(but can not be too large),
	//assume that db is loaded already
	TYPE_ENTITY_LITERAL_ID** id_tuples = new TYPE_ENTITY_LITERAL_ID*[_triple_num];

	//TODO:change the type in this catagolory
	int i = 0;
	//for(i = 0; i < _triple_num; ++i)
	//{
	//id_tuples[i] = new int[3];
	//}
	map<int, EntityBitSet> old_sigmap;
	map<int, EntityBitSet> new_sigmap;
	set<int> new_entity;
	map<int, EntityBitSet>::iterator it;
	EntityBitSet tmpset;
	tmpset.reset();

	int subid, objid, preid;
	bool is_obj_entity;
	for (i = 0; i < _triple_num; ++i)
	{
		bool is_new_sub = false, is_new_pre = false, is_new_obj = false;

		string sub = _triples[i].getSubject();
		subid = this->kvstore->getIDByEntity(sub);
		if (subid == -1)
		{
			is_new_sub = true;
			subid = this->allocEntityID();
#ifdef DEBUG
			//cout << "this is a new subject: " << sub << " " << subid << endl;
#endif
			this->sub_num++;
			this->kvstore->setIDByEntity(sub, subid);
			this->kvstore->setEntityByID(subid, sub);
			new_entity.insert(subid);
			//add info and update buffer
			vertices.push_back(subid);
			//if (subid < this->entity_buffer_size)
			//{
				//this->entity_buffer->set(subid, sub);
			//}
		}

		string pre = _triples[i].getPredicate();
		preid = this->kvstore->getIDByPredicate(pre);
		if (preid == -1)
		{
			is_new_pre = true;
			preid = this->allocPredicateID();
			this->kvstore->setIDByPredicate(pre, preid);
			this->kvstore->setPredicateByID(preid, pre);
			predicates.push_back(preid);
		}

		is_obj_entity = _triples[i].isObjEntity();
		string obj = _triples[i].getObject();
		if (is_obj_entity)
		{
			objid = this->kvstore->getIDByEntity(obj);
			if (objid == -1)
			{
				is_new_obj = true;
				objid = this->allocEntityID();
#ifdef DEBUG
				//cout << "this is a new object: " << obj << " " << objid << endl;
#endif
				//this->obj_num++;
				this->kvstore->setIDByEntity(obj, objid);
				this->kvstore->setEntityByID(objid, obj);
				new_entity.insert(objid);
				//add info and update
				vertices.push_back(objid);
				//if (objid < this->entity_buffer_size)
				//{
					//this->entity_buffer->set(objid, obj);
				//}
			}
		}
		else //isObjLiteral
		{
			objid = this->kvstore->getIDByLiteral(obj);
			if (objid == -1)
			{
				is_new_obj = true;
				objid = this->allocLiteralID();
				//this->obj_num++;
				this->kvstore->setIDByLiteral(obj, objid);
				this->kvstore->setLiteralByID(objid, obj);
				//add info and update
				vertices.push_back(objid);
				//int tid = objid - Util::LITERAL_FIRST_ID;
				//if (tid < this->literal_buffer_size)
				//{
					//this->literal_buffer->set(tid, obj);
				//}
			}
		}

		bool triple_exist = false;
		if (!is_new_sub && !is_new_pre && !is_new_obj)
		{
			triple_exist = this->exist_triple(subid, preid, objid);
		}
		if (triple_exist)
		{
#ifdef DEBUG
			cout << "this triple exist" << endl;
#endif
			continue;
		}
#ifdef DEBUG
		cout << "this triple not exist" << endl;
#endif

		id_tuples[valid_num] = new int[3];
		id_tuples[valid_num][0] = subid;
		id_tuples[valid_num][1] = preid;
		id_tuples[valid_num][2] = objid;
		this->triples_num++;
		valid_num++;

		tmpset.reset();
		Signature::encodePredicate2Entity(preid, tmpset, Util::EDGE_OUT);
		Signature::encodeStr2Entity(obj.c_str(), tmpset);
		if (new_entity.find(subid) != new_entity.end())
		{
			it = new_sigmap.find(subid);
			if (it != new_sigmap.end())
			{
				it->second |= tmpset;
			}
			else
			{
				new_sigmap[subid] = tmpset;
			}
		}
		else
		{
			it = old_sigmap.find(subid);
			if (it != old_sigmap.end())
			{
				it->second |= tmpset;
			}
			else
			{
				old_sigmap[subid] = tmpset;
			}
		}

		if (is_obj_entity)
		{
			tmpset.reset();
			Signature::encodePredicate2Entity(preid, tmpset, Util::EDGE_IN);
			Signature::encodeStr2Entity(sub.c_str(), tmpset);
			if (new_entity.find(objid) != new_entity.end())
			{
				it = new_sigmap.find(objid);
				if (it != new_sigmap.end())
				{
					it->second |= tmpset;
				}
				else
				{
					new_sigmap[objid] = tmpset;
				}
			}
			else
			{
				it = old_sigmap.find(objid);
				if (it != old_sigmap.end())
				{
					it->second |= tmpset;
				}
				else
				{
					old_sigmap[objid] = tmpset;
				}
			}
		}
	}

#ifdef DEBUG
	cout << "old sigmap size: " << old_sigmap.size() << endl;
	cout << "new sigmap size: " << new_sigmap.size() << endl;
	cout << "valid num: " << valid_num << endl;
#endif

	//NOTICE:need to sort and remove duplicates, update the valid num
	//Notice that duplicates in a group can csuse problem
	//We finish this by spo cmp

	//this->kvstore->updateTupleslist_insert(_sub_id, _pre_id, _obj_id);
	//sort and update kvstore: 11 indexes
	//
	//BETTER:maybe also use int* here with a size to start
	//NOTICE:all kvtrees are opened now, one by one if memory is bottleneck
	//
	//spo cmp: s2p s2o s2po sp2o
	{
#ifdef DEBUG
		cout << "INSRET PROCESS: to spo cmp and update" << endl;
#endif
#ifndef PARALLEL_SORT
		qsort(id_tuples, valid_num, sizeof(int*), KVstore::_spo_cmp);
#else
		omp_set_num_threads(thread_num);
		__gnu_parallel::sort(id_tuples, id_tuples + valid_num, KVstore::parallel_spo_cmp);
#endif
		
		//To remove duplicates
		//int ti = 1, tj = 1;
		//while(tj < valid_num)
		//{
		//if(id_tuples[tj][0] != id_tuples[tj-1][0] || id_tuples[tj][1] != id_tuples[tj-1][1] || id_tuples[tj][2] != id_tuples[tj-1][2])
		//{
		//id_tuples[ti][0] = id_tuples[tj][0];
		//id_tuples[ti][1] = id_tuples[tj][1];
		//id_tuples[ti][2] = id_tuples[tj][2];
		//ti++;
		//}
		//tj++;
		//}
		//for(tj = ti; tj < valid_num; ++tj)
		//{
		//delete[] id_tuples[tj];
		//id_tuples[tj] = NULL;
		//}
		//valid_num = ti;
		//
		//Notice that below already consider duplicates in loop

		vector<int> oidlist_s;
		vector<int> pidlist_s;
		vector<int> oidlist_sp;
		vector<int> pidoidlist_s;

		bool _sub_change = true;
		bool _sub_pre_change = true;
		bool _pre_change = true;

		for (int i = 0; i < valid_num; ++i)
			if (i + 1 == valid_num || (id_tuples[i][0] != id_tuples[i + 1][0] || id_tuples[i][1] != id_tuples[i + 1][1] || id_tuples[i][2] != id_tuples[i + 1][2]))
			{
				int _sub_id = id_tuples[i][0];
				int _pre_id = id_tuples[i][1];
				int _obj_id = id_tuples[i][2];

				oidlist_s.push_back(_obj_id);
				oidlist_sp.push_back(_obj_id);
				pidoidlist_s.push_back(_pre_id);
				pidoidlist_s.push_back(_obj_id);
				pidlist_s.push_back(_pre_id);

				_sub_change = (i + 1 == valid_num) || (id_tuples[i][0] != id_tuples[i + 1][0]);
				_pre_change = (i + 1 == valid_num) || (id_tuples[i][1] != id_tuples[i + 1][1]);
				_sub_pre_change = _sub_change || _pre_change;

				if (_sub_pre_change)
				{
#ifdef DEBUG
					cout << "update sp2o: " << _sub_id << " " << _pre_id << " " << oidlist_sp.size() << endl;
#endif
					cout << this->kvstore->getEntityByID(_sub_id) << endl;
					cout << this->kvstore->getPredicateByID(_pre_id) << endl;
					//this->kvstore->updateInsert_sp2o(_sub_id, _pre_id, oidlist_sp);
					oidlist_sp.clear();
				}

				if (_sub_change)
				{
#ifdef DEBUG
					cout << "update s2p: " << _sub_id << " " << pidlist_s.size() << endl;
#endif
					//this->kvstore->updateInsert_s2p(_sub_id, pidlist_s);
					pidlist_s.clear();

#ifdef DEBUG
					cout << "update s2po: " << _sub_id << " " << pidoidlist_s.size() << endl;
#endif
					this->kvstore->updateInsert_s2values(_sub_id, pidoidlist_s);
					pidoidlist_s.clear();

#ifdef DEBUG
					cout << "update s2o: " << _sub_id << " " << oidlist_s.size() << endl;
#endif
#ifndef PARALLEL_SORT
					sort(oidlist_s.begin(), oidlist_s.end());
#else
					omp_set_num_threads(thread_num);
					__gnu_parallel::sort(oidlist_s.begin(), oidlist_s.end());
#endif
					//this->kvstore->updateInsert_s2o(_sub_id, oidlist_s);
					oidlist_s.clear();
				}

			}
#ifdef DEBUG
		cout << "INSERT PROCESS: OUT s2po..." << endl;
#endif
	}
	//ops cmp: o2p o2s o2ps op2s
	{
#ifdef DEBUG
		cout << "INSRET PROCESS: to ops cmp and update" << endl;
#endif
#ifndef PARALLEL_SORT
		qsort(id_tuples, valid_num, sizeof(int**), KVstore::_ops_cmp);
#else
		omp_set_num_threads(thread_num);
		__gnu_parallel::sort(id_tuples, id_tuples + valid_num, KVstore::parallel_ops_cmp);
#endif
		vector<int> sidlist_o;
		vector<int> sidlist_op;
		vector<int> pidsidlist_o;
		vector<int> pidlist_o;

		bool _obj_change = true;
		bool _pre_change = true;
		bool _obj_pre_change = true;

		for (int i = 0; i < valid_num; ++i)
			if (i + 1 == valid_num || (id_tuples[i][0] != id_tuples[i + 1][0] || id_tuples[i][1] != id_tuples[i + 1][1] || id_tuples[i][2] != id_tuples[i + 1][2]))
			{
				int _sub_id = id_tuples[i][0];
				int _pre_id = id_tuples[i][1];
				int _obj_id = id_tuples[i][2];

				sidlist_o.push_back(_sub_id);
				sidlist_op.push_back(_sub_id);
				pidsidlist_o.push_back(_pre_id);
				pidsidlist_o.push_back(_sub_id);
				pidlist_o.push_back(_pre_id);

				_obj_change = (i + 1 == valid_num) || (id_tuples[i][2] != id_tuples[i + 1][2]);
				_pre_change = (i + 1 == valid_num) || (id_tuples[i][1] != id_tuples[i + 1][1]);
				_obj_pre_change = _obj_change || _pre_change;

				if (_obj_pre_change)
				{
#ifdef DEBUG
					cout << "update op2s: " << _obj_id << " " << _pre_id << " " << sidlist_op.size() << endl;
#endif
					//this->kvstore->updateInsert_op2s(_obj_id, _pre_id, sidlist_op);
					sidlist_op.clear();
				}

				if (_obj_change)
				{
#ifdef DEBUG
					cout << "update o2s: " << _obj_id << " " << sidlist_o.size() << endl;
#endif
#ifndef PARALLEL_SORT
					sort(sidlist_o.begin(), sidlist_o.end());
#else
					omp_set_num_threads(thread_num);
					__gnu_parallel::sort(sidlist_o.begin(), sidlist_o.end());
#endif
					//this->kvstore->updateInsert_o2s(_obj_id, sidlist_o);
					sidlist_o.clear();

#ifdef DEBUG
					cout << "update o2ps: " << _obj_id << " " << pidsidlist_o.size() << endl;
#endif
					this->kvstore->updateInsert_o2values(_obj_id, pidsidlist_o);
					pidsidlist_o.clear();

#ifdef DEBUG
					cout << "update o2p: " << _obj_id << " " << pidlist_o.size() << endl;
#endif
					//this->kvstore->updateInsert_o2p(_obj_id, pidlist_o);
					pidlist_o.clear();
				}

			}
#ifdef DEBUG
		cout << "INSERT PROCESS: OUT o2ps..." << endl;
#endif
	}
	//pso cmp: p2s p2o p2so
	{
#ifdef DEBUG
		cout << "INSRET PROCESS: to pso cmp and update" << endl;
#endif
#ifndef PARALLEL_SORT
		qsort(id_tuples, valid_num, sizeof(int*), KVstore::_pso_cmp);
#else
		omp_set_num_threads(thread_num);
		__gnu_parallel::sort(id_tuples, id_tuples + valid_num,  KVstore::parallel_pso_cmp);
#endif
		vector<int> sidlist_p;
		vector<int> oidlist_p;
		vector<int> sidoidlist_p;

		bool _pre_change = true;
		bool _sub_change = true;
		//bool _pre_sub_change = true;

		for (int i = 0; i < valid_num; i++)
			if (i + 1 == valid_num || (id_tuples[i][0] != id_tuples[i + 1][0] || id_tuples[i][1] != id_tuples[i + 1][1] || id_tuples[i][2] != id_tuples[i + 1][2]))
			{
				int _sub_id = id_tuples[i][0];
				int _pre_id = id_tuples[i][1];
				int _obj_id = id_tuples[i][2];

				oidlist_p.push_back(_obj_id);
				sidoidlist_p.push_back(_sub_id);
				sidoidlist_p.push_back(_obj_id);
				sidlist_p.push_back(_sub_id);

				_pre_change = (i + 1 == valid_num) || (id_tuples[i][1] != id_tuples[i + 1][1]);
				_sub_change = (i + 1 == valid_num) || (id_tuples[i][0] != id_tuples[i + 1][0]);
				//_pre_sub_change = _pre_change || _sub_change;

				if (_pre_change)
				{
#ifdef DEBUG
					cout << "update p2s: " << _pre_id << " " << sidlist_p.size() << endl;
#endif
					//this->kvstore->updateInsert_p2s(_pre_id, sidlist_p);
					sidlist_p.clear();

#ifdef DEBUG
					cout << "update p2o: " << _pre_id << " " << oidlist_p.size() << endl;
#endif
#ifndef PARALLEL_SORT
					sort(oidlist_p.begin(), oidlist_p.end());
#else
					omp_set_num_threads(thread_num);
					__gnu_parallel::sort(oidlist_p.begin(), oidlist_p.end());
#endif
					//this->kvstore->updateInsert_p2o(_pre_id, oidlist_p);
					oidlist_p.clear();

#ifdef DEBUG
					cout << "update p2so: " << _pre_id << " " << sidoidlist_p.size() << endl;
#endif
					this->kvstore->updateInsert_p2values(_pre_id, sidoidlist_p);
					sidoidlist_p.clear();
				}
			}
#ifdef DEBUG
		cout << "INSERT PROCESS: OUT p2so..." << endl;
#endif
	}


	for (int i = 0; i < valid_num; ++i)
	{
		delete[] id_tuples[i];
	}
	delete[] id_tuples;
	id_tuples = NULL;

	//for (it = old_sigmap.begin(); it != old_sigmap.end(); ++it)
	//{
		//this->vstree->updateEntry(it->first, it->second);
	//}
	//for (it = new_sigmap.begin(); it != new_sigmap.end(); ++it)
	//{
		//SigEntry _sig(it->first, it->second);
		//this->vstree->insertEntry(_sig);
	//}
#else //USE_GROUP_INSERT
	//NOTICE:we deal with insertions one by one here
	//Callers should save the vstree(node and info) after calling this function
	for (TYPE_TRIPLE_NUM i = 0; i < _triple_num; ++i)
	{
		bool ret = this->insertTriple(_triples[i], &vertices, &predicates);
		if(ret)
		{
			valid_num++;
		}
	}
#endif //USE_GROUP_INSERT

	this->stringindex->SetTrie(kvstore->getTrie());
	//update string index
	this->stringindex->change(vertices, *this->kvstore, true);
	this->stringindex->change(predicates, *this->kvstore, false);

	return valid_num;
}

bool
Database::insert(std::string _rdf_file, bool _is_restore)
{
	bool flag = _is_restore || this->load();
	//bool flag = this->load();
	if (!flag)
	{
		return false;
	}
	cout << "finish loading" << endl;

	long tv_load = Util::get_cur_time();

	TYPE_TRIPLE_NUM success_num = 0;

	ifstream _fin(_rdf_file.c_str());
	if (!_fin)
	{
		cout << "fail to open : " << _rdf_file << ".@insert_test" << endl;
		//exit(0);
		return false;
	}

	//NOTICE+WARN:we can not load all triples into memory all at once!!!
	//the parameter in build and insert must be the same, because RDF parser also use this
	//for build process, this one can be big enough if memory permits
	//for insert/delete process, this can not be too large, otherwise too costly
	TripleWithObjType* triple_array = new TripleWithObjType[RDFParser::TRIPLE_NUM_PER_GROUP];
	//parse a file
	RDFParser _parser(_fin);

	TYPE_TRIPLE_NUM triple_num = 0;
#ifdef DEBUG
	Util::logging("==> while(true)");
#endif
	while (true)
	{
		int parse_triple_num = 0;
		_parser.parseFile(triple_array, parse_triple_num);
#ifdef DEBUG
		stringstream _ss;
		//NOTICE:this is not same as others, use parse_triple_num directly
		_ss << "finish rdfparser" << parse_triple_num << endl;
		Util::logging(_ss.str());
		cout << _ss.str() << endl;
#endif
		if (parse_triple_num == 0)
		{
			break;
		}

		//Compress triple here
		//for(int i = 0; i < parse_triple_num; i++)
		//{
			//TripleWithObjType compressed_triple = trie->Compress(triple_array[i], Trie::QUERYMODE);
			//triple_array[i] = compressed_triple;
		//}

		//Process the Triple one by one
		success_num += this->insert(triple_array, parse_triple_num, _is_restore);
		//some maybe invalid or duplicate
		//triple_num += parse_triple_num;
	}
	delete[] triple_array;
	triple_array = NULL;
	this->kvstore->close_entity2id();
	this->kvstore->close_id2entity();
	this->kvstore->close_literal2id();
	this->kvstore->close_id2literal();
	this->kvstore->close_predicate2id();
	this->kvstore->close_id2predicate();
	this->kvstore->close_subID2values();
	this->kvstore->close_preID2values();
	this->kvstore->close_entityID2UBvalues();
	this->kvstore->close_entityID2Hashvalues();
	flag = this->saveDBInfoFile();
	if (!flag)
		return false;
	long tv_insert = Util::get_cur_time();
	cout << "after insert, used " << (tv_insert - tv_load) << "ms." << endl;
	//BETTER:update kvstore and vstree separately, to lower the memory cost
	//flag = this->vstree->saveTree();
	//if (!flag)
	//{
		//return false;
	//}
	//flag = this->saveDBInfoFile();
	//if (!flag)
	//{
		//return false;
	//}

	cout << "insert rdf triples done." << endl;
	cout<<"inserted triples num: "<<success_num<<endl;

	//int* list = NULL;
	//int len = 0;
	//int root = this->kvstore->getIDByEntity("<root>");
	//int contain = this->kvstore->getIDByPredicate("<contain>");
	//this->kvstore->getobjIDlistBysubIDpreID(root, contain, list, len);
	//cout<<root<<" "<<contain<<endl;
	//if(len == 0)
	//{
	//cout<<"no result"<<endl;
	//}
	//for(int i = 0; i < len; ++i)
	//{
	//cout << this->kvstore->getEntityByID(list[i])<<" "<<list[i]<<endl;
	//}
	//cout<<endl;
	//int t = this->kvstore->getIDByEntity("<node5>");
	//cout<<t<<endl;
	//cout<<this->kvstore->getEntityByID(0)<<endl;

	return true;
}

bool
Database::getPreListBySub(string sub, unsigned& prelen, string*& preList) {
	unsigned *preidlist = NULL;
	unsigned len1 = 0;
	TYPE_ENTITY_LITERAL_ID subid = this->kvstore->getIDByEntity(sub);
	if (!this->kvstore->getpreIDlistBysubID(subid, preidlist, len1, true))
		return false;
	prelen = len1;
	preList = new string[len1];
	for (unsigned i = 0; i < len1; i++)
		preList[i] = this->kvstore->getPredicateByID(preidlist[i]);	
	return true;
}

bool
Database::getpreIDobjIDlistBysub(string sub, unsigned*& preobjList) {
	unsigned preobjlen = 0;
	TYPE_ENTITY_LITERAL_ID subid = this->kvstore->getIDByEntity(sub);
	if (!this->kvstore->getpreIDobjIDlistBysubID(subid, preobjList, preobjlen, true))
		return false;
	return true;
}


bool
Database::getUBListByEntity(string entity, float*& UBList) {
	TYPE_ENTITY_LITERAL_ID entityid = this->kvstore->getIDByEntity(entity);
	if (!this->kvstore->getUBlistByentityID(entityid, UBList))
		return false;
	return true;
}

bool
Database::getHashListByEntity(string entity, unsigned*& HashList) {
	TYPE_ENTITY_LITERAL_ID entityid = this->kvstore->getIDByEntity(entity);
	if (!this->kvstore->getHashlistByentityID(entityid, HashList))
		return false;
	return true;
}

bool
Database::insertUB(std::string _UB_file) {
	cout << "Start inserting UB" << endl;
	ifstream UB_File(_UB_file.c_str());
	string currLine;
	while (getline(UB_File, currLine)) {
		vector<string> lineSplit;
		Util::split(currLine, lineSplit);
		if (lineSplit.size() != K+1)
			return false;
		string entity = lineSplit[0];
		float *currUB = new float[K];
		for (int i = 0; i < K; i++)
			currUB[i] = stof(lineSplit[i+1]);
		this->kvstore->insertUB(entity, currUB);
		//delete[] currUB;
	}
	this->kvstore->close_entityID2UBvalues();
	cout << "Insert complete" << endl;
	return true;
}

bool
Database::insertHash(std::string _Hash_file) {
	cout << "Start inserting Hash" << endl;
	ifstream Hash_File(_Hash_file.c_str());
	string currLine;
	while (getline(Hash_File, currLine)) {
		vector<string> lineSplit;
		Util::split(currLine, lineSplit);
		if (lineSplit.size() != N+1)
			return false;
		string entity = lineSplit[0];
		unsigned *currHash = new unsigned[N];
		for (int i = 0; i < N; i++)
			currHash[i] = stoi(lineSplit[i+1]);
		this->kvstore->insertHash(entity, currHash);
		//delete[] currHash;
	}
	this->kvstore->close_entityID2Hashvalues();
	cout << "Insert complete" << endl;
	return true;
}