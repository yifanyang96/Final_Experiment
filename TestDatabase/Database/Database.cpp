#include "Database.h"

using namespace std;

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

string
Database::getDBInfoFile()
{
	return this->getStorePath() + "/" + this->db_info_file;
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
	this->build_s2xx(_p_id_tuples);

	long t6 = Util::get_cur_time();
	cout << "after s2xx, used " << (t6 - t5) << "ms." << endl;

	//this->kvstore->build_objID2values(_p_id_tuples, this->triples_num);
	this->build_o2xx(_p_id_tuples);

	long t7 = Util::get_cur_time();
	cout << "after o2xx, used " << (t7 - t6) << "ms." << endl;

	//this->kvstore->build_preID2values(_p_id_tuples, this->triples_num);
	this->build_p2xx(_p_id_tuples);

	long t8 = Util::get_cur_time();
	cout << "after p2xx, used " << (t8 - t7) << "ms." << endl;

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