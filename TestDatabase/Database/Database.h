#ifndef _DATABASE_DATABASE_H
#define _DATABASE_DATABASE_H 

#include "../Util/Util.h"
#include "../Util/Triple.h"
#include "../KVstore/KVstore.h"
#include "../Parser/RDFParser.h"
#include "../StringIndex/StringIndex.h"
#include "../VSTree/LRUCache.h"

class Database
{
public:
	static const int ID_MODE = 2;

	Database();
	Database(std::string _name);
	bool build(const string& _rdf_file);
	bool insert(std::string _rdf_file, bool _is_restore = false);
	bool insertUB(std::string _UB_file);
	bool insertHash(std::string _Hash_file);
	bool load();
	string getIDTuplesFile();
	string getSixTuplesFile();
	string getDBInfoFile();
	bool loadDBInfoFile();
    bool getPreListBySub(string sub, unsigned& prelen, string*& preList);
	bool getpreIDobjIDlistBysub(string sub, unsigned*& preobjList);
    bool getUBListByEntity(string entity, float*& UBList);
    bool getHashListByEntity(string entity, unsigned*& HashList);

private:
	string id_tuples_file;
	string six_tuples_file;
	string signature_binary_file;

	string name;
	string store_path;
	bool is_active;
	bool if_loaded;
	pthread_rwlock_t update_lock;

    string update_log;
	string update_log_since_backup;
	string db_info_file;
	int encode_mode;

	TYPE_TRIPLE_NUM triples_num;
	TYPE_ENTITY_LITERAL_ID entity_num;
	TYPE_ENTITY_LITERAL_ID sub_num;
	//BETTER: add object num
	TYPE_PREDICATE_ID pre_num;
	TYPE_ENTITY_LITERAL_ID literal_num;
	TYPE_TRIPLE_NUM* pre2num;
	//pre2subnum mapping
	TYPE_TRIPLE_NUM* pre2sub;
	//pre2objnum mapping
	TYPE_TRIPLE_NUM* pre2obj;

	Buffer* entity_buffer;
	//unsigned offset; //maybe let id start from an offset
	unsigned entity_buffer_size;
	Buffer* literal_buffer;
	unsigned literal_buffer_size;


	KVstore* kvstore;
	StringIndex* stringindex;


	string free_id_file_entity; //the first is limitID, then free id list
	TYPE_ENTITY_LITERAL_ID limitID_entity; //the current maxium ID num(maybe not used so much)
	BlockInfo* freelist_entity; //free id list, reuse BlockInfo for Storage class
	string free_id_file_literal;
	TYPE_ENTITY_LITERAL_ID limitID_literal;
	BlockInfo* freelist_literal;
	string free_id_file_predicate;
	TYPE_PREDICATE_ID limitID_predicate;
	BlockInfo* freelist_predicate;
	TYPE_PREDICATE_ID maxNumPID, minNumPID;

	std::priority_queue <KEY_SIZE_VALUE> candidate_preID;
	std::priority_queue <KEY_SIZE_VALUE> important_subID;
	std::priority_queue <KEY_SIZE_VALUE> important_objID;

	string getStorePath();
	void setPreMap();
	void load_cache();
	void get_important_preID();
	std::vector <TYPE_PREDICATE_ID> important_preID;
	void load_important_sub2values();
	void load_important_obj2values();
	void load_candidate_pre2values();
	void build_CacheOfPre2values();
	void build_CacheOfSub2values();
	void build_CacheOfObj2values();
	void get_important_subID();
	void get_important_objID();
	void get_candidate_preID();
	void load_entity2id(int _mode);
	void load_id2entity(int _mode);
	void load_literal2id(int _mode);
	void load_id2literal(int _mode);
	void load_predicate2id(int _mode);
	void load_id2predicate(int _mode);
	void load_sub2values(int _mode);
	void load_obj2values(int _mode);
	void load_pre2values(int _mode);
	void load_entity2UBvalues(int _mode);
	void load_entity2Hashvalues(int _mode);

	void check();

	void initIDinfo();  //initialize the members
	void resetIDinfo(); //reset the id info for build
	void readIDinfo();  //read and build the free list

	TYPE_ENTITY_LITERAL_ID allocEntityID();
	TYPE_ENTITY_LITERAL_ID allocLiteralID();
	TYPE_PREDICATE_ID allocPredicateID();

	void readIDTuples(ID_TUPLE*& _p_id_tuples);
	void build_s2xx(ID_TUPLE*);
	void build_o2xx(ID_TUPLE*);
	void build_p2xx(ID_TUPLE*);

	bool saveDBInfoFile();

	bool sub2id_pre2id_obj2id_RDFintoSignature(const string _rdf_file);
	bool encodeRDF_new(const string _rdf_file);

	bool exist_triple(TYPE_ENTITY_LITERAL_ID _sub_id, TYPE_PREDICATE_ID _pre_id, TYPE_ENTITY_LITERAL_ID _obj_id);
	bool exist_triple(const TripleWithObjType& _triple);
	unsigned insert(const TripleWithObjType* _triples, TYPE_TRIPLE_NUM _triple_num, bool _is_restore=false);
	bool insertTriple(const TripleWithObjType& _triple, vector<unsigned>* _vertices = NULL, vector<unsigned>* _predicates = NULL);

};

#endif