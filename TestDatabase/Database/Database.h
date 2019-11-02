#ifndef _DATABASE_DATABASE_H
#define _DATABASE_DATABASE_H 

#include "../Util/Util.h"
#include "../Util/Triple.h"
#include "../KVstore/KVstore.h"
#include "../Parser/RDFParser.h"
#include "../StringIndex/StringIndex.h"

class Database
{
public:
	bool build(const string& _rdf_file);
	string getIDTuplesFile();
	string getSixTuplesFile();
	string getDBInfoFile();

private:
	string id_tuples_file;
	string six_tuples_file;

	string name;
	string store_path;
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
	string getStorePath();

	void initIDinfo();  //initialize the members
	void resetIDinfo(); //reset the id info for build

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

};

#endif