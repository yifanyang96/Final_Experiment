#include "../Database/Database.h"

#define SYSTEM_PATH "../data/system/system.nt"

int
main(int argc, char * argv[]) {
    Util util;
    string db_folder = string(argv[1]);
    int len = db_folder.length();
	if(db_folder.substr(len-3, 3) == ".db") {
		cout<<"your database can not end with .db"<<endl;
		return -1;
	}
    Database _db(db_folder);
	_db.load();
    string entity = string(argv[2]);
    string *preList = NULL;
    unsigned prelen = 0;
    if (_db.getPreListBySub(entity, prelen, preList)) {
        for (unsigned i = 0; i < prelen; i++)
            cout << preList[i] << endl;
    }
    float *UBList = NULL;
    if (_db.getUBListByEntity(entity, UBList)) {
        for (unsigned i = 0; i < K; i++)
            cout << UBList[i] << endl;
    }
    unsigned *HashList = NULL;
    if (_db.getHashListByEntity(entity, HashList)) {
        for (unsigned i = 0; i < N; i++)
            cout << HashList[i] << endl;
    }
    return 1;
}