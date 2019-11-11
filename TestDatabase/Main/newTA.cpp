#include <string>
#include <vector>
#include <algorithm>
#include "../Database/Database.h"

using namespace std;



bool
newTAPre(Database& _db, string e1,vector<string> R1,string e2,vector<string> R2, string t){
    unsigned *HashList1 = NULL;
    if (_db.getHashListByEntity(e1, HashList1)) {
        for (unsigned i = 0; i < N; i++)
            cout << HashList1[i] << endl;
    }
    unsigned *HashList2 = NULL;
    if (_db.getHashListByEntity(e2, HashList2)) {
        for (unsigned i = 0; i < N; i++)
            cout << HashList2[i] << endl;
    }
    unsigned *HashListT = NULL;
    if (_db.getHashListByEntity(t, HashListT)) {
        for (unsigned i = 0; i < N; i++)
            cout << HashListT[i] << endl;
    }
    unsigned range_low = max(HashList1[0], HashList2[0], HashListT[0]);
    unsigned range_high = min(HashList1[1], HashList2[1], HashListT[1]);

}