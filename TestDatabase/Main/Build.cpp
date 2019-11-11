#include "../Database/Database.h"

#define SYSTEM_PATH "../data/system/system.nt"

int
main(int argc, char * argv[])
{
	//chdir(dirname(argv[0]));
//#ifdef DEBUG
	Util util;
//#endif
	if(argc < 3)  //./gbuild
	{
		//output help info here
		cout << "the usage of gbuild: " << endl;
		cout << "bin/gbuild your_database_name.db your_dataset_path" << endl;
		return 0;
	}
	//system("clock");
	cout << "gbuild..." << endl;
	{
		cout << "argc: " << argc << "\t";
		cout << "DB_store:" << argv[1] << "\t";
		cout << "RDF_data: " << argv[2] << "\t";
		cout << endl;
	}

	string _db_path = string(argv[1]);
	int len = _db_path.length();
	if(_db_path.length() > 3 && _db_path.substr(len-3, 3) == ".db")
	{
		cout<<"your database can not end with .db or less than 3 characters"<<endl;
		return -1;
	}

	//check if the db_name is system
	if (_db_path == "system")
	{
		cout<< "Your database's name can not be system."<<endl;
		return -1;
	}

	//if(_db_path[0] != '/' && _db_path[0] != '~')  //using relative path
	//{
	//_db_path = string("../") + _db_path;
	//}
	string _rdf = string(argv[2]);

	//check if the db_path is the path of system.nt
	if (_rdf == SYSTEM_PATH)
	{
		cout<< "You have no rights to access system files"<<endl;
		return -1;
	}

	//if(_rdf[0] != '/' && _rdf[0] != '~')  //using relative path
	//{
	//_rdf = string("../") + _rdf;
	//}

	//check if the database is already built
	//build database
	Database _db(_db_path);
	bool flag = _db.build(_rdf);
	if (flag)
	{
		cout << "import RDF file to database done." << endl;
		ofstream f;
		f.open("./"+ _db_path +".db/success.txt");
		f.close();
	}
	else //if fails, drop database and return
	{
		cout << "import RDF file to database failed." << endl;
		string cmd = "rm -r " + _db_path + ".db";
		system(cmd.c_str());
		return 0;
	}

	cout << "insert successfully" << endl;

	cout<<endl<<endl<<endl<< "import RDF file to database done." << endl;
	return 0;
}

