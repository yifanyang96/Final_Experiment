#include <iostream>
#include <fstream>
#include <string>
#include <vector>
using namespace std;

#define K 5

void cp(char*& p2, char* p1, unsigned len) {
    char *np = new char[len];
    memcpy(np, p1, len);
    p2 = np;
}

void
split(const std::string& s, std::vector<std::string>& sv, const char* delim = " ") {
    sv.clear();
	char* buffer = new char[s.size() + 1];
    std::copy(s.begin(), s.end(), buffer);
    char* p = std::strtok(buffer, delim);
    do {
        sv.push_back(p);
    } while ((p = std::strtok(NULL, delim)));
}

int main () {
    string path = "TestUB";
    ifstream UB_File(path.c_str());
	string currLine = "";
	while (getline(UB_File, currLine)) {
        cout << currLine << endl;
		vector<string> lineSplit;
		split(currLine, lineSplit);
		if (lineSplit.size() != K+1) {
			cout << "ERROR" << endl;
            return 0;
        }
		string entity = lineSplit[0];
        cout << entity << endl;
		float *currUB = new float[K];
		for (int i = 0; i < K; i++) {
			currUB[i] = stof(lineSplit[i+1]);
            cout << currUB[i] << endl;
        }
		delete[] currUB;
	}
    UB_File.close();
    return 1;
}