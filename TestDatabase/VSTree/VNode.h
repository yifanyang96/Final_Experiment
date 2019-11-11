/*=============================================================================
# Filename: VNode.h
# Author: Bookug Lobert 
# Mail: zengli-bookug@pku.edu.cn
# Last Modified: 2016-04-11 14:05
# Description: written by liyouhuan
=============================================================================*/

#ifndef _VSTREE_VNODE_H
#define _VSTREE_VNODE_H

#include "../Util/Util.h"
#include "../Signature/SigEntry.h"
#include "LRUCache.h"

class VNode
{
public:
	static const int DEGREE = 100;  //use 100 for normal
	//NOTICE+WARN:always ensure double times due to union operation safety in coalesce
	//here 201 is used to ensure split operation is right
	//(generally, 200 is ok if dealed carefully, but here only 200 is really used)
    static const int MAX_CHILD_NUM = 2 * VNode::DEGREE + 1;
    //static const int MAX_CHILD_NUM = 151;
    static const int MIN_CHILD_NUM = VNode::DEGREE;
    //static const int MIN_CHILD_NUM = 60;
	static const int VNODE_SIZE = sizeof(int) * (3+MAX_CHILD_NUM) + sizeof(SigEntry) * (MAX_CHILD_NUM+1);
	static const unsigned DIRTY_PART = 0x08000000;
	static const unsigned DEL_DIRTY_PART = ~DIRTY_PART;
	static const unsigned LEAF_PART = 0x04000000;
	VNode();
	VNode(bool _is_leaf);
	bool isLeaf() const;
	int getFileLine() const;
    void setFileLine(int _line);
	bool isDirty() const;
	void setDirty(bool _flag = true);
	bool readNode(FILE* _fp);
	bool writeNode(FILE* _fp);

private:
	//BETTER:including height and modify the memory-disk swap strategy
	//then is_root flag is unnecessary
	unsigned flag; 
	//bool dirty;
    //bool is_leaf;
    //bool is_root;
    //int child_num;
    int self_file_line;
    int father_file_line;
    SigEntry entry;
    SigEntry child_entries[VNode::MAX_CHILD_NUM];

	//NOTICE: in leaf node, no need to keep the big child array
    //int child_file_lines[VNode::MAX_CHILD_NUM];
	int* child_file_lines;

	void AllocChilds();
	void InitLock();

};

#endif // _VSTREE_VNODE_H

