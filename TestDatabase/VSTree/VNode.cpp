/*=============================================================================
# Filename: VNode.cpp
# Author: Bookug Lobert 
# Mail: zengli-bookug@pku.edu.cn
# Last Modified: 2016-04-11 14:08
# Description: by liyouhuan and hanshuo
=============================================================================*/

#include "VNode.h"

using namespace std;

VNode::VNode()
{
    //this->is_leaf = false;
    //this->is_root = false;
    //this->child_num = 0;
	this->flag = 0;
    this->self_file_line = -1;
    this->father_file_line = -1;

	this->child_file_lines = new int[VNode::MAX_CHILD_NUM];
    for(int i = 0; i < VNode::MAX_CHILD_NUM; i ++)
    {
    	this->child_file_lines[i] = -1;
    }

	InitLock();
}

VNode::VNode(bool _is_leaf)
{
    //this->is_leaf = false;
    //this->is_root = false;
    //this->child_num = 0;
	this->flag = 0;
    this->self_file_line = -1;
    this->father_file_line = -1;

	if(_is_leaf)
	{
		this->child_file_lines = NULL;
		//return;
	}
	else
	{
		this->AllocChilds();
	}

	InitLock();
}

bool 
VNode::isLeaf() const
{
	return this->flag & VNode::LEAF_PART;
}

int 
VNode::getFileLine() const
{
    return this->self_file_line;
}

void 
VNode::setFileLine(int _line)
{
    this->self_file_line = _line;
	//this->setDirty();
}

bool
VNode::isDirty() const
{
	return this->flag & VNode::DIRTY_PART;
}

void
VNode::setDirty(bool _flag)
{
	if(_flag)
	{
		this->flag |= VNode::DIRTY_PART;
	}
	else
	{
		this->flag &= VNode::DEL_DIRTY_PART;
	}
}

bool
VNode::readNode(FILE* _fp)
{
	int ret = fread(&(this->flag), sizeof(unsigned), 1, _fp);
	if(ret == 0)  //the edn of file
	{
		return false;
	}
	fread(&(this->self_file_line), sizeof(int), 1, _fp);
	//cout<<"to read node: "<<this->self_file_line<<endl;
	//BETTER:if the id < 0, then just move, not read

	fread(&(this->father_file_line), sizeof(int), 1, _fp);
	fread(&(this->entry), sizeof(SigEntry), 1, _fp);

	//for(int i = 0; i < VNode::MAX_CHILD_NUM; ++i)
	//{
		//fread(&(this->child_entries[i]), sizeof(SigEntry), 1, _fp);
	//}
	fread(this->child_entries, sizeof(SigEntry), VNode::MAX_CHILD_NUM, _fp);

	if(!this->isLeaf())  //internal node
	{
		this->child_file_lines = new int[VNode::MAX_CHILD_NUM];
		//for(int i = 0; i < VNode::MAX_CHILD_NUM; ++i)
		//{
			//fread(&(this->child_file_lines[i]), sizeof(int), 1, _fp);
		//}
		fread(this->child_file_lines, sizeof(int), VNode::MAX_CHILD_NUM, _fp);
	}
	else //move to the end of the node block
	{
		fseek(_fp, sizeof(int) * VNode::MAX_CHILD_NUM, SEEK_CUR);
	}
	//this->setDirty(false);

	return true;
}

bool
VNode::writeNode(FILE* _fp)
{
	//clean, then no need to write
	//if(!this->isDirty())
	//{
		//return true;
	//}
	//NOTICE:already dealed in LRUCache
	//this->setDirty(false);

	//cout<<"to write node: "<<this->self_file_line<<endl;
	fwrite(&(this->flag), sizeof(unsigned), 1, _fp);
	fwrite(&(this->self_file_line), sizeof(int), 1, _fp);
	//NOTICE: this must be a old node(not new inserted node), so no need to write more
	if(this->self_file_line < 0)
	{
		return true;
	}

	fwrite(&(this->father_file_line), sizeof(int), 1, _fp);
	fwrite(&(this->entry), sizeof(SigEntry), 1, _fp);

	//for(int i = 0; i < VNode::MAX_CHILD_NUM; ++i)
	//{
		//fwrite(&(this->child_entries[i]), sizeof(SigEntry), 1, _fp);
	//}
	fwrite(this->child_entries, sizeof(SigEntry), VNode::MAX_CHILD_NUM, _fp);

	if(!this->isLeaf())  //internal node
	{
		//for(int i = 0; i < VNode::MAX_CHILD_NUM; ++i)
		//{
			//fwrite(&(this->child_file_lines[i]), sizeof(int), 1, _fp);
		//}
		fwrite(this->child_file_lines, sizeof(int), VNode::MAX_CHILD_NUM, _fp);
	}
	else //move to the end of the node block
	{
		//fseek(_fp, sizeof(int) * VNode::MAX_CHILD_NUM, SEEK_CUR);
		int t = 0;
		for(int i = 0; i < VNode::MAX_CHILD_NUM; ++i)
		{
			fwrite(&t, sizeof(int), 1, _fp);
		}
	}

	return true;
}

void 
VNode::AllocChilds()
{
	this->child_file_lines = new int[VNode::MAX_CHILD_NUM];
    for(int i = 0; i < VNode::MAX_CHILD_NUM; i ++)
    {
    	this->child_file_lines[i] = -1;
    }
}

void 
VNode::InitLock()
{
#ifdef THREAD_VSTREE_ON
	pthread_mutexattr_t attr;
	pthread_mutexattr_init(&attr);
	pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
	pthread_mutex_init(&(this->node_lock), &attr);
#endif
}