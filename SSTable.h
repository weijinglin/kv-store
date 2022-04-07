#pragma once
#include "SkipList.h"
#include <vector>
#include "MurmurHash3.h"

using namespace std;

//注意这里的cache中的offset和key是分开构造，因此保证其一一对应的关系很有必要
class SSTablecache
{
private:
    /* data */
    unsigned long long timeStamp;
    unsigned long long key_count;
    unsigned long long key_min;
    unsigned long long key_max;
    int length;//the length of the SSTable
    bool *Bloom;
    vector<unsigned long long> key_array;//to store all the key value,it is sequential
    vector<int> offset_array;//to store all the offset
public:
    SSTablecache();
    //进行除了offset以外所有元素的构造
    SSTablecache(unsigned long long time,unsigned long long count,unsigned long long min,unsigned long long max,SKNode* p,bool *filter,int offset);
    ~SSTablecache();
    void pushOffset(int offset);
    //判断元素是否在对应的SSTable中
    bool Search(unsigned long long key,int* message);
};

