#pragma once
#include "SkipList.h"
#include <vector>

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
    bool *Bloom;
    vector<unsigned long long> key_array;//to store all the key value
    vector<int> offset_array;//to store all the offset
public:
    SSTablecache();
    SSTablecache(unsigned long long time,unsigned long long count,unsigned long long min,unsigned long long max,SKNode* p,bool *filter);
    ~SSTablecache();
    void pushOffset(int offset);
};

