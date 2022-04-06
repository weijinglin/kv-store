#include "SSTable.h"
#include <iostream>

using namespace std;

SSTablecache::SSTablecache(/* args */)
{
    this->Bloom = new bool[10240];
    this->timeStamp = 1;
    this->key_count = 0;
    this->key_array = NULL;
    this->key_max = 0;
    this->key_min = 0;
}

SSTablecache::SSTablecache(unsigned long long time,unsigned long long count,unsigned long long min,
unsigned long long max,SKNode* p,bool *filter):timeStamp(time),key_count(count),key_min(min),key_max(max)
{
    Bloom = filter;
    while(p->forwards[0] != NIL){
        this->key_array.push_back(p->key);
    }
}

void SSTablecache::pushOffset(int offset)
{
    this->offset_array.push_back(offset);
}

SSTablecache::~SSTablecache()
{
    delete Bloom;
    delete key_array;
}