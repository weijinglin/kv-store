#include "SSTable.h"
#include <iostream>

using namespace std;

SSTablecache::SSTablecache(/* args */)
{
    this->Bloom = new bool[10240];
    this->timeStamp = 1;
    this->key_count = 0;
    this->key_max = 0;
    this->key_min = 0;
}

SSTablecache::SSTablecache(unsigned long long time,unsigned long long count,unsigned long long min,
unsigned long long max,SKNode* p,bool *filter,int offset):timeStamp(time),key_count(count),key_min(min),key_max(max)
{
    Bloom = filter;
    SKNode* NIL = new SKNode(INT_MAX, "", SKNodeType::NIL);
    while(p->forwards[0] != NIL){
        this->key_array.push_back(p->key);
        this->pushOffset(offset);
        offset += p->val.length();
    }
    this->length = offset;
}

void SSTablecache::pushOffset(int offset)
{
    this->offset_array.push_back(offset);
}

//判断元素是否在对应的SSTable中,如果找到了则返回对应true,并把offset和length存在message中
bool SSTablecache::Search(unsigned long long key,int* message)
{
    unsigned int hash[4] = {0};
    MurmurHash3_x64_128(&key,sizeof(key),1,hash);
    if(Bloom[hash[0]%10272] && Bloom[hash[1]%10272] && Bloom[hash[2]%10272] && Bloom[hash[3]%10272]){
        auto first = std::lower_bound(this->key_array.begin(), this->key_array.end(), key);
        if(!(first == this->key_array.end()) && (*first == key)){
            int distance = first - this->key_array.begin();
            message[0] = this->offset_array[distance];
            if(first + 1 == this->key_array.end()){
                message[1] = length - this->offset_array[distance];
            }
            else{
                message[1] = this->offset_array[distance+1] - this->offset_array[distance];
            }
            return true;
        }
        else{
            return false;
        }
    }
    else{
        return false;
    }
}

SSTablecache::~SSTablecache()
{
    delete Bloom;
}