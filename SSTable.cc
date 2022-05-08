#include "SSTable.h"
#include <cstring>
#include <iostream>

using namespace std;

SSTablecache::SSTablecache(/* args */)
{
    this->Bloom = new bool[10240];
    this->timeStamp = 1;
    this->key_count = 0;
    this->key_max = 0;
    this->key_min = 0;
    this->index = 0;
    this->level = 0;
}

SSTablecache::SSTablecache(const SSTablecache& a)
{
    timeStamp = a.timeStamp;
    key_count = a.key_count;
    key_max = a.key_max;
    key_min = a.key_min;
    length = a.length;
    this->Bloom = new bool[10240];
    memcpy(Bloom,a.Bloom,10240);

    this->key_array.resize(key_count);
    this->offset_array.resize(key_count);
    for(int i = 0;i < key_count;++i){
        this->key_array[i] = a.key_array[i];
        this->offset_array[i] = a.offset_array[i];
    }
    this->index = a.index;
    this->level = a.level;
}

SSTablecache::SSTablecache(unsigned long long time,unsigned long long count,unsigned long long min,
unsigned long long max,SKNode* p,bool *filter,int offset):timeStamp(time),key_count(count),key_min(min),key_max(max)
{
    this->Bloom = new bool[10240];
    memcpy(Bloom,filter,10240);

    SKNode* NIL = new SKNode(INT_MAX, "", SKNodeType::NIL);
    this->key_array.resize(key_count);
    this->offset_array.resize(key_count);
    int index = 0;
    while(p->val != ""){
        //this->key_array.push_back(p->key);
        this->key_array[index] = (p->key);
        //this->pushOffset(offset);
        this->offset_array[index] = offset;
        offset += p->val.length();
        p = p->forwards[0];
        index++;
    }
    this->length = offset;
    this->index = 0;
    this->level = 0;
    delete NIL;
}

void SSTablecache::pushOffset(int offset)
{
    this->offset_array.push_back(offset);
}

//判断元素是否在对应的SSTable中,如果找到了则返回对应true,并把offset和length存在message中
bool SSTablecache::Search(unsigned long long &key,int* message)
{
    
    unsigned int hash[4] = {0};

    // for(int i = 0; i < 10240;++i){
    //     cout << Bloom[i] << " ";
    // }

    MurmurHash3_x64_128(&key,sizeof(key),1,hash);
    if(Bloom[hash[0]%10240] && Bloom[hash[1]%10240] && Bloom[hash[2]%10240] && Bloom[hash[3]%10240]){
        auto first = std::lower_bound(this->key_array.begin(), this->key_array.end(), key);
        //bool jug = first == this->key_array.end();
        //unsigned long long a = *(this->key_array.end());
        if(!(first == this->key_array.end()) && (*first == key)){
            int distance = first - this->key_array.begin();
            message[0] = this->offset_array[distance];
            if(distance + 1 == this->key_count){
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
    this->key_array.clear();
    this->offset_array.clear();
}

void SSTablecache::list_key()
{
    cout <<"distance :" << key_array.end() - key_array.begin() << endl;
    for(std::vector<unsigned long long>::iterator iter = key_array.begin();iter != key_array.end();++iter){
        std::cout << *iter << " ";
    }
    std::cout.flush();
}

unsigned long long SSTablecache::getTime()
{
    return this->timeStamp;
}

unsigned long long SSTablecache::getkey_min()
{
    return this->key_min;
}

unsigned long long SSTablecache::getkey_max()
{
    return this->key_max;
}

unsigned long long SSTablecache::getkey_Count()
{
    return this->key_count;
}

int SSTablecache::getindex()
{
    return this->index;
}

int SSTablecache::getlevel()
{
    return this.level;
}

void SSTablecache::setindex(int index)
{
    this.index = index;
}

void SSTablecache::setlevel(int level)
{
    this->level = level;
}


kv_box* SSTablecache::to_kv_box()
{
    kv_box* val = new kv_box(this->key_count);
    for(int i = 0;i < key_count;++i){
        val[i].index = this->index;
        val[i].level = this->level;
        val[i].key = this->key_array.at(i);
        val[i].offset = this->key_array.at(i);
        if(i != key_count - 1){
            val[i].length = this->offset_array.at(i+1) - this->offset_array.at(i);
        }
        else{
            val[i].length = this->length - this->offset_array.at(i);
        }
    }
    return val;
}