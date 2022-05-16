#include "SSTable.h"
#include <cstring>
#include <iostream>

using namespace std;

SSTablecache::SSTablecache(/* args */)
{
    this->Bloom = new uint8_t[10240];
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
    this->Bloom = new uint8_t[10240];
    memcpy(Bloom,a.Bloom,10240);

    //this->kv_array.resize(key_count);
    for(int i = 0;i < key_count;++i){
        this->kv_array.push_back(a.kv_array.at(i));
    }
    this->index = a.index;
    this->level = a.level;
}

SSTablecache::SSTablecache(unsigned long long time,unsigned long long count,unsigned long long min,
unsigned long long max,SKNode* p,uint8_t *filter,int offset):timeStamp(time),key_count(count),key_min(min),key_max(max)
{
    this->Bloom = new uint8_t[10240];
    memcpy(Bloom,filter,10240);

    SKNode* NIL = new SKNode(INT_MAX, "", SKNodeType::NIL);
    //this->kv_array.resize(key_count);
    int index = 0;
    while(p->val != ""){
        kv_pair gen_kv(p->key,offset);
        gen_kv.length = p->val.length();
        this->kv_array.push_back(gen_kv);
        offset += p->val.length();
        p = p->forwards[0];
        index++;
    }
    this->length = offset;
    this->index = 0;
    this->level = 0;
    delete NIL;
}

//判断元素是否在对应的SSTable中,如果找到了则返回对应true,并把offset和length存在message中
bool SSTablecache::Search(unsigned long long &key,int* message)
{

    unsigned int hash[4] = {0};

    // for(int i = 0; i < 10240;++i){
    //     cout << Bloom[i] << " ";
    // }

    MurmurHash3_x64_128(&key,sizeof(key),1,hash);
    //判断思路：通过组装一个uint_8,然后判断是不是每一位都是1
    uint8_t is_exist = 0;
    for(int i = 0;i < 4;++i){
        unsigned int pos = (hash[i] % (10240 * 8));
        unsigned int field = pos / 8;
        unsigned int exact_pos = pos % 8;

        uint8_t tmp = Bloom[field];
        tmp = (tmp >> exact_pos) & 1;
        is_exist = is_exist | (tmp << i);
    }
    //if(Bloom[hash[0]%10240] && Bloom[hash[1]%10240] && Bloom[hash[2]%10240] && Bloom[hash[3]%10240]){
    if(is_exist == 15){
        // auto first = std::lower_bound(this->key_array.begin(), this->key_array.end(), key);
        // if(!(first == this->key_array.end()) && (*first == key)){
        //     int distance = first - this->key_array.begin();
        //     message[0] = this->offset_array[distance];
        //     if(distance + 1 == this->key_count){
        //         message[1] = length - this->offset_array[distance];
        //     }
        //     else{
        //         message[1] = this->offset_array[distance+1] - this->offset_array[distance];
        //     }
        //     return true;
        // }
        // else{
        //     return false;
        // }

        //进行二分查找
        uint64_t left = 0;
        uint64_t right = key_count;
        while(right > left){
            if(this->kv_array.at((left+right)/2).key > key){
                right = (left + right)/2 - 1;
                continue;
            }
            else{
                if(this->kv_array.at((left+right)/2).key == key){
                    message[0] = this->kv_array[(left+right)/2].offset;
                    message[1] = this->kv_array[(left+right)/2].length;
                    message[2] = (left+right)/2;
                    return true;
                }
                else{
                    left = (left+right)/2 + 1;
                    continue;
                }
            }
        }

        //防止非法访问
        if((left+right)/2 == key_count){
            return false;
        }
        if(this->kv_array.at((left+right)/2).key == key){
            message[0] = this->kv_array[(left+right)/2].offset;
            message[1] = this->kv_array[(left+right)/2].length;
            message[2] = (left+right)/2;
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
    delete [] Bloom;
    kv_array.clear();
}

void SSTablecache::list_key()
{

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
    return this->level;
}

void SSTablecache::setindex(int index)
{
    this->index = index;
}

void SSTablecache::setlevel(int level)
{
    this->level = level;
}

int SSTablecache::getlength()
{
    return this->length;
}

kv_box* SSTablecache::to_kv_box()
{
    kv_box* val = new kv_box[this->key_count];
    for(int i = 0;i < key_count;++i){
        val[i].index = this->index;
        val[i].level = this->level;
        val[i].data.key = this->kv_array.at(i).key;
        val[i].data.offset = this->kv_array.at(i).offset;
        val[i].data.length = this->kv_array.at(i).length;
    }
    return val;
}

kv_pair SSTablecache::get_pair(uint64_t index)
{
    return this->kv_array.at(index);
}

uint8_t* SSTablecache::get_bloom()
{
    return this->Bloom;
}
