#pragma once
#include "SkipList.h"
#include <vector>
#include "MurmurHash3.h"

using namespace std;

struct kv_box
{
	/* data */
	//前面两个参数用于定义要读取的字串的基本信息
	uint64_t key;
	int offset;

	int length;//记录字串的长度

	//后面两个参数用于定位要读取的文件的位置
	int index;
	int level;

	kv_box(){
		key = -1;
		offset = -1;
		length = -1;
		index = -1;
		level = -1;
	}
	kv_box(uint64_t k,int o,int len,int in,int le):key(k),offset(o),length(len),index(in),level(le){}
};

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

    //存储缓冲区文件存储位置的信息
    int index;
    int level;
public:
    SSTablecache();
    //复制构造函数，用于解决vector的push_back的问题
    SSTablecache(const SSTablecache& a);
    //进行除了offset以外所有元素的构造
    SSTablecache(unsigned long long time,unsigned long long count,unsigned long long min,unsigned long long max,SKNode* p,bool *filter,int offset);
    ~SSTablecache();
    void pushOffset(int offset);
    //判断元素是否在对应的SSTable中
    bool Search(unsigned long long &key,int* message);
    //用于Debug的函数
    void list_key();

    unsigned long long getkey_min();

    unsigned long long getkey_max();

    unsigned long long getTime();

    unsigned long long getkey_Count();

    int getindex();

    int getlevel();

    void setindex(int index);

    void setlevel(int level);

    kv_box* to_kv_box();
};

