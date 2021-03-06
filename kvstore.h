#pragma once

#include "kvstore_api.h"
#include "SkipList.h"
#include "SSTable.h"
#include "level.h"
#include <vector>


class KVStore : public KVStoreAPI {
	// You can add your implementation here
private:
	SkipList Memtable;//用跳表实现的内存存储 
	std::string rootDir;//存储多级存储的根目录
	unsigned long long timeStamp;//记录SSTable的时间戳
	unsigned long long key_count;//记录写入SStable的键的数量
	uint8_t* Bloom;//写入SSTable的Bloom过滤器
	//std::vector<SSTablecache*> acache;//缓存sstable中的内容
	std::vector<Level*> all_level; //存储各级的level的缓存
	int level;//用来表示目录的嵌套的级数

public:
	KVStore(const std::string &dir);

	~KVStore();

	void resetBloom();

	void put(uint64_t key, const std::string &s) override;

	std::string get(uint64_t key) override;

	bool del(uint64_t key) override;

	void reset() override;

	void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list) override;

	std::string getDir();//获取根目录

	void w_file(SSTablecache* myCache);//简单的用于正常的文件写入

	void do_Compac();

	void Merge_l_zero(kv_box *seq_kv,vector<SSTablecache *> &s,vector<SkipList*> &mem);

	//给定一个kv_box数组和数组的长度，从文件中读取特定的值存在mem中并放在vector中
	void fill_mem(kv_box* gen,uint64_t count,vector<SkipList *> &mem);

	void gen_sstable(vector<SkipList *> &mem,vector<SSTablecache *> &s_list);
	
	void read_kv(vector<SSTablecache*> &mem,vector<kv *> &m);

	kv* read_sorted_kv(vector<SSTablecache*> &mem);

    kv* merger_sort(kv* one,kv* two,uint64_t len_1,uint64_t len_2,uint64_t &mer_time);

	void del_file(vector<SSTablecache*> &s);

	void gen_table_kv(kv* mem,uint64_t len,vector<SSTablecache*> &s_list,vector<SkipList*> &skip);

    kv* merge_self(kv* mem,uint64_t len,bool is_lastint ,uint64_t &mer_time);

	void w_file_plus(SSTablecache* myCache,SkipList * mem);
};
