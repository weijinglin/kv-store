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
	bool* Bloom;//写入SSTable的Bloom过滤器
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

	void w_file(SSTablecache* myCache.int index,int level);//简单的用于正常的文件写入

	void do_Compac(SSTablecache *myCache);

	kv_box* MergeSort(vector<SSTablecache *> &sort_list)//这个归并排序返回一个kv_box数组，然后可以根据这数组进行数据的导入
};
