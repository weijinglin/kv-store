#include "kvstore.h"
#include <string>
#include "utils.h"
#include <filesystem>
#include <iostream>
#include <fstream>
#include "MurmurHash3.h"

using namespace std;

KVStore::KVStore(const std::string &dir): KVStoreAPI(dir),rootDir(dir)
{
	string copy = dir;
	this->timeStamp = 1;
	this->key_count = 0;
	Bloom = new bool[10*1024];
	if(!(utils::dirExists(copy))){
		return;
	}
	string sstable = dir + "/level-0";
	int result = utils::mkdir(sstable.c_str());
	if(!result){
		string myFile = sstable + "/data0.sst";
		ofstream sst(myFile.c_str());
	}
	else{
		exit(0);
	}
}

std::string KVStore::getDir(){
	return this->rootDir;
}

//要进行数据的保存
KVStore::~KVStore()
{
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const string &s)
{
	unsigned long long lengthbefore = this->Memtable.getBytes();
	unsigned long long length = lengthbefore + KEY_LENGTH + OFFSET_LENGTH + s.length();
	if(length > 2 * 1024 * 1024){
		//文件大小超标，开始向有硬盘写入数据
		string dir = this->getDir();
		string file_path = dir + "/level-0/data0.sst";
		ofstream data_file(file_path);
		//write Header
		char *writeIn = reinterpret_cast<char *>(&(this->timeStamp));
		data_file.write(writeIn,8);
		writeIn = reinterpret_cast<char *>(&(this->key_count));
		data_file.write(writeIn,8);
		unsigned min = this->Memtable.getMinkey();
		unsigned max = this->Memtable.getMaxkey();
		writeIn = reinterpret_cast<char *>(&min);
		data_file.write(writeIn,8);
		writeIn = reinterpret_cast<char *>(&max);
		data_file.write(writeIn,8);
		//这相当与这条指令的二进制写入data_file << this->timeStamp << this->key_count << this->Memtable.getMinkey() << this->Memtable.getMaxkey();
		//write Bloom filter
		for(int i = 0;i < 10240;++i){
			writeIn = reinterpret_cast<char *>(&(Bloom[i]));
			data_file.write(writeIn,1);
		}
		long offset = 32 + 10*1024 + key_count * 12;//the offset of the first element
		SKNode *p = this->Memtable.getMinEle();
		SKNode *q = p;
		//write key and offset
		for(int i = 0;i < key_count;++i){
			data_file.write(reinterpret_cast<char *>(p->key),8);
			data_file.write(reinterpret_cast<char *>(offset),8);
			//这相当与这条指令的二进制写入data_file << p->key << offset;
			offset += p->val.length();
			p = p->forwards[0];
		}
		//write value
		for(int i = 0;i < key_count;++i){
			strcpy(writeIn,q->val.c_str());
			data_file.write(writeIn,q->val.length());
			//这相当与这条指令的二进制写入data_file << q->val;
			q = q->forwards[0];
		}
		//clean the MemTable
		this->Memtable.cleanMem();
		this->key_count = 0;
		delete this->Bloom;
		this->Bloom = new bool[10*1024];
		this->timeStamp += 1;//timeStamp update

		// to Insert the k-v after flush
		this->Memtable.Insert(key,s);
		this->key_count += 1;
	}
	else{
		this->Memtable.Insert(key,s);
		this->key_count += 1;
	}
	return;
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{
	return this->Memtable.Search(key);
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
	return this->Memtable.Delete(key);
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, list<pair<uint64_t, string> > &list)
{	
	this->Memtable.ScanSearch(key1,key2,list);
}