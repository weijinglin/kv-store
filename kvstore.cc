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
	//初始发现对应的数据目录下面没有数据文件时的处理方式
	string copy = dir;
	this->timeStamp = 1;
	this->key_count = 0;
	this->level = 0;
	Bloom = new bool[10*1024];
	if(!(utils::dirExists(copy))){
		return;
	}
	string sstable = dir + "/level-0";
	int result = utils::mkdir(sstable.c_str());
	if(!result){
		string myFile = sstable + "/data0.sst";
		ofstream sst(myFile.c_str());
		sst.close();
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
	string myString = this->Memtable.Search(key);
	if(myString != ""){
		length = lengthbefore + s.length() - myString.length();
	}
	if(length > 2 * 1024 * 1024){
		//准备将要写入文件的内容
		this->key_count = this->Memtable.getKetcount();
		unsigned min = this->Memtable.getMinkey();
		unsigned max = this->Memtable.getMaxkey();
		int offset = 32 + 10*1024 + key_count * 12;//the offset of the first element
		int put_offset = offset;

		//write to sstablecache as the cache
		SSTablecache *myCache = new SSTablecache(this->timeStamp,this->key_count,this->Memtable.getMinkey(),this->Memtable.getMaxkey(),
		this->Memtable.getMinEle(),this->Bloom,put_offset);
		//push the cache to the cache vector
		
		//进行SSTable缓存方式的判定
		if(level == 0){
			Level *in_level = new Level(0);
			this->all_level.push_back(in_level);
			in_level->put_SSTable(myCache);
			w_file(myCache,0,0);
			level++;
		}
		else{
			if(this->all_level.at(0)->getCount() == 1){
				this->all_level.at(0)->put_SSTable(myCache);
				w_file(myCache,1,0);
			}
			else{
				//do Compaction
				//首先先看看对应的数据的目录在不在，不在的话要创建对应的level目录

				//先得到相关区间
				uint64_t ckey_min = this->all_level.at(0)->find_cache(0)->getkey_min() >
				this->all_level.at(0)->find_cache(1)->getkey_min() ? 
				 this->all_level.at(0)->find_cache(0)->getkey_min() :
				this->all_level.at(0)->find_cache(1)->getkey_min();
				uint64_t ckey_max = this->all_level.at(0)->find_cache(0)->getkey_max() >
				this->all_level.at(0)->find_cache(1)->getkey_max() ? 
				 this->all_level.at(0)->find_cache(0)->getkey_max() :
				this->all_level.at(0)->find_cache(1)->getkey_max();

				ckey_max = ckey_max >= myCache->getkey_max() ? ckey_max : myCache->getkey_max();
				ckey_min = ckey_min <= myCache->getkey_min() ? ckey_min : myCache->getkey_min();

				//在level-1中进行寻找关联文件
				if(level == 1){
					//level 还没有数据的情况
					Level* in_level = new Level(1);
					this->all_level.push_back(in_level);
					level++;
					//先生成对应的目录
					string dir = this->getDir();
					string sstable = dir + "/level-1";
					int result = utils::mkdir(sstable.c_str());

					//此处生成文件的策略采用运动SSTable的下表index来标识

				}
			}
		}


		//clean the MemTable
		this->Memtable.cleanMem();

		//clean the Bloom filter
		resetBloom();
		

		this->timeStamp += 1;//timeStamp update

		// to Insert the k-v after flush
		this->Memtable.Insert(key,s);

		//更新Bloom filter
		unsigned int hash[4] = {0};
		unsigned long long myKey = key;
		MurmurHash3_x64_128(&myKey,sizeof(myKey), 1, hash);
		for(int i = 0;i < 4;++i){
			Bloom[hash[i] % 10240] = true;
		}

		data_file.close();
	}
	else{
		this->Memtable.Insert(key,s);
		//更新Bloom filter
		unsigned int hash[4] = {0};
		unsigned long long myKey = key;
		MurmurHash3_x64_128(&myKey,sizeof(myKey), 1, hash);
		for(int i = 0;i < 4;++i){
			Bloom[hash[i] % 10240] = true;
		}
	}
	return;
}

void KVStore::do_Compac(SSTablecache *myCache)
{
	
}

void KVStore::w_file(SSTablecache* myCache.int index,int level)
{
		//文件大小超标，开始向有硬盘写入数据
		string dir = this->getDir();

		//准备文件信息
		string file_level = std::to_string(level);
		string file_road = "/level-" + file_level;
		string fileroad = file_road + "/data";
		string timenum = std::to_string(index);
		//cout << timenum << endl;
		fileroad = fileroad + timenum;
		fileroad = fileroad + ".sst";

		string file_path = dir + fileroad;
		ofstream data_file(file_path,ios::out | ios::binary);

		int offset = 32 + 10*1024 + key_count * 12;//the offset of the first element
		unsigned min = myCache->getkey_min();
		unsigned max = myCache->getkey_max();

		//进行文件的写入
		//写入时间戳
		data_file.write(reinterpret_cast<char *>(&(this->timeStamp)),8);
		//写入key的个数
		data_file.write(reinterpret_cast<char *>(&(this->key_count)),8);
		//写入min和max
		data_file.write(reinterpret_cast<char *>(&min),8);
		data_file.write(reinterpret_cast<char *>(&max),8);
		//这相当与这条指令的二进制写入data_file << this->timeStamp << this->key_count << this->Memtable.getMinkey() << this->Memtable.getMaxkey();
		//write Bloom filter
		for(int i = 0;i < 10240;++i){
			//writeIn = reinterpret_cast<char *>(&(Bloom[i]));
			data_file.write(reinterpret_cast<char *>(&(Bloom[i])),1);
		}
		SKNode *p = this->Memtable.getMinEle();
		SKNode *q = p;
		//write key and offset
		for(int i = 0;i < key_count;++i){
			if(!p){
				cout << "seg: " << i << endl;
				cout << "key_count " << key_count << endl;
 			}
			data_file.write(reinterpret_cast<char *>(&(p->key)),8);
			data_file.write(reinterpret_cast<char *>(&(offset)),4);
			//这相当与这条指令的二进制写入data_file << p->key << offset;
			offset += p->val.length();
			p = p->forwards[0];
		}

		//write value
		for(int i = 0;i < key_count;++i){
			data_file.write(q->val.c_str(),q->val.length());
			//这相当与这条指令的二进制写入data_file << q->val;
			q = q->forwards[0];
		}

}

void KVStore::resetBloom()
{
	for(int i = 0;i < 10240;++i){
		Bloom[i] = 0;
	}
}


/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{
	string val = this->Memtable.Search(key);
	if(val != ""){
		return val;
	}
	else{
		//from end to begin,because that can find the most updated data
		//cout << "distance : " << acache.end() - acache.begin() <<endl;
		for(vector<SSTablecache*>::iterator iter=acache.end()-1;iter != acache.begin()-1;iter--){
			int mes[2] ={0};
			//used for debug
			//cout << "timestamp  : " <<  iter->getTime() << endl;

        	if((*iter)->Search(key,mes)){
				// cout << "find : " << find_count << endl;
				int num = iter - acache.begin();//算出是第几个文件
				string dir = this->getDir();
				string fileroad = "/level-0/data";
				string timenum = std::to_string((*iter)->getTime());
				//cout << timenum << endl;
				fileroad = fileroad + timenum;
				fileroad = fileroad + ".sst";
				string file_path = dir + fileroad;
				int offset = mes[0];
				unsigned long long length = mes[1];
				ifstream read_file(file_path,ios::in|ios::binary);
				read_file.seekg(offset,ios::beg);
				if(read_file.eof()){
					cout << "wrong!" << endl;
				}
				char *buffer = new char[length + 1];
				read_file.read(buffer,length);
				buffer[length] = '\0';
				string ans = buffer;
				delete buffer;
				if(ans == "~DELETED~"){
					return "";
				}
				else{
					return ans;
				}
			}
    	}
		return "";
	}
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
	string val = this->get(key);
	//先判断字符串存不存在
	if(val == ""){
		//不存在则return false
		return false;
	}
	if(val == "~DELETED~"){
		return false;
	}

	bool jug = this->Memtable.Delete(key);//返回true代表跳表中有并且已经插入，若为false则代表跳表中没有需要手动put
	if(jug){
		return true;
	}
	else{
		this->put(key,"~DELETED~");
	}
	return true;
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