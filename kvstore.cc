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
		//文件大小超标，开始向有硬盘写入数据
		string dir = this->getDir();

		string fileroad = "/level-0/data";
		string timenum = std::to_string(this->timeStamp);
		//cout << timenum << endl;
		fileroad = fileroad + timenum;
		fileroad = fileroad + ".sst";

		string file_path = dir + fileroad;
		ofstream data_file(file_path,ios::out | ios::binary);
		//write Header
		this->key_count = this->Memtable.getKetcount();
		const char *writeIn = reinterpret_cast<char *>(&(this->timeStamp));
		//写入时间戳
		data_file.write(reinterpret_cast<char *>(&(this->timeStamp)),8);
		//写入key的个数
		writeIn = reinterpret_cast<char *>(&(this->key_count));
		data_file.write(reinterpret_cast<char *>(&(this->key_count)),8);

		unsigned min = this->Memtable.getMinkey();
		unsigned max = this->Memtable.getMaxkey();
		//写入min和max
		writeIn = reinterpret_cast<char *>(&min);
		data_file.write(reinterpret_cast<char *>(&min),8);
		writeIn = reinterpret_cast<char *>(&max);
		data_file.write(reinterpret_cast<char *>(&max),8);
		//这相当与这条指令的二进制写入data_file << this->timeStamp << this->key_count << this->Memtable.getMinkey() << this->Memtable.getMaxkey();
		//write Bloom filter
		for(int i = 0;i < 10240;++i){
			//writeIn = reinterpret_cast<char *>(&(Bloom[i]));
			data_file.write(reinterpret_cast<char *>(&(Bloom[i])),1);
		}

		int offset = 32 + 10*1024 + key_count * 12;//the offset of the first element
		int put_offset = offset;
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

		int a = offset;
		//write to sstablecache as the cache
		SSTablecache *myCache = new SSTablecache(this->timeStamp,this->key_count,this->Memtable.getMinkey(),this->Memtable.getMaxkey(),
		this->Memtable.getMinEle(),this->Bloom,put_offset);
		//push the cache to the cache vector
		this->acache.push_back(myCache);


		//write value
		for(int i = 0;i < key_count;++i){
			data_file.write(q->val.c_str(),q->val.length());
			//这相当与这条指令的二进制写入data_file << q->val;
			q = q->forwards[0];
		}

		//clean the MemTable
		this->Memtable.cleanMem();

		//clean the Bloom filter
		resetBloom();
		
		//used for debug
		//cout << "timestamp  : " <<  this->timeStamp << endl;

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

		//some code used for debug
		// ifstream read_file(file_path);
		// read_file.seekg(33096,ios::beg);
		// if(read_file.eof()){
		// 	cout << "wrong!" << endl;
		// }
		// char *buffer = new char[2];
		// read_file.read(buffer,1);
		// buffer[1] = '\0';
		
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
	if(key == 1901){
		int debug = 0;
	}
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
			if(iter == acache.begin()){
				int debug = 0;
			}

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