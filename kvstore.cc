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
		
		//deal with compaction
		//do_Compac(myCache);
		//这里采用先插入后优化的策略
		int file_count = this->all_level.at(0)->getCount();
		myCache->setindex(file_count);
		myCache->setlevel(0);
		w_file(myCache,file_count,0);

		//now check for the compaction
		do_Compac();

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

void KVStore::do_Compac()
{
	//首先进行判定
	if(this->all_level.at(0)->getCount() >= 3){
		//触发compaction
		int check_level = 1;//used to check for all the level compaction condition
		//先处理level = 0的情况
		vector<SSTablecache*> last_level;//存上一层有问题的SSTable
		vector<SSTablecache*> this_level;//存这一层被命中的SSTable
		//把level-0的所有SSTable都进行compaction的处理
		for(int i =0;i < this->all_level.at(0)->getCount();++i){
			last_level.push_back(this->all_level.at(0)->find_cache(i));
		}
		uint64_t k_min = get_minkey(last_level);
		uint64_t k_max = get_maxkey(last_level);
		//把level-1中符合要求的SSTable写入
		for(int i = 0;i < this->all_level.at(1)->getCount();++i){
			if(isCover(k_min,k_max,this->all_level.at(1)->find_cache(i)->getkey_min(),this->all_level.at(1)->find_cache(i)->getkey_max())){
				this_level.push_back(this->all_level.at(1)->find_cache(i));
			}
		}

		//正式开始处理
		vector<SkipList *> tiny_cache;//用于缓存将要写入level-1的内容
		kv_box* a_cache = new kv_box((2*1024*1024-10240-32)/12);//存储tiny_cache获取数据的所需要的信息
	
		//定义对应的指针并且进行初始化
		uint64_t counter = 0;
		uint64_t* la_pointer = new uint64_t(last_level.size());
		bool *unused = new bool(last_level.size());//用于判断第i个SSTable是否被遍历完
		for(int i = 0;i < last_level.size();++i){
			la_pointer[i] = 0;
			unused[i] = false;
			counter += this->all_level.at(0)->find_cache(i)->getkey_Count();
		}

		kv_box *la_box = new kv_box(counter);
		uint64_t count = 0;
		//先对level-0进行归并排序
		while(true){
			int hit = -1;
			int t_off = 0;
			int len = 0;
			int tmp_min = UINT64_MAX;
			bool jug = true;
			for(int i= 0;i < last_level.size();++i){
				if(tmp_min > this->all_level.at(0)->find_cache(i)->get_pair(la_pointer[i]).key){
					tmp_min = this->all_level.at(0)->find_cache(i)->get_pair(la_pointer[i]).key;
					t_off = this->all_level.at(0)->find_cache(i)->get_pair(la_pointer[i]).offset;
					len = this->all_level.at(0)->find_cache(i)->get_pair(la_pointer[i]).length;
					hit = i;
				}
			}

			la_box[count].index = this->all_level.at(0)->find_cache(hit)->getindex();
			la_box[count].data.key = tmp_min;
			la_box[count].level = 0;
			la_box[count].timestamp = this->all_level.at(0)->find_cache(hit)->getTime();
			la_box[count].data.offset = t_off;
			la_box[count].data.length = len;

			//更新循环参量
			count++;
			la_pointer[hit]++;

			for(int i= 0;i < last_level.size();++i){
				if(la_pointer[i] == last_level.at(i)->getkey_Count()){
					unused[i] = true;
				}
			}
			for(int i= 0;i < last_level.size();++i){
				jug = jug && unused[i];
			}
			if(jug){
				break;
			}
		}

		//进行level-0的简单的Merge,由于块内有序，可以把level-n(n > 0)当作块内有序
		if(this_level.size() > 0){
			Merge_l_zero(la_box,this_level,tiny_caches);
		}

		//后处理level >= 1的情况 
		while(this->all_level.at(check_level)->getCount() >= (1 << (check_level+1) + 1)){

		}
	}
}

void KVStore::Merge_l_zero(kv_box *seq_kv,vector<SSTablecache *> &s,vector<SkipList*> &mem)
{
	//进行一个两路的归并排序
	kv_box* a_cache = new kv_box((2*1024*1024-10240-32)/12);//存储tiny_cache获取数据的所需要的信息
	kv_box* compare = s.at(0)->to_kv_box();
	
}

bool isCover(uint64_t k_min,uint64_t k_max,uint64_t ck_min,uint64_t ck_max){
	if(k_max < ck_min){
		return false;
	}
	if(k_min > ck_max){
		return false;
	}
	return true;
}

uint64_t get_minkey(vector<SSTablecache*> &s)
{
	int min = UINT64_MAX;
	for(int i = 0;i < s.size();++i){
		if(min > s.at(i)->getkey_min()){
			min = s.at(i)->getkey_min();
		}
	}
	return min;
}

uint64_t get_maxkey(vector<SSTablecache*> &s)
{
	int max = -1;
	for(int i = 0;i < s.size();++i){
		if(max < s.at(i)->getkey_min()){
			max = s.at(i)->getkey_min();
		}
	}
	return max;
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