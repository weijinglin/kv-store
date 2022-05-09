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
		w_file(myCache);

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
			Merge_l_zero(la_box,this_level,tiny_cache);
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
	int ca_count = 0;//统计当前的a_cache所指的位置
	sort_vec(s);//保证vector中块间的有序

	uint64_t seq_index = 0;//指向seq_kv的下标
	uint64_t ss_index = 0;//指向SSTable的下标
	uint64_t num = 0;//统计现在在哪个SSTable
	uint64_t bytes = 10240 + 32;//统计现在的byte数目
	uint64_t by_bef;//bytes的预检测变量

	bool end_flag = false;

	while (true)
	{
		/* code */
		//先进行比较
		if(seq_kv[seq_index].data.key < s.at(num)->get_pair(ss_index).key){
			if(ca_count > 0){
				if(seq_kv[seq_index].data.key == a_cache[ca_count-1].data.key){
					if(seq_kv[seq_index].timestamp > a_cache[ca_count-1].timestamp){
						by_bef = bytes - a_cache[ca_count-1].data.length + seq_kv[seq_index].data.length;
						if(by_bef > 2 * 1024 * 1024){
							//长度超标了
							//需要做的事情：1，调整bytes，并且把最后一个元素删掉（因为timestamp太小）
							//			   2，把a_cache恢复到初始状态
							//			   3，调整ca_count
							bytes = bytes - a_cache[ca_count-1].data.length;
							a_cache[ca_count-1].index = -1;
							ca_count -= 1;
							fill_mem(a_cache,ca_count,mem);
							ca_count = 0;
							a_cache[ca_count] = seq_kv[seq_index];
							bytes = 10240 + 32 + seq_kv[seq_index].data.length + KEY_LENGTH + OFFSET_LENGTH;
							ca_count++;
						}
						else{
							a_cache[ca_count-1] = seq_kv[seq_index];
							bytes = by_bef;
						}
						seq_index++;
					}
				}
				else{
					by_bef = bytes + KEY_LENGTH + OFFSET_LENGTH + a_cache[ca_count].data.length;
					if(by_bef > 2 * 1024 * 1024){
							//长度超标了
							//需要做的事情：1，调整bytes，并且把最后一个元素删掉（因为timestamp太小）
							//			   2，把a_cache恢复到初始状态
							//			   3，调整ca_count
							fill_mem(a_cache,ca_count,mem);
							ca_count = 0;
							a_cache[ca_count] = seq_kv[seq_index];
							bytes = 10240 + 32 + seq_kv[seq_index].data.length + KEY_LENGTH + OFFSET_LENGTH;
					}
					else{
						a_cache[ca_count] = seq_kv[seq_index];
						bytes = by_bef;
					}

					ca_count++;
					seq_index++;
				}
			}
			else{
				//只要输入的数据不会很长，应该不会第一次就超了把，先放着，后面再改
				a_cache[ca_count] = seq_kv[seq_index];
				bytes = bytes + KEY_LENGTH + OFFSET_LENGTH + a_cache[ca_count].data.length;
				ca_count++;

				seq_index++;
			}
		}
		else if(seq_kv[seq_index].data.key > s.at(num)->get_pair(ss_index).key){
			if(ca_count > 0){
				if(s.at(num)->get_pair(ss_index).key == a_cache[ca_count-1].data.key){
					if(s.at(num)->getTime() > a_cache[ca_count-1].timestamp){
						by_bef = bytes - a_cache[ca_count-1].data.length + s.at(num)->get_pair(ss_index).length;
						if(by_bef > 2 * 1024 * 1024){
							//长度超标了
							//需要做的事情：1，调整bytes，并且把最后一个元素删掉（因为timestamp太小）
							//			   2，把a_cache恢复到初始状态
							//			   3，调整ca_count
							bytes = bytes - a_cache[ca_count-1].data.length;
							a_cache[ca_count-1].index = -1;
							ca_count -= 1;
							fill_mem(a_cache,ca_count,mem);
							ca_count = 0;
							a_cache[ca_count].data = s.at(num)->get_pair(ss_index);
							a_cache[ca_count].index = s.at(num)->getindex();
							a_cache[ca_count].level = s.at(num)->getlevel();
							a_cache[ca_count].timestamp = s.at(num)->getTime();
							bytes = 10240 + 32 + s.at(num)->get_pair(ss_index).length + KEY_LENGTH + OFFSET_LENGTH;
							ca_count++;
						}
						else{
							a_cache[ca_count-1].data = s.at(num)->get_pair(ss_index);
							a_cache[ca_count-1].index = s.at(num)->getindex();
							a_cache[ca_count-1].level = s.at(num)->getlevel();
							a_cache[ca_count-1].timestamp = s.at(num)->getTime();

							bytes = by_bef;
							ca_count++;
						}

						ss_index++;
					}
				}
				else{
					by_bef = bytes + KEY_LENGTH + OFFSET_LENGTH + s.at(num)->get_pair(ss_index).length;
					if(by_bef > 2 * 1024 * 1024){
						//长度超标了
						//需要做的事情：1，调整bytes，并且把最后一个元素删掉（因为timestamp太小）
						//			   2，把a_cache恢复到初始状态
						//			   3，调整ca_count
						fill_mem(a_cache,ca_count,mem);
						ca_count = 0;
						a_cache[ca_count].data = s.at(num)->get_pair(ss_index);
						a_cache[ca_count].index = s.at(num)->getindex();
						a_cache[ca_count].level = s.at(num)->getlevel();
						a_cache[ca_count].timestamp = s.at(num)->getTime();
						bytes = 10240 + 32 + s.at(num)->get_pair(ss_index).length + KEY_LENGTH + OFFSET_LENGTH;
						ca_count++;
					}
					else{
						a_cache[ca_count].data = s.at(num)->get_pair(ss_index);
						a_cache[ca_count].index = s.at(num)->getindex();
						a_cache[ca_count].level = s.at(num)->getlevel();
						a_cache[ca_count].timestamp = s.at(num)->getTime();
						ca_count++;
						bytes = by_bef;
					}

					ss_index++;
				}
			}
			else{
				//只要输入的数据不会很长，应该不会第一次就超了把，先放着，后面再改
				a_cache[ca_count].data = s.at(num)->get_pair(ss_index);
				a_cache[ca_count].index = s.at(num)->getindex();
				a_cache[ca_count].level = s.at(num)->getlevel();
				a_cache[ca_count].timestamp = s.at(num)->getTime();
				bytes = bytes + KEY_LENGTH + OFFSET_LENGTH + a_cache[ca_count].data.length;
				ca_count++;

				ss_index++;
			}
		}
		else{
			//相等的情况
			//先比较时间戳
			if(seq_kv[seq_index].timestamp > s.at(num)->getTime()){
				if(ca_count > 0){
					if(seq_kv[seq_index].data.key == a_cache[ca_count-1].data.key){
						if(seq_kv[seq_index].timestamp > a_cache[ca_count-1].timestamp){
							by_bef = bytes - a_cache[ca_count-1].data.length + seq_kv[seq_index].data.length;
							if(by_bef > 2 * 1024 *1024){
								//长度超标了
								//需要做的事情：1，调整bytes，并且把最后一个元素删掉（因为timestamp太小）
								//			   2，把a_cache恢复到初始状态
								//			   3，调整ca_count
								bytes = bytes - a_cache[ca_count-1].data.length;
								a_cache[ca_count-1].index = -1;
								ca_count -= 1;
								fill_mem(a_cache,ca_count,mem);
								ca_count = 0;
								a_cache[ca_count] = seq_kv[seq_index];
								bytes = 10240 + 32 + seq_kv[seq_index].data.length + KEY_LENGTH + OFFSET_LENGTH;
								ca_count++;
							}
							else{
								a_cache[ca_count-1] = seq_kv[seq_index];
								bytes = by_bef;
							}
						}
					}
					else{
						by_bef = bytes + KEY_LENGTH + OFFSET_LENGTH + a_cache[ca_count].data.length;
						if(by_bef > 2 * 1024 * 1024){
							//长度超标了
							//需要做的事情：1，调整bytes，并且把最后一个元素删掉（因为timestamp太小）
							//			   2，把a_cache恢复到初始状态
							//			   3，调整ca_count
							fill_mem(a_cache,ca_count,mem);
							ca_count = 0;
							a_cache[ca_count] = seq_kv[seq_index];
							bytes = 10240 + 32 + KEY_LENGTH + OFFSET_LENGTH + seq_kv[seq_index].data.length;
						}
						else{
							a_cache[ca_count] = seq_kv[seq_index];
							bytes = by_bef;
						}
						ca_count++;
					}
				}	
				else{
					//只要输入的数据不会很长，应该不会第一次就超了把，先放着，后面再改
					a_cache[ca_count] = seq_kv[seq_index];
					bytes = bytes + KEY_LENGTH + OFFSET_LENGTH + a_cache[ca_count].data.length;
					ca_count++;
				}
				seq_index++;
				ss_index++;
			}
			else{
				if(ca_count > 0){
					if(s.at(num)->get_pair(ss_index).key == a_cache[ca_count-1].data.key){
						if(s.at(num)->getTime() > a_cache[ca_count-1].timestamp){
							by_bef = bytes - a_cache[ca_count-1].data.length + s.at(num)->get_pair(ss_index).length;
							if(by_bef > 2 * 1024 * 1024){
								//长度超标了
								//需要做的事情：1，调整bytes，并且把最后一个元素删掉（因为timestamp太小）
								//			   2，把a_cache恢复到初始状态
								//			   3，调整ca_count
								bytes = bytes - a_cache[ca_count-1].data.length;
								a_cache[ca_count-1].index = -1;
								ca_count -= 1;
								fill_mem(a_cache,ca_count,mem);
								ca_count = 0;
								a_cache[ca_count].data = s.at(num)->get_pair(ss_index);
								a_cache[ca_count].index = s.at(num)->getindex();
								a_cache[ca_count].level = s.at(num)->getlevel();
								a_cache[ca_count].timestamp = s.at(num)->getTime();
								bytes = 10240 + 32 + s.at(num)->get_pair(ss_index).length + KEY_LENGTH + OFFSET_LENGTH;
								ca_count++;
							}
							else{
								a_cache[ca_count-1].data = s.at(num)->get_pair(ss_index);
								a_cache[ca_count-1].index = s.at(num)->getindex();
								a_cache[ca_count-1].level = s.at(num)->getlevel();
								a_cache[ca_count-1].timestamp = s.at(num)->getTime();
								
								bytes = by_bef;
							}
							ss_index++;
						}
					}
					else{
						by_bef = bytes + KEY_LENGTH + OFFSET_LENGTH + s.at(num)->get_pair(ss_index).length;
						if(by_bef > 2 * 1024 * 1024){
							//长度超标了
							//需要做的事情：1，调整bytes，并且把最后一个元素删掉（因为timestamp太小）
							//			   2，把a_cache恢复到初始状态
							//			   3，调整ca_count
							fill_mem(a_cache,ca_count,mem);
							ca_count = 0;
							a_cache[ca_count].data = s.at(num)->get_pair(ss_index);
							a_cache[ca_count].index = s.at(num)->getindex();
							a_cache[ca_count].level = s.at(num)->getlevel();
							a_cache[ca_count].timestamp = s.at(num)->getTime();
							bytes = 10240 + 32 + s.at(num)->get_pair(ss_index).length + KEY_LENGTH + OFFSET_LENGTH;
							ca_count++;
						}
						else{
							a_cache[ca_count].data = s.at(num)->get_pair(ss_index);
							a_cache[ca_count].index = s.at(num)->getindex();
							a_cache[ca_count].level = s.at(num)->getlevel();
							a_cache[ca_count].timestamp = s.at(num)->getTime();
							ca_count++;
							bytes = by_bef;
						}

						ss_index++;
					}
				}
				else{
					//只要输入的数据不会很长，应该不会第一次就超了把，先放着，后面再改
					a_cache[ca_count].data = s.at(num)->get_pair(ss_index);
					a_cache[ca_count].index = s.at(num)->getindex();
					a_cache[ca_count].level = s.at(num)->getlevel();
					a_cache[ca_count].timestamp = s.at(num)->getTime();
					bytes = bytes + KEY_LENGTH + OFFSET_LENGTH + a_cache[ca_count].data.length;
					ca_count++;

					ss_index++;
				}
				seq_index++;
			}
		}
	
		//要进行边界的检测
		if(ss_index == s.at(num)->getkey_Count()){
			num++;
			ss_index = 0;
			if(num >= s.size()){
				end_flag = true;
				break;
			}
		}
		if(seq_kv[seq_index].index == -1){
			break;
		}
	}

	//根据end_flag的值进行不同的操作
	if(end_flag){
		//SSTable被遍历完的情况
		//第一步，先把值赋值到a_cache中（采用循环的方式）
		//第二步，如果超过了数量上限就放到vector中
		while(true){
			//根据键值是否重复可以分成两种情况
			if(a_cache[ca_count-1].data.key == seq_kv[seq_index].data.key){
				//大致思路：先判断时间戳，再判断bytes是否会超标，根据结果进行不同的操作
				if(a_cache[ca_count-1].timestamp < seq_kv[seq_index].timestamp){
					by_bef = bytes + seq_kv[seq_index].data.length - a_cache[ca_count-1].data.length;
					if(by_bef > 2 * 1024 * 1024){
						//长度超标了
						//需要做的事情：1，调整bytes，并且把最后一个元素删掉（因为timestamp太小）
						//			   2，把a_cache恢复到初始状态
						//			   3，调整ca_count
						bytes = bytes - a_cache[ca_count-1].data.length;
						a_cache[ca_count-1].index = -1;
						ca_count -= 1;
						fill_mem(a_cache,ca_count,mem);
						ca_count = 0;
						a_cache[ca_count] = seq_kv[seq_index];
						bytes = 10240 + 32 + seq_kv[seq_index].data.length + KEY_LENGTH + OFFSET_LENGTH;
						ca_count++;
					}
					else{
						a_cache[ca_count-1] = seq_kv[seq_index];
						bytes = by_bef;
					}
					seq_index++;
				}
			}
			else{
				by_bef = bytes + seq_kv[seq_index].data.length + KEY_LENGTH + OFFSET_LENGTH;
				if(by_bef > 2 (* 1024 * 1024)){
					//长度超标了
					//需要做的事情：1，调整bytes，并且把最后一个元素删掉（因为timestamp太小）
					//			   2，把a_cache恢复到初始状态
					//			   3，调整ca_count
					fill_mem(a_cache,ca_count,mem);
					ca_count = 0;
					a_cache[ca_count] = seq_kv[seq_index];
					bytes = 10240 + 32 + seq_kv[seq_index].data.length + KEY_LENGTH + OFFSET_LENGTH;
				}
				else{
					a_cache[ca_count] = seq_kv[seq_index];
					bytes = by_bef;
				}
				ca_count++;
				seq_index++;
			}

			//进行检测
			if(seq_kv[seq_index].index == -1){
				fill_mem(a_cache,ca_count,mem);
				break;
			}
		}

	}
	else{
		//seq_kv被遍历完的情况
		//第一步，先把值赋值到a_cache中（采用循环的方式）
		//第二步，如果超过了数量上限就放到vector中
		while(true){
			if(s.at(num)->get_pair(ss_index).key == a_cache[ca_count-1].data.key){
					if(s.at(num)->getTime() > a_cache[ca_count-1].timestamp){
						by_bef = bytes - a_cache[ca_count-1].data.length + s.at(num)->get_pair(ss_index).length;
						if(by_bef > 2 * 1024 * 1024){
							//长度超标了
							//需要做的事情：1，调整bytes，并且把最后一个元素删掉（因为timestamp太小）
							//			   2，把a_cache恢复到初始状态
							//			   3，调整ca_count
							bytes = bytes - a_cache[ca_count-1].data.length;
							a_cache[ca_count-1].index = -1;
							ca_count -= 1;
							fill_mem(a_cache,ca_count,mem);
							ca_count = 0;
							a_cache[ca_count].data = s.at(num)->get_pair(ss_index);
							a_cache[ca_count].index = s.at(num)->getindex();
							a_cache[ca_count].level = s.at(num)->getlevel();
							a_cache[ca_count].timestamp = s.at(num)->getTime();
							bytes = 10240 + 32 + s.at(num)->get_pair(ss_index).length + KEY_LENGTH + OFFSET_LENGTH;
							ca_count++;
						}
						else{
							a_cache[ca_count-1].data = s.at(num)->get_pair(ss_index);
							a_cache[ca_count-1].index = s.at(num)->getindex();
							a_cache[ca_count-1].level = s.at(num)->getlevel();
							a_cache[ca_count-1].timestamp = s.at(num)->getTime();

							bytes = by_bef;
							ca_count++;
						}

						ss_index++;
					}	
			}
			else{
					by_bef = bytes + KEY_LENGTH + OFFSET_LENGTH + s.at(num)->get_pair(ss_index).length;
					if(by_bef > 2 * 1024 * 1024){
						//长度超标了
						//需要做的事情：1，调整bytes，并且把最后一个元素删掉（因为timestamp太小）
						//			   2，把a_cache恢复到初始状态
						//			   3，调整ca_count
						fill_mem(a_cache,ca_count,mem);
						ca_count = 0;
						a_cache[ca_count].data = s.at(num)->get_pair(ss_index);
						a_cache[ca_count].index = s.at(num)->getindex();
						a_cache[ca_count].level = s.at(num)->getlevel();
						a_cache[ca_count].timestamp = s.at(num)->getTime();
						bytes = 10240 + 32 + s.at(num)->get_pair(ss_index).length + KEY_LENGTH + OFFSET_LENGTH;
						ca_count++;
					}
					else{
						a_cache[ca_count].data = s.at(num)->get_pair(ss_index);
						a_cache[ca_count].index = s.at(num)->getindex();
						a_cache[ca_count].level = s.at(num)->getlevel();
						a_cache[ca_count].timestamp = s.at(num)->getTime();
						ca_count++;
						bytes = by_bef;
					}

					ss_index++;
				}

			//进行检测
			if(ss_index == s.at(num)->getkey_Count()){
				num++;
				if(num == s.size()){
					fill_mem(a_cache,ca_count,mem);
					break;
				}
			}
		}	
	}
}

//给出一个结构体数组，根据这个结构体数组从不同的文件中读东西到内存中（SkipList中）
void KVStore::fill_mem(kv_box* gen,uint64_t count,vector<SkipList *> &mem)
{
	//用于定位文件中的字串
	int in_offset;
	uint64_t in_key;
	int in_length;
	
	//用于定位对应文件的位置
	int in_timestamp;
	int in_index;
	int in_level;

	SkipList* in_mem = new SkipList();
	for(int i = 0;i < count;++i){
		string dir = this->getDir();
		in_level = gen[i].level;
		in_index = gen[i].index;
		in_timestamp = gen[i].timestamp;
		
		in_offset = gen[i].data.offset;
		in_key = gen[i].data.key;
		in_length = gen[i].data.length;

		string fileroad = fname_gen(in_level,in_timestamp,in_index);
		string file_path = dir + fileroad;

		ifstream read_file(file_path,ios::in|ios::binary);
		read_file.seekg(in_offset,ios::beg);
		if(read_file.eof()){
			cout << "wrong!" << endl;
		}
		char *buffer = new char[in_length + 1];
		read_file.read(buffer,in_length);
		buffer[in_length] = '\0';
		string ans = buffer;
		delete buffer;

		in_mem->Insert(in_key,ans);

		//对这个数组的元素进行清空
		gen[i].index = -1;
		gen[i].level = -1;
		gen[i].timestamp = -1;

	}

	mem.push_back(in_mem);
}

//先使用稳定的冒泡排序，后面可以再优化
void sort_vec(vector<SSTablecache *> &s){
	SSTablecache* compare;
	bool flag = true;
	for(int i = 0;i < s.size() && flag;++i){
		flag = false;
		for(int j = 0;j < s.size()-i - 1;++j){
			if(s.at(j)->getkey_min() > s.at(j+1)->getkey_min()){
				compare = s.at(j);
				s.at(j) = s.at(j+1);
				s.at(j+1) = compare;
				flag = true;
			}		
		}
	}
	return;

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

//定义文件名的映射关系
string fname_gen(int level,int timestamp,int index){
	string file_level = std::to_string(level);
	string file_road = "/level-" + file_level;
	string fileroad = file_road + "/data";
	string timenum = std::to_string(timestamp);
	timenum = timenum + "-";
	timenum = timenum + std::to_string(index);
	fileroad = fileroad + timenum + ".sst";
	return fileroad;
}

void KVStore::w_file(SSTablecache* myCache)
{
		//文件大小超标，开始向有硬盘写入数据
		string dir = this->getDir();

		string fileroad = fname_gen(myCache->getlevel(),myCache->getTime(),myCache->getindex());

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
				string fileroad = fname_gen((*iter)->getlevel(),(*iter)->getTime(),(*iter)->getindex());
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