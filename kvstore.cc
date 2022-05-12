#include "kvstore.h"
#include <string>
#include "utils.h"
#include <iostream>
#include <fstream>
#include "MurmurHash3.h"

using namespace std;

uint64_t get_minkey(vector<SSTablecache*> &s);

uint64_t get_maxkey(vector<SSTablecache*> &s);

bool* gen_bloom(SkipList* in_mem);

bool isCover(uint64_t k_min,uint64_t k_max,uint64_t ck_min,uint64_t ck_max);

void sort_vec(vector<SSTablecache *> &s);

string fname_gen(int level,int timestamp,int index);

void parse_filename(string file_name,int &timestamp,uint64_t &index)
{
    int commaPos = file_name.find('-');
    timestamp = atoi(file_name.substr(4, commaPos).c_str());
    index = atoi(file_name.substr(commaPos + 1).c_str());
}

uint64_t get_level_num(vector<string> &f,int count){
    uint64_t max = 0;
    int time;
    uint64_t index;
    for(int i = 0;i < count;++i){
        parse_filename(f.at(i),time,index);
        if(max < index){
            max = index;
        }
    }
    return max;
}

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

    vector<string> file_name;
    //进行loading的操作
    int counter = 0;
    while (true)
    {
        /* code */
        //每个目录进行检查来载入对应的文件
        string dir_name = copy + "/level-" + std::to_string(counter);
        if(utils::dirExists(dir_name)){
            //进行loading的操作
            this->level++;
            int file_count = utils::scanDir(dir_name,file_name);

            if(file_count == 0){
                Level *new_level = new Level(counter);
                this->all_level.push_back(new_level);
                counter++;
                continue;
            }

            //对于得到的文件进行解析
            uint64_t Num = get_level_num(file_name,file_count);
            Level *new_level = new Level(counter);
            this->all_level.push_back(new_level);
            new_level->setNum(Num);
            SSTablecache *new_table;
            for(int i = 0;i < file_count;++i){
                ifstream read_file(dir_name + "/" + file_name.at(i),ios::binary);

                //parse the file name
                string name_file = file_name.at(i);
                int time;
                uint64_t index;
                parse_filename(name_file,time,index);
                if(time > this->timeStamp){
                    this->timeStamp = time;
                }
                uint64_t key_num;
                uint64_t key_min;
                uint64_t key_max;
                uint64_t time_buf;
                read_file.read((char*)&time_buf,sizeof (time_buf));
                read_file.read((char*)&key_num,sizeof (key_num));
                read_file.read((char*)&key_min,sizeof (key_min));
                read_file.read((char*)&key_max,sizeof (key_max));

                SkipList in_mem;

                vector<uint64_t> keys_arr;
                keys_arr.resize(key_num);
                vector<int> off_arr;
                off_arr.resize(key_num);

                //读取布隆过滤器
                bool *filter = new bool[10240];

                read_file.read((char*)filter,10240);

                for(unsigned int i = 0;i < key_num;++i){
                    uint64_t key;
                    int off;
                    read_file.read((char*)&(key),8);
                    read_file.read((char*)&(off),4);
                    keys_arr[i] = key;
                    off_arr[i] = off;
                }
                //得到了所有的key和value
                //现在开始进行插入
                char* buf = new char[2 * 1024 * 1024 - 10240 - 32];
                read_file.read(buf,2 * 1024 * 1024 - 10240 - 32);
                int all_read = read_file.gcount();
                buf[all_read] = '\0';

                int read_pos = 0;
                char* read_in;
                //开始对字符串进行解析
                for(unsigned int i = 0;i < key_num - 1;++i){
                    int length = off_arr[i+1] - off_arr[i];
                    read_in = new char[length + 1];
                    memcpy(read_in,buf+read_pos,length);
                    read_in[length] = '\0';
                    string in_string = read_in;
                    in_mem.Insert(keys_arr[i],in_string);
                    delete [] read_in;
                    read_pos += length;
                }
                //对于最后一段的读写
                int length = all_read - off_arr[key_num-1] + off_arr[0];

                read_in = new char[length+1];
                memcpy(read_in,buf+read_pos,length);
                read_in[length] = '\0';
                string in_string = read_in;
                in_mem.Insert(keys_arr[key_num-1],in_string);
                delete [] read_in;

                new_table = new SSTablecache(time,key_num,key_min,key_max,in_mem.getMinEle(),gen_bloom(&in_mem),off_arr[0]);
                this->all_level.at(counter)->set_ele(new_table,index);
                delete [] buf;
            }
            file_name.clear();
            counter++;
        }
        else{
            break;
        }
    }

}

std::string KVStore::getDir(){
    return this->rootDir;
}

//要进行数据的保存
KVStore::~KVStore()
{
    this->key_count = this->Memtable.getKetcount();
    SSTablecache *myCache = new SSTablecache(this->timeStamp,this->key_count,this->Memtable.getMinkey(),
    this->Memtable.getMaxkey(),this->Memtable.getMinEle(),this->Bloom,10240+32 + 12*this->key_count);
    myCache->setlevel(0);

    if(level == 0){
        Level* new_level = new Level(0);
        this->all_level.push_back(new_level);
        string sstable = this->getDir() + "/level-0";
        int result = utils::mkdir(sstable.c_str());
        if(result){
            exit(0);
        }
        level++;
    }
    this->all_level.at(0)->put_SSTable((myCache));
    w_file(myCache);
    do_Compac();
    for(unsigned int i = 0;i < this->all_level.size();++i){
        for(unsigned int j = 0;j < this->all_level.at(i)->getCount();++j){
            delete this->all_level.at(i)->find_cache(j);
        }
    }
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
        int offset = 32 + 10*1024 + key_count * 12;//the offset of the first element
        int put_offset = offset;

        //write to sstablecache as the cache
        SSTablecache *myCache = new SSTablecache(this->timeStamp,this->key_count,this->Memtable.getMinkey(),this->Memtable.getMaxkey(),
        this->Memtable.getMinEle(),this->Bloom,put_offset);
        //push the cache to the cache vector

        //deal with compaction
        //do_Compac(myCache);
        //这里采用先插入后优化的策略
        myCache->setlevel(0);

        if(level == 0){
            Level* new_level = new Level(0);
            this->all_level.push_back(new_level);
            string sstable = this->getDir() + "/level-0";
            int result = utils::mkdir(sstable.c_str());
            if(result){
                exit(0);
            }
            level++;
        }

        this->all_level.at(0)->put_SSTable(myCache);

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

bool* gen_bloom(SkipList* in_mem){
    bool* Bloom_out = new bool[10240];
    unsigned int hash[4] = {0};
    unsigned long long myKey;

    int length = in_mem->getKetcount();
    int counter = 0;

    SKNode* p = in_mem->getMinEle();
    while(counter < length){
        counter++;
        myKey = p->key;
        MurmurHash3_x64_128(&myKey,sizeof(myKey), 1, hash);
        for(int i = 0;i < 4;++i){
            Bloom_out[hash[i] % 10240] = true;
        }
        p = p->forwards[0];
    }
    return Bloom_out;
}


//给定所有的key-value对，产生对应的SSTable列表
void KVStore::gen_table_kv(kv* mem,uint64_t len,vector<SSTablecache*> &s_list,vector<SkipList*> &skip)
{
    uint64_t bytes = 10240 + 32;//用于判定是否构造新的SSTable
    uint64_t by_bef = bytes;

    SSTablecache* in_table;
    bool* in_Bloom;
    uint64_t in_count;
    uint64_t time = this->timeStamp;

    SkipList *in_mem = new SkipList();

    for(unsigned int i = 0;i < len;++i){
        by_bef = bytes + KEY_LENGTH + OFFSET_LENGTH + mem[i].value.length();
        if(i > 9200){
            uint64_t a = mem[i].key;
            string val = mem[i].value;
            int b= 0;
            b = a;
        }
        if(by_bef > 2 * 1024 * 1024){
            in_count = in_mem->getKetcount();
            in_Bloom = gen_bloom(in_mem);
            in_table = new SSTablecache(time,in_count,in_mem->getMinkey(),in_mem->getMaxkey(),
            in_mem->getMinEle(),in_Bloom,10240+32+12*in_count);

            s_list.push_back(in_table);
            skip.push_back(in_mem);

            in_mem = new SkipList();

            bytes = 10240 + 32 + KEY_LENGTH + OFFSET_LENGTH + mem[i].value.length();

            in_mem->Insert(mem[i].key,mem[i].value);
        }
        else{
            bytes = by_bef;
            in_mem->Insert(mem[i].key,mem[i].value);
        }
    }
    if(bytes > 10240 + 32){
        in_count = in_mem->getKetcount();
        in_Bloom = gen_bloom(in_mem);
        in_table = new SSTablecache(time,in_count,in_mem->getMinkey(),in_mem->getMaxkey(),
        in_mem->getMinEle(),in_Bloom,10240+32+12*in_count);

        s_list.push_back(in_table);
        skip.push_back(in_mem);
    }

}


//由SSTable产生对应的kv的vector
void KVStore::read_kv(vector<SSTablecache*> &mem,vector<kv *> &m){
    string dir = this->getDir();
    string file_path;
    int length;
    int offset;
    char *buf;
    kv* in_kv;
    ifstream read_file;
    for(unsigned int i = 0;i < mem.size();++i){
        file_path = dir + fname_gen(mem.at(i)->getlevel(),mem.at(i)->getTime(),mem.at(i)->getindex());
        read_file.open(file_path,ios::binary);

        in_kv = new kv[mem.at(i)->getkey_Count()];
        for(unsigned int j = 0;j < mem.at(i)->getkey_Count();++j){
            length = mem.at(i)->get_pair(j).length;
            offset = mem.at(i)->get_pair(j).offset;
            buf = new char[length+1];
            read_file.seekg(offset,ios::beg);
            read_file.read(buf,length);
            buf[length] = '\0';
            in_kv[j].key = mem.at(i)->get_pair(j).key;
            in_kv[j].value = buf;
            in_kv[j].timestamp = mem.at(i)->getTime();
            delete [] buf;
        }
        m.push_back(in_kv);
        read_file.close();
    }
}

//由SSTable产生对应的kv的vector,但是这些SSTable块间有序
kv* KVStore::read_sorted_kv(vector<SSTablecache*> &mem)
{
    string dir = this->getDir();
    string file_path;
    int length;
    int offset;
    char *buf;
    kv* in_kv;
    uint64_t all_count = 0;
    ifstream read_file;
    for(unsigned int i = 0;i < mem.size();++i){
        all_count += mem.at(i)->getkey_Count();
    }
    in_kv = new kv[all_count];
    int count = 0;
    for(unsigned int i = 0;i < mem.size();++i){
        file_path = dir + fname_gen(mem.at(i)->getlevel(),mem.at(i)->getTime(),mem.at(i)->getindex());
        read_file.open(file_path,ios::binary);

        for(unsigned int j = 0;j < mem.at(i)->getkey_Count();++j){
            length = mem.at(i)->get_pair(j).length;
            offset = mem.at(i)->get_pair(j).offset;
            buf = new char[length+1];
            read_file.seekg(offset,ios::beg);
            read_file.read(buf,length);
            buf[length] = '\0';
            in_kv[count].key = mem.at(i)->get_pair(j).key;
            in_kv[count].value = buf;
            in_kv[count].timestamp = mem.at(i)->getTime();
            delete buf;
            count++;
        }

        read_file.close();
    }
    return in_kv;
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
        for(unsigned int i =0;i < this->all_level.at(0)->getCount();++i){
            last_level.push_back(this->all_level.at(0)->find_cache(i));
        }
        uint64_t k_min = get_minkey(last_level);
        uint64_t k_max = get_maxkey(last_level);

        vector<kv *> zero_kv;
        //将SSTable的值载入到内存中
        read_kv(last_level,zero_kv);

        //把level-1中符合要求的SSTable写入
        if(level == 1){
            Level* new_level = new Level(1);
            this->all_level.push_back(new_level);
            level++;
            //创建对应的文件

            string dir = this->getDir();
            string fileroad = dir + "/level-" + std::to_string(1);
            int result = utils::mkdir(fileroad.c_str());
            if(result){
                exit(0);
            }
        }

        for(unsigned int i = 0;i < this->all_level.at(1)->getCount();++i){
            if(isCover(k_min,k_max,this->all_level.at(1)->find_cache(i)->getkey_min(),this->all_level.at(1)->find_cache(i)->getkey_max())){
                this_level.push_back(this->all_level.at(1)->find_cache(i));
            }
        }

        //正式开始处理
        //定义对应的指针并且进行初始化
        uint64_t counter = 0;
        uint64_t* la_pointer = new uint64_t[zero_kv.size()];
        bool *unused = new bool(last_level.size());//用于判断第i个SSTable是否被遍历完
        for(unsigned int i = 0;i < last_level.size();++i){
            la_pointer[i] = 0;
            unused[i] = false;
            counter += this->all_level.at(0)->find_cache(i)->getkey_Count();
        }

        kv *la_box = new kv[counter];
        uint64_t count = 0;
        //先对level-0进行归并排序
        while(true){
            int hit = -1;//记录当前命中的SSTable的下标
            uint64_t tmp_min = UINT64_MAX;
            bool jug = true;
            for(unsigned int i= 0;i < last_level.size();++i){
                if(!unused[i] && tmp_min > zero_kv.at(i)[la_pointer[i]].key){
                    tmp_min = zero_kv.at(i)[la_pointer[i]].key;
                    hit = i;
                }
            }

            la_box[count] = zero_kv.at(hit)[la_pointer[hit]];

            //更新循环参量
            count++;
            la_pointer[hit]++;

            for(unsigned int i= 0;i < last_level.size();++i){
                if(la_pointer[i] == last_level.at(i)->getkey_Count()){
                    unused[i] = true;
                }
            }
            for(unsigned int i= 0;i < last_level.size();++i){
                jug = jug && unused[i];
            }
            if(jug){
                break;
            }
        }

        //进行vector中的内存的释放
        for(unsigned int i = 0;i < zero_kv.size();++i){
            delete [] zero_kv.at(i);
        }

        zero_kv.clear();

        kv* one_kv;

        kv* sorted_kv;

        int len_2 = 0;

        int length;

        uint64_t merge_time;//用于记录块内合并的次数，用于得到正确的长度信息

        //进行level-0的简单的Merge,由于块内有序，可以把level-n(n > 0)当作块内有序
        if(this_level.size() > 0){
            sort_vec(this_level);//保证vector中块间的有序

            one_kv = read_sorted_kv(this_level);

            //计算one_kv的长度
            for(unsigned int i = 0;i < this_level.size();++i){
                len_2 += this_level.at(i)->getkey_Count();
            }

            sorted_kv = merger_sort(la_box,one_kv,count,len_2,merge_time);
            length = merge_time;
        }
        else{
            if(level == 2){
                sorted_kv = merge_self(la_box,count,true,merge_time);
                length = merge_time;
            }
            else{
                sorted_kv = merge_self(la_box,count,false,merge_time);
                length = merge_time;
            }
        }

        //现在已经得到要写到level-1的文件了
        //先删除对应的文件
        del_file(last_level);
        del_file(this_level);
        //删除对应的SSTable缓存
        this->all_level.at(1)->de_table(k_min,k_max);
        this->all_level.at(0)->del_all();

        //通过kv* 获得对应的SSTable，并push到对应的level
        vector<SSTablecache*> in_table;
        vector<SkipList*> in_mem;
        gen_table_kv(sorted_kv,length,in_table,in_mem);


        //进行文件的写入
        //先写入SSTable
        for(unsigned int i = 0;i < in_table.size();++i){
            this->all_level.at(1)->put_SSTable(in_table.at(i));
        }

        //进行文件的写入
        for(unsigned int i = 0;i < in_table.size();++i){
            w_file_plus(in_table.at(i),in_mem.at(i));
        }

        //进行相应资源的回收
        delete [] sorted_kv;
        delete [] la_box;

        for(unsigned int i = 0;i < in_mem.size();++i){
            delete in_mem.at(i);
        }
        in_mem.clear();

        //后处理level >= 1的情况
        while(this->all_level.at(check_level)->getCount() >= ((1 << (check_level+1)) + 1)){
            //对于更高层的Compaction的处理
            //第一步，选择对应的SSTable
            //确定合并所需要的SSTable的数目
            int com_num = this->all_level.at((check_level))->getCount() - (1 << (check_level+1));
            last_level.clear();
            this_level.clear();

            this->all_level.at(check_level)->get_table_time(last_level,com_num);

            k_min = get_minkey(last_level);
            k_max = get_maxkey(last_level);

            //先进行判断，并创建目录
            if(level == check_level+1){
                Level* new_level = new Level(check_level+1);
                this->all_level.push_back(new_level);
                level++;
                //创建对应的文件

                string dir = this->getDir();
                string fileroad = dir + "/level-" + std::to_string(check_level+1);
                int result = utils::mkdir(fileroad.c_str());
                if(result){
                    exit(0);
                }
            }
            //初始化this_level
            for(unsigned int i = 0;i < this->all_level.at(check_level+1)->getCount();++i){
                if(isCover(k_min,k_max,this->all_level.at(check_level+1)->find_cache(i)->getkey_min(),this->all_level.at(check_level+1)->find_cache(i)->getkey_max())){
                    this_level.push_back(this->all_level.at(check_level+1)->find_cache(i));
                }
            }

            kv* sorted_kv;
            kv* this_kv;
            kv* next_kv;
            int len_this = 0;
            int len_next = 0;
            int length;

            uint64_t merge_time;

            sort_vec(last_level);

            this_kv = read_sorted_kv(last_level);

            //计算this_kv的长度
            for(unsigned int i = 0;i < last_level.size();++i){
                len_this += last_level.at(i)->getkey_Count();
            }

            if(this_level.size() > 0){
                sort_vec(this_level);

                next_kv = read_sorted_kv(this_level);

                //计算this_kv的长度
                for(unsigned int i = 0;i < this_level.size();++i){
                    len_next += this_level.at(i)->getkey_Count();
                }

                sorted_kv = merger_sort(this_kv,next_kv,len_this,len_next,merge_time);
                length = merge_time;

            }
            else{
                if(level == check_level + 1){
                    sorted_kv = merge_self(this_kv,len_this,true,merge_time);
                    length = merge_time;
                }
                else{
                    sorted_kv = merge_self(this_kv,len_this,false,merge_time);
                    length = merge_time;
                }

            }

            //现在已经得到要写到level-1的文件了
            //先删除对应的文件
            del_file(last_level);
            del_file(this_level);
            //删除对应的SSTable缓存
            this->all_level.at(check_level+1)->de_table(k_min,k_max);
            this->all_level.at(check_level)->de_table(k_min,k_max);

            //通过kv* 获得对应的SSTable，并push到对应的level
            vector<SSTablecache*> in_table;
            vector<SkipList*> in_mem;
            gen_table_kv(sorted_kv,length,in_table,in_mem);


            //进行文件的写入
            //先写入SSTable
            for(unsigned int i = 0;i < in_table.size();++i){
                this->all_level.at(check_level+1)->put_SSTable(in_table.at(i));
            }

            //进行文件的写入
            for(unsigned int i = 0;i < in_table.size();++i){
                w_file_plus(in_table.at(i),in_mem.at(i));
            }

            check_level++;

            //进行相应资源的回收
            delete [] this_kv;
            delete [] sorted_kv;

            for(unsigned int i = 0;i < in_mem.size();++i){
                delete in_mem.at(i);
            }
            in_mem.clear();
        }
    }
}

void KVStore::w_file_plus(SSTablecache* myCache,SkipList * mem)
{
    string dir = this->getDir();
    string file_path = dir + fname_gen(myCache->getlevel(),myCache->getTime(),myCache->getindex());
    ofstream data_file(file_path,ios::out | ios::binary);


    uint64_t key_count = mem->getKetcount();
    uint64_t min = mem->getMinkey();
    uint64_t max = mem->getMaxkey();
    //进行文件的写入
    //写入时间戳
    data_file.write(reinterpret_cast<char *>(&(this->timeStamp)),8);
    //写入key的个数
    data_file.write(reinterpret_cast<char *>(&(key_count)),8);
    //写入min和max
    data_file.write(reinterpret_cast<char *>(&min),8);
    data_file.write(reinterpret_cast<char *>(&max),8);
    //write Bloom filter
    for(int i = 0;i < 10240;++i){
        //writeIn = reinterpret_cast<char *>(&(Bloom[i]));
        data_file.write(reinterpret_cast<char *>(&(myCache->get_bloom()[i])),1);
    }

    int offset = 10240+32+mem->getKetcount() * 12;
    //开始写key和offset
    SKNode *p = mem->getMinEle();
    SKNode *q = p;
    //write key and offset
    for(unsigned int i = 0;i < key_count;++i){
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
    for(unsigned int i = 0;i < key_count;++i){
        data_file.write(q->val.c_str(),q->val.length());
        //这相当与这条指令的二进制写入data_file << q->val;
        q = q->forwards[0];
    }

    data_file.close();
}

//给定SSTable列表，删除对应的文件

void KVStore::del_file(vector<SSTablecache*> &s)
{
    string dir = this->getDir();
    string file_path;
    for(unsigned int i = 0;i < s.size();++i){
        file_path = dir + fname_gen(s.at(i)->getlevel(),s.at(i)->getTime(),s.at(i)->getindex());

        if(utils::rmfile(file_path.c_str()) != 0){
            cout << "rmfile wrong" << endl;
        }
    }
}

kv* KVStore::merge_self(kv* mem,uint64_t len,bool is_last,uint64_t &mer_time)
{
    kv* sorted_kv = new kv[len];
    mer_time = 0;
    //注意：对于删除的value要进行处理
    uint64_t count = 0;

    uint64_t index = 0;

    for(;index < len;++index){
        if(mem[index].value == "~DELETED~" && is_last){
            continue;
        }
        if(count == 0){
           sorted_kv[count] = mem[index];
           count++;
        }
        else{
            if(sorted_kv[count-1].key == mem[index].key){
                mer_time++;
                if(mem[index].timestamp > sorted_kv[count-1].timestamp){
                    sorted_kv[count-1] = mem[index];
                }
            }
            else{
                sorted_kv[count] = mem[index];
                count++;
            }
        }
    }
    mer_time = count;
    return sorted_kv;
}

kv* KVStore::merger_sort(kv* one,kv* two,uint64_t len_1,uint64_t len_2,uint64_t &mer_time)
{
    //进行一个两路的合并
    kv* sorted_kv = new kv[len_1 + len_2];
    mer_time = 0;

    uint64_t index_one = 0;
    uint64_t index_two = 0;

    bool is_one_end = false;

    uint64_t count = 0;

    while(true){
        if(one[index_one].key > two[index_two].key){
            if(count == 0){
                sorted_kv[count] = two[index_two];
                index_two++;
                count++;
            }
            else{
                if(two[index_two].key == sorted_kv[count-1].key){
                    mer_time++;
                    if(two[index_two].timestamp > sorted_kv[count-1].timestamp){
                        sorted_kv[count - 1] = two[index_two];
                        index_two++;
                    }
                }
                else{
                    sorted_kv[count] = two[index_two];
                    index_two++;
                    count++;
                }
            }
        }
        else if(one[index_one].key < two[index_two].key){
            if(count == 0){
                sorted_kv[count] = one[index_one];
                index_one++;
                count++;
            }
            else{
                if(one[index_one].key == sorted_kv[count-1].key){
                    mer_time++;
                    if(one[index_one].timestamp > sorted_kv[count-1].timestamp){
                        sorted_kv[count - 1] = one[index_one];
                        index_one++;
                    }
                }
                else{
                    sorted_kv[count] = one[index_one];
                    index_one++;
                    count++;
                }
            }
        }
        else{
            if(one[index_one].timestamp > two[index_two].timestamp){
                if(count == 0){
                    sorted_kv[count] = one[index_one];
                    index_one++;
                    count++;
                }
                else{
                    if(one[index_one].key == sorted_kv[count-1].key){
                        mer_time++;
                        if(one[index_one].timestamp > sorted_kv[count-1].timestamp){
                            sorted_kv[count - 1] = one[index_one];
                            index_one++;
                        }
                    }
                    else{
                        sorted_kv[count] = one[index_one];
                        index_one++;
                        count++;
                    }
                }
                index_two++;
            }
            else{
                if(count == 0){
                    sorted_kv[count] = two[index_two];
                    index_two++;
                    count++;
                }
                else{
                    if(two[index_two].key == sorted_kv[count-1].key){
                        mer_time++;
                        if(two[index_two].timestamp > sorted_kv[count-1].timestamp){
                            sorted_kv[count - 1] = two[index_two];
                            index_two++;
                        }
                    }
                    else{
                        sorted_kv[count] = two[index_two];
                        index_two++;
                        count++;
                    }
                }
                index_one++;
            }
        }

        //进行检测
        if(index_one == len_1){
            is_one_end = true;
            break;
        }
        if(index_two == len_2){
            break;
        }
    }

    if(is_one_end){
        for(;index_two < len_2;++index_two){
            if(count == 0){
                sorted_kv[count] = two[index_two];;
                count++;
            }
            else{
                if(two[index_two].key == sorted_kv[count-1].key){
                    mer_time++;
                    if(two[index_two].timestamp > sorted_kv[count-1].timestamp){
                        sorted_kv[count - 1] = two[index_two];
                    }
                }
                else{
                    sorted_kv[count] = two[index_two];
                    count++;
                }
            }
        }
    }
    else{
        for(;index_one < len_1;++index_one){
            if(count == 0){
                sorted_kv[count] = one[index_one];
                count++;
            }
            else{
                if(one[index_one].key == sorted_kv[count-1].key){
                    mer_time++;
                    if(one[index_one].timestamp > sorted_kv[count-1].timestamp){
                        sorted_kv[count - 1] = one[index_one];
                    }
                }
                else{
                    sorted_kv[count] = one[index_one];
                    count++;
                }
            }
        }
    }
    mer_time = count;

    return sorted_kv;
}

//先使用稳定的冒泡排序，后面可以再优化
void sort_vec(vector<SSTablecache *> &s){
    SSTablecache* compare;
    bool flag = true;
    for(unsigned int i = 0;i < s.size() && flag;++i){
        flag = false;
        for(unsigned int j = 0;j < s.size()-i - 1;++j){
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

uint64_t get_minkey(vector<SSTablecache*> &s)
{
    uint64_t min = UINT64_MAX;
    for(unsigned int i = 0;i < s.size();++i){
        if(min > s.at(i)->getkey_min()){
            min = s.at(i)->getkey_min();
        }
    }
    return min;
}

uint64_t get_maxkey(vector<SSTablecache*> &s)
{
    uint64_t max = 0;
    for(unsigned int i = 0;i < s.size();++i){
        if(max < s.at(i)->getkey_max()){
            max = s.at(i)->getkey_max();
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

//这个w_file只能用于正常的写入
void KVStore::w_file(SSTablecache* myCache)
{
        //文件大小超标，开始向有硬盘写入数据
        string dir = this->getDir();

        int index = myCache->getindex();
        string fileroad = fname_gen(myCache->getlevel(),myCache->getTime(),index);

        string file_path = dir + fileroad;
        ofstream data_file(file_path,ios::out | ios::binary);

        int offset = 32 + 10*1024 + key_count * 12;//the offset of the first element
        unsigned long long min = myCache->getkey_min();
        unsigned long long max = myCache->getkey_max();

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
        for(unsigned int i = 0;i < key_count;++i){
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
        string write_in = "";
        for(unsigned int i = 0;i < key_count;++i){
            //data_file.write(q->val.c_str(),q->val.length());
            write_in = write_in + q->val.c_str();
            if(q->key > max){
                cout << "key_count " << key_count << endl;
                cout << "indeed " << i << endl;
                cout << q->key;
            }
            if(i == key_count - 10){
                int a = 0;
            }
            //这相当与这条指令的二进制写入data_file << q->val;
            q = q->forwards[0];
        }

        data_file.write(write_in.c_str(),write_in.length());

        data_file.close();
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
            if(val == "~DELETED~"){
                return "";
            }
            return val;
        }
    else{
        //level-0采用遍历的方式
        //level-n采用区间识别的方式
        //cout << key << endl;
        //level-0
        if(level == 0){
            return "";
        }
        Level *zero_do = this->all_level.at(0);
        uint64_t time = 0;
        string ans;
        int mes[3] = {0};
        for(unsigned int i = 0;i < zero_do->getCount();++i){
            if(zero_do->find_cache(i)->Search(key,mes)){
                if(time > zero_do->find_cache(i)->getTime()){
                    continue;
                }
                string dir = this->getDir();
                string file_path = dir + fname_gen(0,zero_do->find_cache(i)->getTime(),zero_do->find_cache(i)->getindex());
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
                ans = buffer;
                time = zero_do->find_cache(i)->getTime();
                delete [] buffer;
                read_file.close();
            }
        }
        if(time > 0){
            if(ans == "~DELETED~"){
                return "";
            }
            return ans;
        }

        //level-n
        for(int i = 1;i < level;++i){
            for(unsigned int j = 0;j < this->all_level.at(i)->getCount();++j){
                if(this->all_level.at(i)->find_cache(j)->getkey_min() > key ||
                this->all_level.at(i)->find_cache(j)->getkey_max() < key){
                    continue;
                }
                else{
                    if(this->all_level.at(i)->find_cache(j)->Search(key,mes)){
                        string dir = this->getDir();
                        string file_path = dir + fname_gen(i,this->all_level.at(i)->find_cache(j)->getTime(),
                        this->all_level.at(i)->find_cache(j)->getindex());
                        int offset = mes[0];
                        uint64_t length = mes[1];
                        ifstream read_file(file_path,ios::in|ios::binary);
                        read_file.seekg(offset,ios::beg);
                        if(read_file.eof()){
                            cout << "wrong!" << endl;
                        }
                        char *buffer = new char[length + 1];
                        read_file.read(buffer,length);
                        buffer[length] = '\0';
                        ans = buffer;
                        time = this->all_level.at(i)->find_cache(j)->getTime();
                        delete [] buffer;
                        read_file.close();
                        if(ans == "~DELETED~"){
                            return "";
                        }
                        return ans;
                    }
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
    //把所有的目录清空
    this->Memtable.cleanMem();
    //删除对应的目录
    for(unsigned int i = 0;i < this->all_level.size();++i){
        //对应一个目录
        for(unsigned int j = 0;j < this->all_level.at(i)->getCount();++j){
            string dir = this->getDir();
            string file_path = dir + fname_gen(i,this->all_level.at(i)->find_cache(j)->getTime(),
            this->all_level.at(i)->find_cache(j)->getindex());
            int result = utils::rmfile(file_path.c_str());
            if(result){
                cout << "rmfile fail" << endl;
            }
            else{
                delete this->all_level.at(i)->find_cache(j);
            }
        }
        delete this->all_level.at(i);

        string dir = this->getDir();
        string dir_name = dir + "/level-" + std::to_string(i);
        int result = utils::rmdir(dir_name.c_str());
        if(result){
            cout << "rmdir fail" << endl;
        }
    }

    this->level = 0;
    this->resetBloom();
    this->key_count = 0;
    this->timeStamp = 0;
    this->all_level.clear();
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, list<pair<uint64_t, string> > &list)
{
    //对scan函数的实现
    //先扫Memtable
    //this->Memtable.ScanSearch(key1,key2,list);

    SkipList mem(this->Memtable);

    //接着从level中读取
    kv* read_in;//记录从每个SSTable中读到的内容
    ifstream read_file;
    //level-0特殊处理，因为区间可能会重叠
    if(level == 0){
        this->Memtable.ScanSearch(key1,key2,list);
        return;
    }
    else{
        for(int i = this->all_level.at(0)->getCount()-1;i > -1;--i){
            if(this->all_level.at(0)->find_cache(i)->getkey_min() > key2 ||
            this->all_level.at(0)->find_cache(i)->getkey_max() < key1){
                continue;
            }
            else{
                //区间有重叠，进行处理
                int mes[3] = {0};
                uint64_t k_max = (this->all_level.at(0)->find_cache(i)->getkey_max() > key2) ? key2 :
                this->all_level.at(0)->find_cache(i)->getkey_max();
                uint64_t k_min = (this->all_level.at(0)->find_cache(i)->getkey_min() < key1) ? key1 :
                this->all_level.at(0)->find_cache(i)->getkey_min();

                for(uint64_t j = k_min;j <= k_max;++j){
                    if(this->all_level.at(0)->find_cache(i)->Search(j,mes)){
                        //表示从这里开始的元素都是符合题意的
                        int offset = mes[0];
                        int index = mes[2];//代表对应元素的下标位置
                        string dir = this->getDir();
                        string file_path = dir + fname_gen(0,this->all_level.at(0)->find_cache(i)->getTime(),
                        this->all_level.at(0)->find_cache(i)->getindex());
                        read_file.open(file_path,ios::in|ios::binary);
                        read_file.seekg(offset,ios::beg);

                        char *buf = new char[this->all_level.at(0)->find_cache(i)->getlength()];

                        read_file.read(buf,this->all_level.at(0)->find_cache(i)->getlength());

                        uint64_t read_num = read_file.gcount();

                        read_file.close();

                        buf[read_num] = '\0';

                        read_in = new kv[k_max - k_min + 1];

                        int count = 0;

                        char* in_buf;

                        //开始对读进来的字符串进行解析
                        uint64_t read_pos = 0;//标识当前字符串解析的位置
                        while (true)
                        {
                            /* code */
                            in_buf = new char[this->all_level.at(0)->find_cache(i)->get_pair(index).length + 1];
                            memcpy(in_buf,buf + read_pos,this->all_level.at(0)->find_cache(i)->get_pair(index).length);

                            cout << this->all_level.at(0)->find_cache(i)->get_pair(index).length << endl;
                            cout << "okk : " << read_pos <<endl;

                            in_buf[this->all_level.at(0)->find_cache(i)->get_pair(index).length] = '\0';

                            read_in[count].key = this->all_level.at(0)->find_cache(i)->get_pair(index).key;
                            read_in[count].value = in_buf;
                            delete [] in_buf;
                            read_in[count].timestamp = this->all_level.at(0)->find_cache(i)->getTime();

                            count++;
                            index++;

                            if(index >= this->all_level.at(i)->find_cache(j)->getkey_Count()){
                                break;
                            }
                            if(this->all_level.at(0)->find_cache(i)->get_pair(index).key > k_max){
                                break;
                            }
                            read_pos += this->all_level.at(0)->find_cache(i)->get_pair(index).length - 1;;
                        }

                        for(int i = 0;i < count;++i){
                            string val = mem.Search(read_in[i].key);
                            if(val != ""){
                                continue;
                            }
                            else{
                                mem.Insert(read_in[i].key,read_in[i].value);
                            }
                        }
                        delete [] buf;
                        delete [] read_in;
                        break;
                    }
                }
            }
        }

        //对于level-n的情况的处理
        for(int i = 1;i < this->level;++i){
            //对于第i层的处理
            for(uint64_t j = 0;j < this->all_level.at(i)->getCount();++j){
                if(key1 > this->all_level.at(i)->find_cache(j)->getkey_max() ||
                key2 < this->all_level.at(i)->find_cache(j)->getkey_min()){
                    continue;
                }
                else{
                    //对于有覆盖的情况的处理
                    //区间有重叠，进行处理
                    int mes[3] = {0};
                    uint64_t k_max = (this->all_level.at(i)->find_cache(j)->getkey_max() > key2) ? key2 :
                    this->all_level.at(i)->find_cache(j)->getkey_max();
                    uint64_t k_min = (this->all_level.at(i)->find_cache(j)->getkey_min() < key1) ? key1 :
                    this->all_level.at(i)->find_cache(j)->getkey_min();

                    for(uint64_t m = k_min;m <= k_max;++m){
                        if(this->all_level.at(i)->find_cache(j)->Search(m,mes)){
                            //表示从这里开始的元素都是符合题意的
                            int offset = mes[0];
                            int index = mes[2];//代表对应元素的下标位置
                            string dir = this->getDir();
                            string file_path = dir + fname_gen(i,this->all_level.at(i)->find_cache(j)->getTime(),
                            this->all_level.at(i)->find_cache(j)->getindex());
                            read_file.open(file_path,ios::in|ios::binary);
                            read_file.seekg(offset,ios::beg);

                            char *buf = new char[this->all_level.at(i)->find_cache(j)->getlength()];

                            read_file.read(buf,this->all_level.at(i)->find_cache(j)->getlength());

                            uint64_t read_num = read_file.gcount();

                            //cout << "okk : " << read_num <<endl;
                            //code for debug
                            if(read_num == this->all_level.at(i)->find_cache(j)->getlength()){
                                cout << "illegal reference" << endl;
                            }

                            read_file.close();

                            buf[read_num] = '\0';

                            read_in = new kv[k_max - k_min + 1];

                            int count = 0;

                            char* in_buf;

                            //开始对读进来的字符串进行解析
                            uint64_t read_pos = 0;//标识当前字符串解析的位置
                            while (true)
                            {
                                /* code */
                                if(count == 236){

                                    int a = 0;
                                }

                                in_buf = new char[this->all_level.at(i)->find_cache(j)->get_pair(index).length + 1];
                                memcpy(in_buf,buf + read_pos,this->all_level.at(i)->find_cache(j)->get_pair(index).length);
                                in_buf[this->all_level.at(i)->find_cache(j)->get_pair(index).length] = '\0';

                                //cout << this->all_level.at(0)->find_cache(i)->get_pair(index).length << endl;
                                //cout << "okk : " << read_pos <<endl;

                                read_in[count].key = this->all_level.at(i)->find_cache(j)->get_pair(index).key;
                                read_in[count].value = in_buf;

                                delete [] in_buf;
                                read_in[count].timestamp = this->all_level.at(i)->find_cache(j)->getTime();

                                count++;
                                index++;

                                if(index >= this->all_level.at(i)->find_cache(j)->getkey_Count()){
                                    break;
                                }
                                if(this->all_level.at(i)->find_cache(j)->get_pair(index).key > k_max){
                                    break;
                                }
                                read_pos += this->all_level.at(i)->find_cache(j)->get_pair(index).length - 1;
                            }

                            for(int i = 0;i < count;++i){
                                string val = mem.Search(read_in[i].key);
                                if(val != ""){
                                    continue;
                                }
                                else{
                                    mem.Insert(read_in[i].key,read_in[i].value);
                                }
                            }
                            delete [] buf;
                            delete [] read_in;
                            break;
                        }
                    }
                }
            }
        }
    }

    mem.ScanSearch(key1,key2,list);
}
