#pragma once

#include <vector>
#include "SSTable.h"

using namespace std;


//更新的大致思路：在Compaction的时候，首先直接利用指针的特性更新对应的SSTable里面的值，然后根据
//更新后的SSTable对SSt_chunk的内容进行更新，多出来的就不插入了，直接参与下一次Compaction
class Level
{
private:
    /* data */
    uint64_t count;//used to log the number of SSTable the level store
    int level;// used to log for the level of the level 
    vector<SSTablecache*> SSt_chunk;//used to store the SSTable in the level    
    uint64_t Num;//used to find the corrsponding file in the level(it represent the num of file the level has ever haven)

    //Num和count的区别在于一个只增不减，一个可增可减

public:
    Level(int l);

    ~Level();

    SSTablecache* find_cache(uint64_t index);//use index to get a pointer to a SSTable

    void put_SSTable(SSTablecache *table);//put a table into the level(a helper function that will be used in Compaction)

    int getLevel();

    uint64_t getCount();

    void de_table(uint64_t k_min,uint64_t k_max);//删除和这个范围有交集的SSTable

    void del_all();

    uint64_t get_min_time();

    void get_table_time(vector<SSTablecache*> &s,int count);//根据给定的数目初始化对应的vector(选择timestamp和key小的）
};
