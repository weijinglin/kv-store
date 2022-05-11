#include "level.h"
#include "utils.h"

bool isCover(uint64_t k_min,uint64_t k_max,uint64_t ck_min,uint64_t ck_max){
	if(k_max < ck_min){
		return false;
	}
	if(k_min > ck_max){
		return false;
	}
	return true;
}

Level::Level(int l)
{
    count = 0;
    Num = 0;
    level = l;
    this->SSt_chunk.resize(1 << (level + 2));//留下充足的余量
}

Level::~Level()
{
    //here,it is need to think when to delete the SSTable(because the SSTable it stored by pointer)

}

SSTablecache* Level::find_cache(uint64_t index)
{
    return this->SSt_chunk.at(index);
}


void Level::put_SSTable(SSTablecache *table)
{
    this->SSt_chunk[count] = table;
    count++;
    table->setlevel(this->level);
    table->setindex(Num);
    Num++;
}

int Level::getLevel()
{
    return this->level;
}

uint64_t Level::getCount()
{
    return this->count;
}

void Level::de_table(uint64_t k_min,uint64_t k_max)
{
    //包含删除对应的SSTable
    uint64_t del_num = 0;
    for(uint64_t i = 0;i < this->count;++i){
        if(isCover(k_min,k_max,SSt_chunk.at(i)->getkey_min(),SSt_chunk.at(i)->getkey_max())){
            //进行删除操作
            SSTablecache *p = SSt_chunk.at(i);
            delete p;
            SSt_chunk.at(i) = nullptr;
            del_num++;
        }
    }

    uint64_t find_num = 0;
    //进行位置上的调整
    for(uint64_t i = 0;i < this->count;++i){
        if(SSt_chunk.at(i) == nullptr){
            for(int j = i;j < this->count - find_num;++j){
                SSt_chunk[j] = SSt_chunk.at(j+1);
            }
            find_num++;
            i--;
        }
        if(find_num == del_num){
            break;
        }
    }
    count -= del_num;
}

void Level::del_all()
{
    for(int i = 0;i < this->count;++i){
        SSTablecache *p = SSt_chunk.at(i);
        delete p;
        SSt_chunk.at(i) = nullptr;
    }
    count = 0;
}

uint64_t Level::get_min_time()
{
    uint64_t time_min = UINT64_MAX;
    for(unsigned int i = 0;i < this->count;++i){
        if(time_min > this->SSt_chunk.at(i)->getTime()){
            time_min = this->SSt_chunk.at(i)->getTime();
        }
    }
    return time_min;
}

void Level::get_table_time(vector<SSTablecache*> &s,int count)
{
    //先保证正确性，采用最最土的方法
    uint64_t index_array[count];
    bool *hit = new bool[this->getCount()];

    for(unsigned int i = 0;i < this->getCount();++i){
        hit[i] = false;
    }
    uint64_t index = 0;
    uint64_t k_min = UINT64_MAX;
    uint64_t time_min = UINT64_MAX;
    for(int i = 0;i < count;++i){
        for(unsigned int i = 0;i < this->getCount();++i){
            if(time_min > this->SSt_chunk.at(i)->getTime() && !hit[i]){
                index = i;
                time_min = this->SSt_chunk.at(i)->getTime();
                k_min = this->SSt_chunk.at(i)->getkey_min();
            }
            else if(time_min == this->SSt_chunk.at(i)->getTime() && !hit[i]){
                if(this->SSt_chunk.at(i)->getkey_min() < k_min){
                    index = i;
                    k_min = this->SSt_chunk.at(i)->getkey_min();
                }
            }
        }
        index_array[i] = index;
        hit[index] = true;
        time_min = UINT64_MAX;
    }
    for(int i = 0;i < count;++i){
        s.push_back(this->SSt_chunk.at(index_array[i]));
    }
}
