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
    this->SSt_chunk.push_back(table);
    count++;
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

    //进行位置上的调整
    for(uint64_t i = 0;i < this->count;++i){
        if(SSt_chunk.at(i) == nullptr){
            for(int j = i;j < this->count - 1;++j){
                SSt_chunk[j] = SSt_chunk.at(j+1);
            }
        }
    }
}