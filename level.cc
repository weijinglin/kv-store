#include "level.h"


Level::Level(int l)
{
    count = 0;
    level = l;
    this->SSt_chunk.resize(1 << level);
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
}

int Level::getLevel()
{
    return this->level;
}

uint64_t Level::getCount()
{
    return this->count;
}