#pragma once

#include <vector>
#include <climits>
#include <string>
#include <cstdint>
#include <list>

#define MAX_LEVEL 16
#define KEY_LENGTH 8
#define OFFSET_LENGTH 4

enum SKNodeType
{
    HEAD = 1,
    NORMAL,
    NIL
};

struct SKNode
{
    uint64_t key;
    std::string val;
    SKNodeType type;
    std::vector<SKNode *> forwards;
    SKNode(uint64_t _key, std::string _val, SKNodeType _type)
        : key(_key), val(_val), type(_type)
    {
        for (int i = 0; i < MAX_LEVEL; ++i)
        {
            forwards.push_back(nullptr);
        }
    }
};

class SkipList
{
private:
    SKNode *head;
    SKNode *NIL;
    unsigned long long s = 1;
    unsigned long long bytes;//记录当前跳表转化为SStable的时候的大小
    unsigned long long key_count;//记录写入MemTable中键值的数量
    double my_rand();
    int randomLevel();

public:
    SkipList()
    {
        head = new SKNode(0, "", SKNodeType::HEAD);
        NIL = new SKNode(INT_MAX, "", SKNodeType::NIL);
        bytes = 10240 + 32;
        key_count = 0;
        for (int i = 0; i < MAX_LEVEL; ++i)
        {
            head->forwards[i] = NIL;
        }
    }
    void Insert(uint64_t key, std::string value);
    unsigned long long getBytes();
    std::string Search(uint64_t key);
    bool Delete(uint64_t key);
    uint64_t getMaxkey();//获取跳表中最大的键
    uint64_t getMinkey();//获取跳表中最小的键
    SKNode* getMinEle();//获得最小的key对应的节点
    void ScanSearch(uint64_t key_start, uint64_t key_end,std::list<std::pair<uint64_t, std::string> > &list);
    void cleanMem();//clean the skipList
    unsigned long long getKetcount();
    ~SkipList()
    {
        SKNode *n1 = head;
        SKNode *n2;
        while (n1)
        {
            n2 = n1->forwards[0];
            delete n1;
            n1 = n2;
        }
    }
};
