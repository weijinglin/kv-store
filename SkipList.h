#pragma once

#include <vector>
#include <climits>
#include <string>
#include <cstdint>
#include <list>

#define MAX_LEVEL 8

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
    double my_rand();
    int randomLevel();

public:
    SkipList()
    {
        head = new SKNode(0, 0, SKNodeType::HEAD);
        NIL = new SKNode(INT_MAX, 0, SKNodeType::NIL);
        for (int i = 0; i < MAX_LEVEL; ++i)
        {
            head->forwards[i] = NIL;
        }
    }
    void Insert(uint64_t key, std::string value);
    std::string Search(uint64_t key);
    bool Delete(uint64_t key);
    void ScanSearch(uint64_t key_start, uint64_t key_end,std::list<std::pair<uint64_t, std::string> > &list);
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
