#include <iostream>
#include <stdlib.h>
#include "SkipList.h"

using namespace std;

double SkipList::my_rand()
{
    s = (16807 * s) % 2147483647ULL;
    return (s + 0.0) / 2147483647ULL;
}

int SkipList::randomLevel()
{
    int result = 1;
    while (result < MAX_LEVEL && my_rand() < 0.5)
    {
        ++result;
    }
    return result;
}

unsigned long long SkipList::getBytes()
{
    return this->bytes;
}


void SkipList::Insert(uint64_t key, std::string value)
{
    // TODO
    //to ensure the value exist or not
    int count_search = MAX_LEVEL;
    SKNode *find = head;
    bool is_find = false;
    while(count_search > 0){
        if(find->forwards[count_search-1] == NIL){
            count_search--;
        }
        else{
            if(key < find->forwards[count_search-1]->key){
                count_search--;
                continue;
            }
            else if(key == find->forwards[count_search-1]->key){
                //to do some operation
                is_find = true;
                std::string oldval = find->forwards[count_search-1]->val;
                find->forwards[count_search-1]->val = value;
                this->bytes = this->bytes + value.length() - oldval.length();
                break;
            }
            else{
                while (key > find->forwards[count_search-1]->key)
                {
                    /* code */
                    find = find->forwards[count_search-1];
                }
                if(key == find->forwards[count_search-1]->key){
                    is_find = true;
                    std::string oldval = find->forwards[count_search-1]->val;
                    find->forwards[count_search-1]->val = value;
                    this->bytes = this->bytes + value.length() - oldval.length();
                    break;
                }
                else{
                    count_search--;
                    continue;
                }

            }
        }
    }

    if(is_find){
        //cout << "insert cost : " << time_used.count() << endl;
        return;
    }
    else{
            this->key_count += 1;
            this->bytes = KEY_LENGTH + OFFSET_LENGTH + this->bytes + value.length();
            int level = randomLevel();
           // cout << level << "is level" << endl;
            int count = MAX_LEVEL;
            std::vector<SKNode*> update(MAX_LEVEL);
            SKNode *p = head;
            while (count > 0)
            {
                /* code */
                if(p->forwards[count-1] == NIL){
                    update[count-1] = p;
                    count--;
                }
                else{
                    while (key > p->forwards[count-1]->key)
                    {
                        /* code */
                        p = p->forwards[count-1];
                    }
                    if(count <= level){
                        update[count-1] = p;
                    }

                    count--;
                }
            }
            for(int i = level + 1; i < MAX_LEVEL+1;++i){
                update[i-1] = head;
            }
            SKNode *build = new SKNode(key,value,SKNodeType::NORMAL);
            for(int i = 0;i < level;++i){
                build->forwards[i] = update[i]->forwards[i];
                update[i]->forwards[i] = build;
            }
        }
    //cout << "insert cost : " << time_used.count() << endl;
    return;
}

unsigned long long SkipList::getKetcount()
{
    return this->key_count;
}

std::string SkipList::Search(uint64_t key)
{
    // TODO
    int level = MAX_LEVEL;
    SKNode *p = head;
    for(;level > 0;--level){
        if(level == 1){
            while(key > p->forwards[level-1]->key){
                p = p->forwards[level-1];
            }
            if(p->forwards[level-1]->key == key){
                if(p->forwards[level-1]->val == "~DELETED~"){
                    return "~DELETED~";
                }
                return p->forwards[level-1]->val;
            }
            else{
                return "";
            }
        }
        while(key > p->forwards[level-1]->key){
            p = p->forwards[level-1];
        }
    }
    return "";
    //cout << "search cost : " << time_used.count() << endl;
}

void SkipList::ScanSearch(uint64_t key_start, uint64_t key_end,std::list<std::pair<uint64_t, std::string> > &list)
{
    // TODO
    int level = MAX_LEVEL;
    SKNode *p = head;
    for(;level > 0;--level){
        while(key_start > p->forwards[level-1]->key){
            p = p->forwards[level-1];
        }
        if(level ==  1){
            int count = 0;
            while(p->forwards[0]->key <= key_end){
                list.push_back(std::pair<uint64_t, std::string>(p->forwards[0]->key,p->forwards[0]->val)); 
                p = p->forwards[0];
            }
        }
    }
    //cout << "search cost : " << time_used.count() << endl;
//    for(int i = key_start;i <= key_end;++i){
//        Search(i);
//    }
}

bool SkipList::Delete(uint64_t key)
{
    // TODO
    SKNode *p = head;
    std::vector<SKNode*> update(MAX_LEVEL);
    for(int i = MAX_LEVEL;i > 0;--i){
        while(key > p->forwards[i-1]->key){
            p = p->forwards[i-1];
        }
        update[i-1] = p;
    }
    if(p->forwards[0]->key == key){
        for(int i = 0;i < MAX_LEVEL;++i){
            if(update[i]->forwards[i]->key == key){
                if(update[i]->forwards[i]->val == "~DELETED~"){
                    return false;
                }
                //此处要更新bytes大小
                this->bytes = this->bytes + 9 - update[i]->forwards[i]->val.length();
                update[i]->forwards[i]->val = "~DELETED~";
                return true;
            }
            else{
                return false;
            }
        }
    }

    return false;
    //cout << "delete cost : " << time_used.count() << endl;
}

uint64_t SkipList::getMaxkey()
{
    SKNode *p = this->head;
    int count = MAX_LEVEL;
    for(;count > 0;--count){
        while (p->forwards[count-1] != this->NIL)
        {
            /* code */
            p = p->forwards[count-1];
        }
    }
    return p->key;
    
}

SKNode* SkipList::getMinEle()
{
    return this->head->forwards[0];
}

uint64_t SkipList::getMinkey()
{
    return this->head->forwards[0]->key;
}

void SkipList::cleanMem()
{
    SKNode *n1 = head;
    SKNode *n2 = n1->forwards[0];
    while(n2 != NIL){
        n1->forwards[0] = n2->forwards[0];
        delete n2;
        n2 = n1->forwards[0];
    }
    for(int i = 0;i < MAX_LEVEL;++i){
        n1->forwards[i] = NIL;
    }
    this->bytes = 10240 + 32;
    this->key_count = 0;
}