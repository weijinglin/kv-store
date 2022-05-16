#include <iostream>
#include "kvstore.h"

using namespace std;

class MyTest
{
private:
    /* data */
    KVStore store;//用于测试的kv_store

    Put_test(int num);
    Get_test(int num);
    Del_test(int num);

public:
    MyTest(const std::string &dir);
    ~MyTest();
    void startTest(int num);
};

void MyTest::startTest(int num){
    cout << "start to test for performance" << endl;
    this->store.reset();

    for (int i = 0; i < num; ++i) {
        store.put(i, std::string(i+1, 's'));
    }

    cout << "all put finish" << endl;

    for (int i = 0; i < num; ++i) {
        store.get(i + 1);
    }

    cout << "all get finish" << endl;

    for (int i = 0; i < num; ++i) {
        store.del(i + 1);
    }

    cout << "all del finish" << endl;

    store.report();
}

MyTest::MyTest(const std::string &dir):store(dir)
{
}

MyTest::~MyTest()
{
}

int main(){
    cout << "input the num of test" << endl;

    int num = 0;
    
    cin >> num;

    MyTest test("./data");

    test.startTest(num);

    cout << "test end!" << endl;
}