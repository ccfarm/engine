#include <assert.h>
#include <stdio.h>
#include <string>
#include <iostream>
#include <pthread.h>
#include "include/engine.h"

static const char kEnginePath[] = "/home/chao/engine/data";
static const char kDumpPath[] = "/home/chao/engine/data";

using namespace polar_race;

class DumpVisitor : public Visitor {
public:
  DumpVisitor(int* kcnt)
    : key_cnt_(kcnt) {}

  ~DumpVisitor() {}

  void Visit(const PolarString& key, const PolarString& value) {
    printf("Visit %s --> %s\n", key.data(), value.data());
    (*key_cnt_)++;
  }
  
private:
  int* key_cnt_;
};

void* writeThread(Engine* engine) {
  for (int i = 0; i < 10000; i++) {
    char* _key = new char[8];
    char* _value = new char[4096];
    int tmp = i;
    int j = 0;
    while (tmp > 0) {
      _key[j] = tmp % 10;
      _value[j] = tmp % 10;
      tmp /= 10;
      j += 1;
    }
    PolarString* key = new PolarString(_key, 8);
    PolarString* value = new PolarString(_value, 4096);
    // for (int j = 0; j <8; j++) {
    //   std::cout<<(int)_key[0]<<' '<<(int)_value[0]<<std::endl;
    // }
    engine->Write(*key, *value);
  }
}

void* readThread(Engine* engine) {
  for (int i = 0; i < 10000; i++) {
    char* _key = new char[8];
    PolarString* key = new PolarString(_key, 8);
    int tmp = i;
    int j = 0;
    while (tmp > 0) {
      _key[j] = tmp % 10;
      tmp /= 10;
      j += 1;
    }
    std::string value;
    engine->Read(*key, &value);
    // for (int j = 0; j <8; j++) {
    //   std::cout<<(int)_key[0]<<' '<<(int)value[0]<<std::endl;
    // }
    // for (int j = 0; j < 8; j++) {
    //   std::cout<<(int)_key[j]<<' ';
    // }
    // std::cout<<"test"<<std::endl;
    // for (int j = 0; j < 8; j++) {
    //   std::cout<<(int)value[j]<<' ';
    // }
    // std::cout<<"test2"<<std::endl;
  }
}


int main() {
  Engine *engine = NULL;

  RetCode ret = Engine::Open(kEnginePath, &engine);
  assert (ret == kSucc);

  writeThread(engine);
  readThread(engine);

  // char* ch = new char[4096];
  // ch[0] = 49;
  // ch[1] = 50;
  // PolarString* ps = new PolarString(ch, 4096);

  // ret = engine->Write("aaaaaaaa", *ps);
  // assert (ret == kSucc);
  // ret = engine->Write("aaaaaaaa", *ps);
  // ret = engine->Write("aaaaaaaa", *ps);
  // ret = engine->Write("aaaaaaaa", *ps);
  // ret = engine->Write("aaaaaaaa", *ps);
  // ret = engine->Write("bbbbbbbb", *ps);
  // assert (ret == kSucc);

  // ret = engine->Write("ccdccccc", *ps);

  // std::string value;
  // ret = engine->Read("aaaaaaaa", &value);
  // printf("Read aaaaaaaa value: %s\n", value.c_str());
  // // std::cout<<value.c_str()[0]<<"test";
  // for (int i = 0; i < 8; i++) {
  //   std::cout<<(int)value[i]<<"vend";
  // }
  
  // ret = engine->Read("bbbbbbbb", &value);
  // assert (ret == kSucc);
  // printf("Read bbbbbbbb value: %s\n", value.c_str());

  // int key_cnt = 0;
  // DumpVisitor vistor(&key_cnt);
  // ret = engine->Range("b", "", vistor);
  // assert (ret == kSucc);
  // printf("Range key cnt: %d\n", key_cnt);
  // delete engine;



  return 0;
}
