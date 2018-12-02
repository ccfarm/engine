// Copyright [2018] Alibaba Cloud All rights reserved
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <map>
#include "util.h"
#include "engine_race.h"
#include <iostream>
#define FILENUM 256

namespace polar_race {

static const char kLockFile[] = "LOCK";

RetCode Engine::Open(const std::string& name, Engine** eptr) {
  return EngineRace::Open(name, eptr);
}

Engine::~Engine() {
}

/*
 * Complete the functions below to implement you own engine
 */

// 1. Open engine
RetCode EngineRace::Open(const std::string& name, Engine** eptr) {
  *eptr = NULL;
  if (!FileExists(name.c_str())
      && 0 != mkdir(name.c_str(), 0755)) {
    return kIOError;
  }
  EngineRace *engine_race = new EngineRace(name);
  *eptr = engine_race;
  return kSucc;
}

// 2. Close engine
EngineRace::~EngineRace() {
}

void EngineRace::ReadyForWrite() {
  pthread_mutex_lock(&mu_);
  if (!readyForWrite) {
    keyPos = GetFileLength(path + "/key");
    if (keyPos < 0) {
      keyPos = 0;
    }
    keyFile = open((path + "/key").c_str(), O_RDWR | O_CREAT, 0644);
    keyPos = GetFileLength(path + "/key");
    lseek(keyFile, keyPos, SEEK_SET);
    std::cout<<keyPos<<std::endl;
    valuePos = GetFileLength(path + "/value");
    if (valuePos < 0) {
      valuePos = 0;
    }
    valueFile = open((path + "/value").c_str(), O_RDWR | O_CREAT, 0644);
    lseek(valueFile, valuePos, SEEK_SET);
    std::cout<<valuePos<<std::endl;
    buf = new char[8];
    readyForWrite = true;
  }
  pthread_mutex_unlock(&mu_);
}

// 3. Write a key-value pair into engine
RetCode EngineRace::Write(const PolarString& key, const PolarString& value) {
  if (!readyForWrite) {
    ReadyForWrite();
  }
  pthread_mutex_lock(&mu_);
  write(valueFile, value.data(), 4096);
  write(keyFile, key.data(), 8);
  LongToChars(valuePos, buf);
  //std::cout<<valuePos<<std::endl;
  write(keyFile, buf, 8);
  valuePos += 4096;
  pthread_mutex_unlock(&mu_);
  // for (int i = 0; i < 8; i++) {
  //   std::cout<<(int)key[0]<<' ';
  // }
  // std::cout<<std::endl;
  // for (int i = 0; i < 8; i++) {
  //   std::cout<<(int)value[0]<<' ';
  // }
  // std::cout<<std::endl;
  return kSucc;
}

void EngineRace::ReadyForRead() {
  pthread_mutex_lock(&mu_);
  if (!readyForRead) {
    map = new Map();
    buf = new char[8];
    buf4096 = new char[4096];
    keyFile = open((path + "/key").c_str(), O_RDWR | O_CREAT, 0644);
    keyPos = 0;
    char *keyBuf = new char[8];
    while (read(keyFile, keyBuf, 8) > 0) {
      lseek(keyFile, keyPos, SEEK_SET);
      read(keyFile, keyBuf, 8);
      read(keyFile, buf, 8);
      keyPos += 16;
      map->Set(keyBuf, CharsToLong(buf));
      keyBuf = new char[8];
      //std::cout<<"mark"<<CharsToLong(buf)<<std::endl;
    }
    valueFile = open((path + "/value").c_str(), O_RDWR | O_CREAT, 0644);
    readyForRead = true;
  }
  pthread_mutex_unlock(&mu_);
}

// 4. Read value of a key
RetCode EngineRace::Read(const PolarString& key, std::string* value) {
  if (!readyForRead) {
    ReadyForRead();
  }
  int64_t pos = map->Get(key);
  //std::cout<<"mark"<<pos<<std::endl;
  pthread_mutex_lock(&mu_);
  lseek(valueFile, pos, SEEK_SET);
  read(valueFile, buf4096, 4096);
  // for (int i = 0; i < 8; i++) {
  //   std::cout<<buf4096[i]<<' ';
  // }
  *value = std::string(buf4096, 4096);
  // for (int i = 0; i < 8; i++) {
  //   std::cout<<(int)(*value)[i]<<"valueend";
  // }

  // for (int i = 0; i < 8; i++) {
  //   std::cout<<(int)key[0]<<' ';
  // }
  // std::cout<<std::endl;
  // for (int i = 0; i < 8; i++) {
  //   std::cout<<(int)(*value)[0]<<' ';
  // }
  //std::cout<<std::endl;
  pthread_mutex_unlock(&mu_);
  return kSucc;
}

/*
 * NOTICE: Implement 'Range' in quarter-final,
 *         you can skip it in preliminary.
 */
// 5. Applies the given Vistor::Visit function to the result
// of every key-value pair in the key range [first, last),
// in order
// lower=="" is treated as a key before all keys in the database.
// upper=="" is treated as a key after all keys in the database.
// Therefore the following call will traverse the entire database:
//   Range("", "", visitor)
RetCode EngineRace::Range(const PolarString& lower, const PolarString& upper,
    Visitor &visitor) {
    pthread_mutex_lock(&mu_);
    pthread_mutex_unlock(&mu_);
  return kSucc;
}

}  // namespace polar_race
