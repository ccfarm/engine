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
#define BUFSIZE 256

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
    count = 0;
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
  if (count < 100) {
    count += 1;
    for (int i = 0; i < 8; i++) {
      std::cout<<(int)key[i]<<' ';
    }
    std::cout<<std::endl;
    for (int i = 0; i < 8; i++) {
      std::cout<<(int)value[i]<<' ';
    }
    std::cout<<"write"<<std::endl;
  }
  return kSucc;
}

void EngineRace::ReadyForRead() {
  pthread_mutex_lock(&mu_);
  if (!readyForRead) {
    count = 0;
    map = new Map();
    buf = new char[8];
    
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
    buf4096 = new char[4096];
    map->Write(path);
    readyForRead = true;
  }
  pthread_mutex_unlock(&mu_);
}

// 4. Read value of a key
RetCode EngineRace::Read(const PolarString& key, std::string* value) {
  if (!readyForRead) {
    ReadyForRead();
  }
  pthread_mutex_lock(&mu_);
  int64_t pos = map->Get(key);
  if (pos == -1) {
    if (count < 10000) {
      count += 1;
      for (int i = 0; i < 8; i++) {
        std::cout<<(int)key[i]<<' ';
      }
      std::cout<<count<<" "<<pos<<" "<<"notFound"<<std::endl;
    }
    pthread_mutex_unlock(&mu_);
    return kNotFound;
  }
  
  //std::cout<<"mark"<<pos<<std::endl;
  
  lseek(valueFile, pos, SEEK_SET);
  read(valueFile, buf4096, 4096);
  // for (int i = 0; i < 8; i++) {
  //   std::cout<<buf4096[i]<<' ';
  // }
  *value = std::string(buf4096, 4096);
  if (count < 100) {
    count += 1;
    for (int i = 0; i < 8; i++) {
      std::cout<<(int)key[i]<<' ';
    }
    std::cout<<std::endl;
    for (int i = 0; i < 8; i++) {
      std::cout<<(int)(*value)[i]<<' ';
    }
    std::cout<<"read"<<std::endl;
  }
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
void EngineRace::ReadyForRange() {
        pthread_mutex_lock(&mu_);
        if (!readyForRange) {
            count = 0;
            keyFile = open((path + "/_key").c_str(), O_RDWR | O_CREAT, 0644);
            keys = (int64_t *)malloc(sizeof(int64_t) * MAPSIZE);
            values = (int64_t *)malloc(sizeof(int64_t) * MAPSIZE);
            keyPos = 0;
            buf = new char[8];
            lseek(keyFile, keyPos, SEEK_SET);
            while (read(keyFile, buf, 8) > 0) {
                keys[count] = CharsToLong(buf);
                //std::cout<<CharsToLong(buf)<<std::endl;
//                for (int j = 0; j < 8; j++) {
//                    std::cout<<(int)buf[j]<<' ';
//                }
//                std::cout<<"range"<<std::endl;
                read(keyFile, buf, 8);
                values[count] = CharsToLong(buf);
                keyPos += 16;
                lseek(keyFile, keyPos, SEEK_SET);
                //std::cout<<keys[count]<<' '<<values[count]<<std::endl;
                count += 1;
            }
            for (int i = 0; i < count; i++)
                for (int j = i + 1; j < count; j++) {
                    if ((uint64_t) keys[i] > (uint64_t) keys[j]) {
                        int64_t t = keys[i];
                        keys[i] = keys[j];
                        keys[j] = t;
                        t = values[i];
                        values[i] = values[j];
                        values[j] = t;
                    }
                }

            for (int i = 0; i < 10; i++) {
                LongToChars(keys[i], buf);
                for (int j = 0; j < 8; j ++) {
                    std::cout<<(int)buf[j]<<' ';
                }
                std::cout<<"readyForRange"<<std::endl;
            }
            bufKeys = (int64_t *)malloc(sizeof(int64_t) * BUFSIZE);
            bufValues = (char *)malloc(sizeof(char) * 4096 * BUFSIZE);
            valueFile = open((path + "/value").c_str(), O_RDWR | O_CREAT, 0644);
            readyForRange = true;
        }
        pthread_mutex_unlock(&mu_);
    }
    RetCode EngineRace::Range(const PolarString& lower, const PolarString& upper,
                              Visitor &visitor) {
        if (!readyForRange) {
            ReadyForRange();
        }
        char *buf = new char[8];
        for (int i = 0; i < count; i++) {
            if (keys[i] != bufKeys[i % BUFSIZE]) {
                pthread_mutex_lock(&mu_);
                if (keys[i] != bufKeys[i % BUFSIZE]) {
                    read(valueFile, (bufValues + 4096 * (i % BUFSIZE)), 4096);
                    bufKeys[i % BUFSIZE] = keys[i];
                }
                pthread_mutex_unlock(&mu_);
            }
            LongToChars(keys[i], buf);
//            for (int j = 0; j < 8; j ++) {
//                std::cout<<(int)buf[j]<<' ';
//            }
//            std::cout<<"read"<<keys[i]<<std::endl;
            visitor.Visit(*(new PolarString(buf, 8)), *(new PolarString(bufValues + 4096 * (i % BUFSIZE), 4096)));
        }

        pthread_mutex_lock(&mu_);
        pthread_mutex_unlock(&mu_);
        return kSucc;
    }

}  // namespace polar_race
