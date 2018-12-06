// Copyright [2018] Alibaba Cloud All rights reserved
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <thread>
#include <map>
#include "util.h"
#include "engine_race.h"
#include <iostream>
#define FILENUM 256
#define BUFSIZE 25600

namespace polar_race {
    void* excitThread() {
      sleep(300);
      std::exit(-1);
    }
    void qsort(uint64_t* keys, uint64_t* values, int ll, int rr) {
        uint64_t tk = keys[ll];
        uint64_t  tv = values[ll];
        int l = ll;
        int r = rr;
        while (l < r) {
            while ((keys[r] >= tk) && (l < r)) r--;
            if (l < r) {
                keys[l] = keys[r];
                values[l] = values[r];
                l += 1;
            }
            while ((keys[l] <= tk) && (l < r)) l++;
            if (l < r) {
                keys[r] = keys[l];
                values[r] = values[l];
                r -= 1;
            }
        }
        keys[l] = tk;
        values[l] = tv;
        l ++;
        r--;
        if (l < rr) qsort(keys, values, l, rr);
        if (ll < r) qsort(keys, values, ll, r);
    }

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
        //std::thread et(excitThread);
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
            // keyPos = GetFileLength(path + "/key");
            // if (keyPos < 0) {
            //     keyPos = 0;
            // }
            keyFile = open((path + "/key").c_str(), O_RDWR | O_CREAT, 0644);
            keyPos = 0;
            lseek(keyFile, keyPos, SEEK_SET);
            std::cout<<keyPos<<std::endl;
            // valuePos = GetFileLength(path + "/value");
            // if (valuePos < 0) {
            //     valuePos = 0;
            // }
            valuePos = 0;
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
            std::cout<<"1"<<std::endl;
            std::thread et(excitThread);
            et.detach();
            std::cout<<"2"<<std::endl;
            count = 0;
            keyFile = open((path + "/_key").c_str(), O_RDWR | O_CREAT, 0644);
            keys = (int64_t *)malloc(sizeof(int64_t) * MAPSIZE);
            values = (int64_t *)malloc(sizeof(int64_t) * MAPSIZE);
            keyPos = 0;
            char* buf = new char[8];
            lseek(keyFile, keyPos, SEEK_SET);
            std::cout<<"3"<<std::endl;
            while (read(keyFile, buf, 8) > 0) {
                keys[count] = CharsToLong(buf);
                //std::cout<<CharsToLong(buf)<<std::endl;
//                for (int j = 0; j < 8; j++) {
//                    std::cout<<(int)buf[j]<<' ';
//                }
//                std::cout<<"range"<<std::endl;
                keyPos += 8;
                lseek(keyFile, keyPos, SEEK_SET);
                read(keyFile, buf, 8);
                values[count] = CharsToLong(buf);
                keyPos += 8;
                lseek(keyFile, keyPos, SEEK_SET);
                if (count < 10) {
                  std::cout<<keys[count]<<' '<<values[count]<<std::endl;
                }
                count += 1;
            }
            std::cout<<"4"<<std::endl;
            for (int i = 0; i < 10; i++) {
                LongToChars(keys[i], buf);
                for (int j = 0; j < 8; j ++) {
                    std::cout<<(int)buf[j]<<' ';
                }
                std::cout<<"readyForRange "<<values[i]<<std::endl;
            }

            qsort((uint64_t*)keys, (uint64_t*)values, 0, count - 1);

            for (int i = 0; i < 10; i++) {
                LongToChars(keys[i], buf);
                for (int j = 0; j < 8; j ++) {
                    std::cout<<(int)buf[j]<<' ';
                }
                std::cout<<"readyForRange "<<values[i]<<std::endl;
            }
            bufKeys = (int64_t *)malloc(sizeof(int64_t) * BUFSIZE);
            bufValues = (char *)malloc(sizeof(char) * 4096 * BUFSIZE);
            valueFile = open((path + "/value").c_str(), O_RDWR | O_CREAT, 0644);
            //bufLock = new pthread_mutex_t[BUFSIZE]();
            bufLock = (pthread_mutex_t*) malloc(sizeof(pthread_mutex_t) * BUFSIZE);
            for (int i = 0; i < BUFSIZE; i++) {
                bufLock[i] = PTHREAD_MUTEX_INITIALIZER;
            }
            std::cout<<"5"<<std::endl;
            readyForRange = true;
        }
        pthread_mutex_unlock(&mu_);
    }
    RetCode EngineRace::Range(const PolarString& lower, const PolarString& upper,
                              Visitor &visitor) {
                                
        if (!readyForRange) {
            ReadyForRange();
        }
        std::cout<<"6"<<std::endl;
        char *buf = new char[8];
        for (int i = 0; i < count; i++) {
            if (i < 10) {
              std::cout<<"7"<<std::endl;
            }
            if (keys[i] != bufKeys[i % BUFSIZE]) {
                pthread_mutex_lock(bufLock+(i % BUFSIZE));
                if (keys[i] != bufKeys[i % BUFSIZE]) {
                    lseek(valueFile, values[i], SEEK_SET);
                    read(valueFile, (bufValues + 4096 * (i % BUFSIZE)), 4096);
                    bufKeys[i % BUFSIZE] = keys[i];
                }
                pthread_mutex_unlock(bufLock+(i % BUFSIZE));
            }
            LongToChars(keys[i], buf);

            if (i < 100) {
                for (int j = 0; j < 8; j ++) {
                    std::cout<<(int)buf[j]<<' ';
                }
                std::cout<<"range1"<<std::endl;
                for (int j = 0; j < 8; j ++) {
                    std::cout<<(int)*(bufValues+ 4096 * (i % BUFSIZE) + j)<<' ';
                }
                std::cout<<"range2"<<std::endl;
            }
            visitor.Visit(*(new PolarString(buf, 8)), *(new PolarString(bufValues + 4096 * (i % BUFSIZE), 4096)));
        }
        return kSucc;
    }

}  // namespace polar_race
