// Copyright [2018] Alibaba Cloud All rights reserved
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <thread>
#include <map>
#include "util.h"
#include "engine_race.h"
#include "city.h"
#include <iostream>
#define FILENUM 256
#define BUFSIZE 10000

namespace polar_race {
    thread_local char* bufLocal = 0;

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
        std::cout<<"close"<<std::endl;
    }

    void EngineRace::ReadyForWrite() {
        pthread_mutex_lock(&mu_);
        if (!readyForWrite) {
            count = 0;
//            keyPos = GetFileLength(path + "/key");
//            if (keyPos < 0) {
//                keyPos = 0;
//            }
            keyFile = open((path + "/key").c_str(), O_RDWR | O_CREAT, 0644);
            keyPos = 0;
            lseek(keyFile, keyPos, SEEK_SET);
//            valuePos = GetFileLength(path + "/value");
//            if (valuePos < 0) {
//                valuePos = 0;
//            }
            valueFile = new int[FILENUM];
            valuePos = new int64_t[FILENUM];
            for (int i = 0; i < FILENUM; i++) {
                valueFile[i] = open((path + "/value" + std::to_string(i)).c_str(), O_RDWR | O_CREAT, 0644);
                valuePos[i] = 0;
                lseek(valueFile[i], valuePos[i], SEEK_SET);
            }
            valueLock = (pthread_mutex_t*) malloc(sizeof(pthread_mutex_t) * FILENUM);
            for (int i = 0; i < FILENUM; i++) {
                valueLock[i] = PTHREAD_MUTEX_INITIALIZER;
            }
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
        uint32_t hash = StrHash(key.data(), 8) % FILENUM;
        pthread_mutex_lock(valueLock + hash);
        write(valueFile[hash], value.data(), 4096);
        pthread_mutex_lock(&mu_);
        write(keyFile, key.data(), 8);
        ShortToChars((int16_t)(valuePos[hash]>>12), buf);
        //std::cout<<(valuePos[hash])<<"ppppp_"<<std::endl;
        write(keyFile, buf, 2);
        pthread_mutex_unlock(&mu_);
        valuePos[hash] += 4096;
        pthread_mutex_unlock(valueLock + hash);
        pthread_mutex_lock(&mu_);
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
        pthread_mutex_unlock(&mu_);
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
                //lseek(keyFile, keyPos, SEEK_SET);
                read(keyFile, buf, 2);
                //std::cout<<(int)buf[0]<<(int)buf[1]<<"short____"<<std::endl;
                keyPos += 10;
                map->Set(CharsToLong(keyBuf), CharsToShort(buf));
                //std::cout<<"mark"<<CharsToLong(buf)<<std::endl;
                //std::cout<<CharsToShort(buf)<<"short____"<<std::endl;
            }
            valueFile = new int[FILENUM];
            for (int i = 0; i < FILENUM; i++) {
                valueFile[i] = open((path + "/value" + std::to_string(i)).c_str(), O_RDWR | O_CREAT, 0644);
            }
            valueLock = (pthread_mutex_t*) malloc(sizeof(pthread_mutex_t) * FILENUM);
            for (int i = 0; i < FILENUM; i++) {
                valueLock[i] = PTHREAD_MUTEX_INITIALIZER;
            }
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
        int16_t _pos = map->Get(CharsToLong(key.data()));
        //std::cout<<_pos<<"short____"<<std::endl;
        if (_pos == -1) {
            if (count < 10000) {
                count += 1;
                for (int i = 0; i < 8; i++) {
                    std::cout<<(int)key[i]<<' ';
                }
                std::cout<<count<<" "<<_pos<<" "<<"notFound"<<std::endl;
            }
            pthread_mutex_unlock(&mu_);
            return kNotFound;
        }
        int64_t pos = _pos;
        pos <<= 12;
        //std::cout<<"mark"<<pos<<std::endl;
        uint32_t hash = StrHash(key.data(), 8) % FILENUM;
//        std::cout<<"mark"<<hash<<std::endl;
        if (!bufLocal) {
            //std::cout<<"new";
            bufLocal = new char[4096];
        }
        pthread_mutex_lock(valueLock + hash);
//        std::cout<<"mark"<<pos<<std::endl;
        lseek(valueFile[hash], pos, SEEK_SET);
        read(valueFile[hash], bufLocal, 4096);
        pthread_mutex_unlock(valueLock + hash);
        // for (int i = 0; i < 8; i++) {
        //   std::cout<<buf4096[i]<<' ';
        // }

//        int tmpFile = open((path + "/value" + std::to_string(2573)).c_str(), O_RDWR | O_CREAT, 0644);
//        char* buf4096 = new char[4096];
//        lseek(tmpFile, 0, SEEK_SET);
//        read(tmpFile, buf4096, 4096);
//        for (int j = 0; j < 8; j ++) {
//            std::cout<<(int)(buf4096[j])<<' ';
//        }
//        std::cout<<"range2222"<<std::endl;

        *value = std::string(bufLocal, 4096);
        pthread_mutex_lock(&mu_);
        count += 1;
        if (count > 1000000) {
                    std::cout<<"byebye"<<std::endl;
                    std::exit(-1);
                }
        if (count < 100) {
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
//            std::thread et(excitThread);
//            et.detach();
            // if (map) {
            //     delete map;
            // }
            count = 0;
            keyFile = open((path + "/_key").c_str(), O_RDWR | O_CREAT, 0644);
            keys = (int64_t *)malloc(sizeof(int64_t) * MAPSIZE);
            values = (int64_t *)malloc(sizeof(int64_t) * MAPSIZE);
            keyPos = 0;
            char* buf = new char[8];
            lseek(keyFile, keyPos, SEEK_SET);
            while (read(keyFile, buf, 8) > 0) {
                keys[count] = CharsToLong(buf);
                keyPos += 8;
                lseek(keyFile, keyPos, SEEK_SET);
                read(keyFile, buf, 2);
                int16_t _pos = CharsToShort(buf);
                values[count] = _pos<<12;
                keyPos += 2;
                lseek(keyFile, keyPos, SEEK_SET);
                if (count < 10) {
                    std::cout<<keys[count]<<' '<<values[count]<<std::endl;
                }
                count += 1;
                if (count > 1000000) {
                    std::cout<<"byebye"<<std::endl;
                    std::exit(-1);
                }
            }
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
            bufLock = (pthread_mutex_t*) malloc(sizeof(pthread_mutex_t) * BUFSIZE);
            for (int i = 0; i < BUFSIZE; i++) {
                bufLock[i] = PTHREAD_MUTEX_INITIALIZER;
            }
            valueFile = new int[FILENUM];
            for (int i = 0; i < FILENUM; i++) {
                valueFile[i] = open((path + "/value" + std::to_string(i)).c_str(), O_RDWR | O_CREAT, 0644);
            }
            valueLock = (pthread_mutex_t*) malloc(sizeof(pthread_mutex_t) * FILENUM);
            for (int i = 0; i < FILENUM; i++) {
                valueLock[i] = PTHREAD_MUTEX_INITIALIZER;
            }
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
            if (keys[i] != bufKeys[i % BUFSIZE]) {
                pthread_mutex_lock(bufLock+(i % BUFSIZE));
                if (keys[i] != bufKeys[i % BUFSIZE]) {
                    LongToChars(keys[i], buf);
                    uint32_t hash = StrHash(buf, 8) % FILENUM;
//                    std::cout<<"mark"<<hash<<std::endl;
//                    std::cout<<"mark"<<values[i]<<std::endl;
                    lseek(valueFile[hash], values[i], SEEK_SET);
                    read(valueFile[hash], bufValues + 4096 * (i % BUFSIZE), 4096);
                    bufKeys[i % BUFSIZE] = keys[i];
//                    int tmpFile = open((path + "/value" + std::to_string(2573)).c_str(), O_RDWR | O_CREAT, 0644);
//                    char* buf4096 = new char[4096];
//                    lseek(tmpFile, 0, SEEK_SET);
//                    read(tmpFile, buf4096, 4096);
//                    for (int j = 0; j < 8; j ++) {
//                        std::cout<<(int)(buf4096[j])<<' ';
//                    }
//                    std::cout<<"range2222"<<std::endl;
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
                    std::cout<<(int)*(bufValues + 4096 * (i % BUFSIZE) + j)<<' ';
                }
                std::cout<<"range2"<<std::endl;
            }
            visitor.Visit(*(new PolarString(buf, 8)), *(new PolarString(bufValues + 4096 * (i % BUFSIZE), 4096)));
        }
        return kSucc;
    }
//    RetCode EngineRace::Range(const PolarString& lower, const PolarString& upper,
//                              Visitor &visitor) {
//        return kSucc;
//    }

}  // namespace polar_race