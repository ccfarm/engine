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
#include <time.h>
#include <atomic>
#include <sys/mman.h>
#define FILENUM 4096
#define BUFSIZE 131072 * 2
#define THREADNUM 4
#define WRITEBLOCK 640000 * 10


namespace polar_race {
    thread_local char* bufLocal = 0;
    thread_local int threadId = -1;
    thread_local int rangeTime = 0;
    std::atomic<int> posMark;
    std::atomic<int> posMark2;

    void* excitThread() {
        sleep(300);
        std::exit(-1);
    }
    void qsort(uint64_t* keys, int16_t* values, int ll, int rr) {
        uint64_t tk = keys[ll];
        int16_t  tv = values[ll];
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
        if ((stage == 1) && (keyFile) && (pos > 0)) {
            lseek(keyFile, keyPos, SEEK_SET);
            write(keyFile, buf, pos);
            std::cout<<"_write in ~EngineRace"<<std::endl;
        }
        std::cout<<time(NULL) - _time<<" close"<<std::endl;
    }

    void EngineRace::ReadyForWrite() {
        pthread_mutex_lock(&mu_);
        if (!readyForWrite) {
            stage = 1;
            count = 0;
            keyFile = open((path + "/key").c_str(), O_RDWR | O_CREAT, 0644);
            keyPos = 0;
            pos = 0;
            lseek(keyFile, keyPos, SEEK_SET);
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
            buf = (char *) malloc(WRITEBLOCK * 10);
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
        int tmp = valuePos[hash];
        valuePos[hash] += 4096;
        pthread_mutex_unlock(valueLock + hash);



        pthread_mutex_lock(&mu_);
        memcpy(buf + pos, key.data(), 8);
        ShortToChars((int16_t)(tmp>>12), buf + pos + 8);
        pos += 10;
        if (pos == WRITEBLOCK) {
            lseek(keyFile, keyPos, SEEK_SET);
            write(keyFile, buf, WRITEBLOCK);
            pos = 0;
            keyPos += WRITEBLOCK;
            std::cout<<keyPos<<std::endl;
        }
        pthread_mutex_unlock(&mu_);



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
            stage = 2;
            count = 0;
            map = new Map();
            buf = new char[8];
            keyFile = open((path + "/key").c_str(), O_RDWR | O_CREAT, 0644);
            keyPos = 0;

//            char *keyBuf = new char[8];
//            while (read(keyFile, keyBuf, 8) > 0) {
//                count += 1;
//                if (count % 100000 == 0){
//                    std::cout<<count<<std::endl;
//                }
//                read(keyFile, buf, 2);
//                keyPos += 10;
//                map->Set(CharsToLong(keyBuf), CharsToShort(buf));
//            }
//            std::cout<<count<<std::endl;


            int ccount;
            int block = 64 * 1024 * 5;
            char* buff = (char *) malloc(block);
            while ((ccount = read(keyFile, buff, block)) > 0) {
                //std::cout<<ccount<<std::endl;
                int pos = 0;
                while (pos < ccount) {
                    map->Set(CharsToLong(buff + pos), CharsToShort(buff + pos + 8));
                    count += 1;
                    pos += 10;
                    if (count % 100000 == 0){
                        std::cout<<count<<std::endl;
                    }
                }
            }
            free(buff);
            std::cout<<count<<std::endl;





            count = 0;
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
            //pthread_mutex_unlock(&mu_);
            return kNotFound;
        }
        int64_t pos = _pos;
        pos <<= 12;
        uint32_t hash = StrHash(key.data(), 8) % FILENUM;
        if (!bufLocal) {
            bufLocal = new char[4096];
        }
        pthread_mutex_lock(valueLock + hash);
        lseek(valueFile[hash], pos, SEEK_SET);
        read(valueFile[hash], bufLocal, 4096);
        pthread_mutex_unlock(valueLock + hash);

        *value = std::string(bufLocal, 4096);
        pthread_mutex_lock(&mu_);
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

    void EngineRace::ReadTT() {
        if (bufLocal == 0) {
            bufLocal = new char[8];
        }
        if (posMark2.load() - posMark.load() > 1000) {
            return;
        }
        int i = posMark2.fetch_add(1);
        LongToChars(keys[i], bufLocal);
        uint32_t hash = StrHash(bufLocal, 8) % FILENUM;
        pthread_mutex_lock(valueLock + hash);
        lseek(valueFile[hash], ((int64_t)values[i])<<12, SEEK_SET);
        read(valueFile[hash], bufValues + 4096 * (i % BUFSIZE), 4096);
        pthread_mutex_unlock(valueLock + hash);
        bufKeys[i % BUFSIZE] = keys[i];

    }

    void EngineRace::ReadyForRange() {
        pthread_mutex_lock(&mu_);
        if (!readyForRange) {
            if (map) {
                delete map;
            }
            threadId = 0;
            stage = 3;
            count = 0;
            keyFile = open((path + "/_key").c_str(), O_RDWR | O_CREAT, 0644);
            keys = (int64_t *)malloc(sizeof(int64_t) * MAPSIZE);
            values = (int16_t *)malloc(sizeof(int64_t) * MAPSIZE);
            keyPos = 0;

            lseek(keyFile, keyPos, SEEK_SET);
            std::cout<<time(NULL) - _time<<"beforeRead"<<std::endl;
            int ccount;
            int block = 64 * 1024 * 5;
            char* buff = (char *) malloc(block);
            while ((ccount = read(keyFile, buff, block)) > 0) {
                //std::cout<<ccount<<std::endl;
                int pos = 0;
                while (pos < ccount) {
                    keys[count] = CharsToLong(buff + pos);
                    values[count] = CharsToShort(buff + pos + 8);
                    //std::cout<<values[count]<<std::endl;
                    count += 1;
                    pos += 10;
                }
            }
            free(buff);
            std::cout<<count<<std::endl;
            char* buf = new char[8];
            for (int i = 0; i < 10; i++) {
                LongToChars(keys[i], buf);
                for (int j = 0; j < 8; j ++) {
                    std::cout<<(int)buf[j]<<' ';
                }
                std::cout<<"readyForRange "<<values[i]<<std::endl;
            }
            std::cout<<time(NULL) - _time<<"beforeSort"<<std::endl;
            qsort((uint64_t*)keys, values, 0, count - 1);
            std::cout<<time(NULL) - _time<<"afterSort"<<std::endl;
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
        rangeTime += 1;
        PolarString* key;
        PolarString* value;
        char* buf = new char[8];
        if (threadId == 0) {
            posMark2.store(0);
            posMark.store(-1);
            for (int i = 0; i < count; i++) {
                if (i > posMark.load()) {
                    ReadTT();
                }
                while (keys[i] != bufKeys[i % BUFSIZE]);
                LongToChars(keys[i], buf);
                key = new PolarString(buf, 8);
                value = new PolarString(bufValues + 4096 * (i % BUFSIZE), 4096);
                visitor.Visit(*key, *value);
                posMark.store(i);
                if (i % 100000 == 0) {
                    std::cout<<std::this_thread::get_id()<<' '<<i<<' '<<posMark.load()<<' '<<posMark2.load()<<std::endl;
                }
                delete key;
                delete value;
            }
        } else {
            for (int i = 0; i < count; i++) {
                if (i > posMark.load()) {
                    ReadTT();
                }
                while (keys[i] != bufKeys[i % BUFSIZE]);
                LongToChars(keys[i], buf);
                key = new PolarString(buf, 8);
                value = new PolarString(bufValues + 4096 * (i % BUFSIZE), 4096);
                visitor.Visit(*key, *value);
                if (i % 100000 == 0) {
                    std::cout<<std::this_thread::get_id()<<' '<<i<<' '<<posMark.load()<<' '<<posMark2.load()<<std::endl;
                }
                delete key;
                delete value;
            }
        }
        std::cout<<threadId<<' '<<count<<" done\n";


        return kSucc;
    }
}  // namespace polar_race