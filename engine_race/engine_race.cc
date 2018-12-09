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


namespace polar_race {
    thread_local char* bufLocal = 0;
    thread_local int threadId = -1;
    thread_local int rangeTime = 0;
    std::atomic<int> posMark;
    std::atomic<int> posMark2;
//    pthread_mutex_t lock_ = PTHREAD_MUTEX_INITIALIZER;
    //thread_local int threadNum = 0;

    void* excitThread() {
        sleep(300);
        std::cout<<"posMark "<<posMark.load()<<std::endl;
        std::cout<<"posMark2 "<<posMark2.load()<<std::endl;
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
//        if (valuePos) {
//            for (int i=0; i<FILENUM; i++) {
//                std::cout<<valuePos[i]<<std::endl;
//                std::cout<<(int16_t)(valuePos[i]>>12)<<std::endl;
//            }
//        }
//        time(NULL) - _time;
//        if (stage == 1) {
//            lseek(keyFile, 0, SEEK_SET);
//            write(keyFile, buf, pos);
//        }
        std::cout<<time(NULL) - _time<<"close"<<std::endl;
    }

    void EngineRace::ReadyForWrite() {
        pthread_mutex_lock(&mu_);
        if (!readyForWrite) {
            stage = 1;
            count = 0;
//            keyPos = GetFileLength(path + "/key");
//            if (keyPos < 0) {
//                keyPos = 0;
//            }
            keyFile = open((path + "/key").c_str(), O_RDWR | O_CREAT, 0644);
            keyPos = 0;
            ftruncate(keyFile, 64000000 * 10);
            char* bufff = new char[16];
            read(keyFile, bufff, 16);
            //lseek(keyFile, 64000000 * 10, SEEK_SET);
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
            //int block = 64 * 4096 * 5;
//            buf = (char *) malloc(64000000 * 10);
            buf = static_cast<char*>(mmap(NULL, 64000000 * 10, PROT_READ | PROT_WRITE,
                             MAP_SHARED, keyFile, 0));
            memset(buf, 0, 64000000 * 10);
            pos = 0;
            //posBlock = 0;
            readyForWrite = true;
        }
        pthread_mutex_unlock(&mu_);
    }

// 3. Write a key-value pair into engine
    RetCode EngineRace::Write(const PolarString& key, const PolarString& value) {
        if (!readyForWrite) {
            ReadyForWrite();
        }
        //std::cout<<pos<<std::endl;
        uint32_t hash = StrHash(key.data(), 8) % FILENUM;
        pthread_mutex_lock(valueLock + hash);
        write(valueFile[hash], value.data(), 4096);
        int tmp = valuePos[hash];
        valuePos[hash] += 4096;
        pthread_mutex_unlock(valueLock + hash);
        //std::cout<<pos<<std::endl;


        pthread_mutex_lock(&mu_);
        //std::cout<<pos<<std::endl;
        for (int i = 0; i < 8; i++) {
            buf[pos + i] = key.data()[i];
        }

//        for (int j = 0; j < 8; j ++) {
//            std::cout<<(int)buf[pos + j]<<' ';
//        }
//        std::cout<<pos<<"ready "<<(int16_t)(tmp>>12)<<std::endl;
        ShortToChars((int16_t)(tmp>>12), buf + pos + 8);
        pos += 10;
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
            int block = 64 * 4096 * 5;
            char* buff = (char *) malloc(block);
            stage = 2;
            count = 0;
            map = new Map();
            //buf = new char[8];
            keyFile = open((path + "/key").c_str(), O_RDWR | O_CREAT, 0644);
            keyPos = 0;
            //char *keyBuf = new char[8];
            int64_t key;
            while ((count = read(keyFile, buff, block)) > 0) {
                int pos = 0;
                while (pos < count) {
//                    for (int j = 0; j < 8; j ++) {
//                        std::cout<<(int)buff[pos + j]<<' ';
//                    }
//                    std::cout<<"readyT1 "<<std::endl;
                    key = CharsToLong(buff + pos);
                    if (key != 0) {
                        map->Set(key, CharsToShort(buff + pos + 8));
                    }
                    pos += 10;
                }
                //std::cout<<"mark"<<CharsToLong(buf)<<std::endl;
                //std::cout<<CharsToShort(buf)<<"short____"<<std::endl;
            }
            free(buff);
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

    void* ReadT(pthread_mutex_t** valueLock, int count,
            int* valueFile, int64_t* keys, int16_t* values, int64_t* bufKeys,char* bufValues) {
        char *buf = new char[8];
        int i;
        while (true) {
            i = posMark2.fetch_add(1);
            if (i >= count) break;
            //std::cout<<i<<std::endl;
            while (i - posMark.load() > 1000) {
                //std::this_thread::yield;
            }

            LongToChars(keys[i], buf);
            uint32_t hash = StrHash(buf, 8) % FILENUM;

            pthread_mutex_lock(*valueLock + hash);
            lseek(valueFile[hash], ((int64_t)values[i])<<12, SEEK_SET);
            read(valueFile[hash], bufValues + 4096 * (i % BUFSIZE), 4096);
            pthread_mutex_unlock(*valueLock + hash);
            bufKeys[i % BUFSIZE] = keys[i];
            //std::cout<<'a'<<i<<' '<<keys[i]<<' '<<bufKeys[i % BUFSIZE]<<std::endl;
            //std::this_thread::yield;
        }
        std::cout<<"readt end\n";
    }

    void EngineRace::ReadyForRange() {
        pthread_mutex_lock(&mu_);
        if (!readyForRange) {
//            std::thread et(excitThread);
//            et.detach();
//            if (map) {
//                delete map;
//            }
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
            bufKeys =  (int64_t *)malloc(sizeof(int64_t) * BUFSIZE);
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

//    void* ReadI() {
//        if ((posMark2.load() >= count) && (posMark2.load() - posMark.load() > 1000)) {
//            return;
//        }
//        int i = posMark2.fetch_add(1);
//        //std::cout<<i<<' '<<threadId<<" 111 "<<posMark<<' '<<posMark2<<"rest\n";
//        if (i >= count) {
//            return;
//        }
//        if (bufLocal == 0) {
//            bufLocal = new char[8];
//        }
//        //std::cout<<i<<' '<<threadId<<" 111 "<<posMark<<' '<<posMark2<<"rest\n";
//        LongToChars(keys[i], bufLocal);
//        uint32_t hash = StrHash(bufLocal, 8) % FILENUM;
////        for (int j = 0; j < 8; j ++) {
////            std::cout<<(int)bufLocal[j]<<' ';
////        }
//        //std::cout<<"readyT "<<values[i]<<std::endl;
//        pthread_mutex_lock(valueLock + hash);
//        lseek(valueFile[hash], ((int64_t)values[i])<<12, SEEK_SET);
//        read(valueFile[hash], bufValues + 4096 * (i % BUFSIZE), 4096);
//        //std::cout<<hash<<" readyT "<<values[i]<<std::endl;
////                for (int j = 0; j < 8; j ++) {
////                    std::cout<<(int)bufValues[4096 * (i % BUFSIZE) + j]<<' ';
////                }
////                std::cout<<"readyT "<<values[i]<<std::endl;
//        pthread_mutex_unlock(valueLock + hash);
//        bufKeys[i % BUFSIZE] = keys[i];
//    }


    RetCode EngineRace::Range(const PolarString& lower, const PolarString& upper,
                              Visitor &visitor) {

        if (!readyForRange) {
            ReadyForRange();
        }
        rangeTime += 1;
        PolarString* key;
        PolarString* value;
        char* buf = new char[8];
//        if (threadId == -1) {
//            pthread_mutex_lock(&mu_);
//            threadId = threadNum;
//            threadNum += 1;
//            pthread_mutex_unlock(&mu_);
//        }

        //std::cout<<threadId<<"threadId\n";
        if (threadId == 0) {
            posMark2.store(0);
            posMark.store(-1);
            for (int i = 0; i < THREADNUM; i++) {
                std::thread rt(ReadT, &valueLock, count, valueFile, keys, values, bufKeys, bufValues);
                rt.detach();
            }

            // std::thread t(excitThread);
            // t.detach();

            for (int i = 0; i < count; i++) {
                bool flag = true;
                while (keys[i] != bufKeys[i % BUFSIZE]) {
                    if (flag) {
                        flag = false;
                        std::cout<<i<<' '<<threadId<<' '<<posMark.load()<<"rest\n";
                        std::cout<<keys[i]<<' '<<bufKeys[i % BUFSIZE]<<"rest\n";
                        LongToChars(keys[i], buf);
                        for (int j = 0; j < 8; j ++) {
                            std::cout<<(int)buf[j]<<' ';
                        }
                        std::cout<<"rangewait"<<std::endl;
                        LongToChars(bufKeys[i % BUFSIZE], buf);
                        for (int j = 0; j < 8; j ++) {
                            std::cout<<(int)buf[j]<<' ';
                        }
                        std::cout<<"rangewait"<<std::endl;
                    }
//                    pthread_mutex_lock(&mu_);
//                    std::cout<<i<<' '<<threadId<<' '<<posMark.load()<<"rest\n";
//                    pthread_mutex_unlock(&mu_);
                    //std::this_thread::yield;
                }
                //td::cout<<keys[i];


//                LongToChars(keys[i], buf);
//                uint32_t hash = StrHash(buf, 8) % FILENUM;
////        for (int j = 0; j < 8; j ++) {
////            std::cout<<(int)bufLocal[j]<<' ';
////        }
//                //std::cout<<"readyT "<<values[i]<<std::endl;
//                lseek(valueFile[hash], ((int64_t)values[i])<<12, SEEK_SET);
//                read(valueFile[hash], bufValues + 4096 * (i % BUFSIZE), 4096);
//                //std::cout<<hash<<" readyT "<<values[i]<<std::endl;
////                for (int j = 0; j < 8; j ++) {
////                    std::cout<<(int)bufValues[4096 * (i % BUFSIZE) + j]<<' ';
////                }
////                std::cout<<"readyT "<<values[i]<<std::endl;
//                bufKeys[i % BUFSIZE] = keys[i];


                LongToChars(keys[i], buf);
                key = new PolarString(buf, 8);
                value = new PolarString(bufValues + 4096 * (i % BUFSIZE), 4096);
//                for (int j = 0; j < 8; j ++) {
//                    std::cout<<(int)bufValues[4096 * (i % BUFSIZE) + j]<<' ';
//                }
//                std::cout<<"readyT "<<values[i]<<std::endl;
                visitor.Visit(*key, *value);
                posMark.store(i);
                if (i % 100000 == 0) {
                    std::cout<<std::this_thread::get_id()<<' '<<i<<' '<<posMark.load()<<' '<<posMark2.load()<<std::endl;
                }
                delete key;
                delete value;
            }
            range = true;
        } else {
            for (int i = 0; i < count; i++) {
                while (keys[i] != bufKeys[i % BUFSIZE]) {
//                    pthread_mutex_lock(&mu_);
//                    std::cout<<i<<' '<<threadId<<' '<<posMark.load()<<"rest\n";
//                    pthread_mutex_unlock(&mu_);
                    //std::this_thread::yield;
                }
                LongToChars(keys[i], buf);
                key = new PolarString(buf, 8);
                value = new PolarString(bufValues + 4096 * (i % BUFSIZE), 4096);
                visitor.Visit(*key, *value);
                if (i % 100000 == 0) {
                    std::cout<<std::this_thread::get_id()<<' '<<i<<' '<<posMark.load()<<' '<<posMark2.load()<<std::endl;
                }
                delete key;
                delete value;
                //std::cout<<i<<threadId<<"threadId\n";
                //std::cout<<"threadId\n";
            }
        }
        std::cout<<threadId<<' '<<count<<" done\n";


        return kSucc;
    }
//    RetCode EngineRace::Range(const PolarString& lower, const PolarString& upper,
//                              Visitor &visitor) {
//        return kSucc;
//    }

}  // namespace polar_race
