#include "util.h"
#include <iostream>
#include "include/engine.h"
#include <fcntl.h>
#include <unistd.h>
#define MAPSIZE 128000000
#define BLOCK  64 * 4096 * 5

namespace polar_race {

    class Entry{
    public:
        Entry() {
            //key = _key;
            value = -1;
        }
        Entry(int64_t _key, int16_t _value) {
            key = _key;
            value = _value;
        }
        int64_t GetKey() {
            return key;
        }
        int16_t GetValue() {
            return value;
        }

        void SetKey(int16_t newValue) {
            value = newValue;
        }

        void SetValue(int16_t newValue) {
            value = newValue;
        }

        int64_t key;
        int16_t value;

    private:
    };

    class Map  {
    public:
        Map() {
            //hashKey = new uint32_t[MAPSIZE];
            //values = new Entry*[MAPSIZE];
            values = new Entry[MAPSIZE] ();
            //std::cout<<(int64_t)(*values)<<std::endl;
            //*values = 0;
            //std::cout<<values[0].value<<std::endl;
        }
        ~Map() {
//            Entry **next;
//            Entry **tmp;
//            for (int i = 0; i < MAPSIZE; i++) {
//                Entry **next = (values + i);
//                while (*next) {
//                    tmp = next;
//                    next = &((*next)->next);
//                    delete *tmp;
//                }
//            }
//            free(values);
//            std::cout<<"free map"<<std::endl;
        }
        void Set(int64_t key, int16_t value) {
            uint32_t hash = StrHash((char*)(&key), 8) % MAPSIZE;
            //std::cout<<hash<<std::endl;
//            for (int i = 0; i < 8; i++) {
//                std::cout<<(int)(*key)[i]<<' ';
//            }
//            std::cout<<std::endl;
//            std::cout<<value<<"set"<<std::endl;
            //std::cout<<(int64_t)(values)<<std::endl<<(int64_t)(values + 1)<<std::endl;
            //std::cout<<(int64_t)(next)<<std::endl<<(int64_t)(values + hash)<<std::endl;
            //std::cout<<&next<<std::endl;
            //std::cout<<hash<<std::endl;
            while ((values[hash].key != key) && (values[hash].value != -1)) {
                hash += 1;
                if (hash == MAPSIZE) hash = 0;
            }
            values[hash].key = key;
            values[hash].value = value;
        }
        int16_t Get(int64_t key) {
            //std::cout<<"hello";
//            for (int i = 0; i < 8; i++) {
//                std::cout<<(int)(key)[i]<<' ';
//            }
//            std::cout<<std::endl;
//            std::cout<<"get"<<std::endl;
            uint32_t hash = StrHash((char*)(&key), 8) % MAPSIZE;
            //std::cout<<hash<<std::endl;
            //std::cout<<hash<<' '<<next.key<<' '<<next.value<<std::endl;
//             for (int i = 0; i < 8; i++) {
//                 std::cout<<(int)key[i]<<' ';
//             }
//             std::cout<<key.size()<<std::endl;
            //std::cout<<&next<<std::endl;
            //std::cout<<(int64_t)next<<std::endl;
            //next = 0x7fff3be80148;
            //std::cout<<"hello";
            while ((values[hash].key != key) && (values[hash].value != -1)) {
                hash += 1;
                if (hash == MAPSIZE) hash = 0;
            }
            return values[hash].value;
        }

        void Write(std::string path) {
            int keyFile = open((path + "/_key").c_str(), O_RDWR | O_CREAT, 0644);
            //Entry next;
            int block = 64 * 1024 * 5;
            char* buf = (char *) malloc(block);
            memset(buf, 0, sizeof(char) * block);
            char* buf8 = (char *) malloc(8);
            memset(buf8, 0, sizeof(char) * 8);
            int pos = 0;
            int count = 0;
            int countHash = 0;
            for (int i = 0; i < MAPSIZE; i++) {
                if (values[i].value != -1) {
                    countHash += 1;
                    LongToChars(values[i].key, buf + count);
                    //std::cout<<std::endl;
                    count += 8;
                    ShortToChars(values[i].value, buf + count);
                    //std::cout<<(*next)->GetKey()<<"short"<<std::endl;
                    count += 2;

                    if (count == block) {
                        lseek(keyFile, pos, SEEK_SET);
                        write(keyFile, buf, block);
                        count = 0;
                        pos += block;
                    }
                }
            }
            std::cout<<"countHash"<<countHash<<std::endl;
            if (count > 0) {
                lseek(keyFile, pos, SEEK_SET);
                write(keyFile, buf, count);
            }
        }


    private:
        //uint32_t* hashKey;
        Entry* values;
    };
};