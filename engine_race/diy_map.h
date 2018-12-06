#include "util.h"
#include <iostream>
#include "include/engine.h"
#include <fcntl.h>
#include <unistd.h>
#include "city.h"
#define MAPSIZE 64000000
//#define MAPSIZE 64000

namespace polar_race {

    class Entry{
    public:
        Entry(int64_t _key, int16_t _value) {
            key = _key;
            value = _value;
            next = 0;
        }
        int64_t GetKey() {
            return key;
        }
        int16_t GetValue() {
            return value;
        }

        void SetValue(int16_t newValue) {
            value = newValue;
        }

        polar_race::Entry* next;

    private:
        int64_t key;
        int16_t value;
    };

    class Map  {
    public:
        Map() {
            //hashKey = new uint32_t[MAPSIZE];
            //values = new Entry*[MAPSIZE];
            values = (Entry **)malloc(MAPSIZE * 8);
            memset(values, 0, sizeof(Entry**) * MAPSIZE);
            //std::cout<<(int64_t)(*values)<<std::endl;
            //*values = 0;
            //std::cout<<(int64_t)(*values)<<std::endl;
        }
        ~Map() {
            Entry **next;
            Entry **tmp;
            for (int i = 0; i < MAPSIZE; i++) {
                Entry **next = (values + i);
                while (*next) {
                    tmp = next;
                    next = &((*next)->next);
                    delete *tmp;
                }
            }
            free(values);
            std::cout<<"free map"<<std::endl;
        }
        void Set(int64_t key, int16_t value) {
            uint32_t hash = StrHash((char*)(&key), 8) % MAPSIZE;
            //std::cout<<hash<<std::endl;
            Entry** next = (values + hash);
//            for (int i = 0; i < 8; i++) {
//                std::cout<<(int)(*key)[i]<<' ';
//            }
//            std::cout<<std::endl;
//            std::cout<<value<<"set"<<std::endl;
            //std::cout<<(int64_t)(values)<<std::endl<<(int64_t)(values + 1)<<std::endl;
            //std::cout<<(int64_t)(next)<<std::endl<<(int64_t)(values + hash)<<std::endl;
            //std::cout<<&next<<std::endl;
            //std::cout<<(int64_t)next<<std::endl;
            if (!*next) {
                *next = new Entry(key, value);
                //std::cout<<(int64_t)next<<std::endl;
                return;
            }
            if ((*next)->GetKey() == key) {
                (*next)->SetValue(value);
                return;
            }
            int count = 0;
            while ((*next)->next) {
                count += 1;
                //std::cout<<count<<hash<<std::endl;
                if ((*next)->next->GetKey() == key) {
                    (*next)->next->SetValue(value);
                    //std::cout<<count<<hash<<std::endl;
                    return;
                }
                else
                {
                    //std::cout<<count<<hash<<std::endl;
                    next = &((*next)->next);
                }
            }
            //Entry* entry = new Entry(*key, value);
            (*next)->next = new Entry(key, value);
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
            Entry **next = (values + hash);
//             for (int i = 0; i < 8; i++) {
//                 std::cout<<(int)key[i]<<' ';
//             }
//             std::cout<<key.size()<<std::endl;
            //std::cout<<&next<<std::endl;
            //std::cout<<(int64_t)next<<std::endl;
            //next = 0x7fff3be80148;
            //std::cout<<"hello";
            while (*next) {
                //std::cout<<key.ToString()<<std::endl;
                // std::cout<<"get"<<next->GetKey().ToString()<<"=?"<<key.ToString()<<"value"<<next->GetValue()<<std::endl;
                // for (int i = 0; i < 8; i++) {
                //     std::cout<<(int)next->GetKey()[i]<<' ';
                // }
                // std::cout<<next->GetKey().size()<<std::endl;
//                 for (int i = 0; i < 8; i++) {
//                     std::cout<<(int)key[i]<<' ';
//                 }
//                 std::cout<<key.size()<<"get"<<std::endl;
                if ((*next)->GetKey() == key) {
                    return (*next)->GetValue();
                }
                else
                {
                    next = &((*next)->next);
                }
            }
            return -1;
        }

        void Write(std::string path) {
            int keyFile = open((path + "/_key").c_str(), O_RDWR | O_CREAT, 0644);
            Entry **next;
            int block = 64 * 1024 * 5;
            char* buf = (char *) malloc(block);
            memset(buf, 0, sizeof(char) * block);
            char* buf8 = (char *) malloc(8);
            memset(buf8, 0, sizeof(char) * 8);
            int pos = 0;
            int count = 0;
            for (int i = 0; i < MAPSIZE; i++) {
                Entry **next = (values + i);
                while (*next) {
                    //memcmp((*next)->GetKey().data(),(buf+count),  8);
//                    for (int j = 0; j < 8; j++) {
//                        *(buf+count + j) = (*next)->GetKey().data()[j];
//                        //std::cout<<(int)(*next)->GetKey().data()[j]<<' ';
//                    }
                    LongToChars((*next)->GetKey(), buf+count);
                    //std::cout<<std::endl;
                    count += 8;
                    ShortToChars((*next)->GetValue(), buf+count);
                    //std::cout<<(*next)->GetKey()<<"short"<<std::endl;
                    count += 2;

                    //if (i < 10000) {
//                        int tmp = count - 10;
//                        for (int j = 0; j < 10; j++) {
//                            std::cout<<(int)*(buf + tmp + j)<<' ';
//                        }
//                        std::cout<<std::endl;
//                        std::cout<<(((int64_t)CharsToShort(buf + tmp + 8))<<12)<<std::endl;
                    //}

                    if (count == block) {
                        lseek(keyFile, pos, SEEK_SET);
                        write(keyFile, buf, block);
                        count = 0;
                        pos += block;

                    }
                    next = &((*next)->next);
                }
            }
            if (count > 0) {
                lseek(keyFile, pos, SEEK_SET);
                write(keyFile, buf, count);
            }
        }


    private:
        //uint32_t* hashKey;
        Entry** values;
    };
};
