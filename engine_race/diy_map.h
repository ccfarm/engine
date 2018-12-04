#include "util.h"
#include <iostream>
#include "include/engine.h"
#include <fcntl.h>
#include <unistd.h>
//#define MAPSIZE 64000000
#define MAPSIZE 64000

namespace polar_race {
class Entry{
    public:
        Entry(polar_race::PolarString& _key, int64_t _value) {
            key = _key;
            value = _value;
            next = 0;
        }
        polar_race::PolarString GetKey() {
            return key;
        }
        int64_t GetValue() {
            return value;
        }

        void SetValue(int64_t newValue) {
            value = newValue;
        }

        polar_race::Entry* next;

    private:
        polar_race::PolarString key;
        int64_t value;
    };

    class Map  {
    public:
        Map() {
            //hashKey = new uint32_t[MAPSIZE];
            //values = new Entry*[MAPSIZE];
            values = (Entry **)malloc(MAPSIZE * 8);
            memset(values, 0, sizeof(char) * 10);
            //std::cout<<(int64_t)(*values)<<std::endl;
            *values = 0;
            //std::cout<<(int64_t)(*values)<<std::endl;
        }
        void Set(char* _key, int64_t value) {
            polar_race::PolarString* key = new PolarString(_key, 8);
            uint32_t hash = StrHash(_key, 8) % MAPSIZE;
            Entry** next = (values + hash);
            //std::cout<<(int64_t)(values)<<std::endl<<(int64_t)(values + 1)<<std::endl;
            //std::cout<<(int64_t)(next)<<std::endl<<(int64_t)(values + hash)<<std::endl;
            //std::cout<<&next<<std::endl;
            //std::cout<<(int64_t)next<<std::endl;
            if (!*next) {
                *next = new Entry(*key, value);
                //std::cout<<(int64_t)next<<std::endl;
                return;
            }
            if ((*next)->GetKey() == *key) {
                (*next)->SetValue(value);
                delete key;
                delete _key;
                return;
            }
            while ((*next)->next) {
                if ((*next)->next->GetKey() == *key) {
                    (*next)->next->SetValue(value);
                    delete key;
                    delete _key;
                    return;
                }
                else
                {
                    next = &((*next)->next);
                }
            }
            Entry* entry = new Entry(*key, value);
            (*next)->next = entry;
        }
        int64_t Get(const PolarString& key) {
            //std::cout<<"hello";
            uint32_t hash = StrHash(key.ToString().c_str(), 8) % MAPSIZE;
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
            int block = 64 * 1024;
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
                    for (int j = 0; j < 8; j++) {
                        *(buf+count + j) = (*next)->GetKey().data()[j];
                        //std::cout<<(int)(*next)->GetKey().data()[j]<<' ';
                    }
                    //std::cout<<std::endl;
                    count += 8;
                    LongToChars((*next)->GetValue(), buf+count);
                    count += 8;
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