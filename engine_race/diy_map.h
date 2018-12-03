#include "util.h"
#include <iostream>
#include "include/engine.h"
//#define MAPSIZE 64000000
#define MAPSIZE 64000

namespace polar_race {
    class Entry{
    public:
        Entry(polar_race::PolarString& _key, int64_t _value) {
            key = _key;
            value = _value;
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
            values = new Entry*[MAPSIZE];
        }
        void Set(char* _key, int64_t value) {
            polar_race::PolarString* key = new PolarString(_key, 8);
            uint32_t hash = StrHash(_key, 8) % MAPSIZE;
            Entry** next = (values + hash);
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
                    *next = (*next)->next;
                }
            }
            Entry* entry = new Entry(*key, value);
            (*next)->next = entry;
        }
        int64_t Get(const PolarString& key) {
            uint32_t hash = StrHash(key.ToString().c_str(), 8) % MAPSIZE;
            Entry* next = *(values + hash);
            //std::cout<<&next<<std::endl;
            //std::cout<<(int64_t)next<<std::endl;
            //next = 0x7fff3be80148;
            while (next) {
                // std::cout<<"get"<<next->GetKey().ToString()<<"=?"<<key.ToString()<<"value"<<next->GetValue()<<std::endl;
                // for (int i = 0; i < 8; i++) {
                //     std::cout<<(int)next->GetKey()[i]<<' ';
                // }
                // std::cout<<next->GetKey().size()<<std::endl;
                // for (int i = 0; i < 8; i++) {
                //     std::cout<<(int)key[i]<<' ';
                // }
                // std::cout<<key.size()<<"get"<<std::endl;
                if (next->GetKey() == key) {
                    return next->GetValue();
                }
                else
                {
                    next = next->next;
                }
            }
            return -1;
        }


    private:
        //uint32_t* hashKey;
        Entry** values;
    };



};