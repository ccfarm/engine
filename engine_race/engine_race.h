// Copyright [2018] Alibaba Cloud All rights reserved
#ifndef ENGINE_RACE_ENGINE_RACE_H_
#define ENGINE_RACE_ENGINE_RACE_H_
#include <pthread.h>
#include <string>
#include <hash_map>
#include "include/engine.h"
#include "util.h"
#include "diy_map.h"

namespace polar_race {

class EngineRace : public Engine  {
 public:
  static RetCode Open(const std::string& name, Engine** eptr);

  explicit EngineRace(const std::string& dir) 
  : mu_(PTHREAD_MUTEX_INITIALIZER),
    db_lock_(NULL), path(dir), readyForWrite(false),
    readyForRead(false), readyForRange(false) {
    }

  ~EngineRace();

  RetCode Write(const PolarString& key,
      const PolarString& value) override;

  RetCode Read(const PolarString& key,
      std::string* value) override;

  /*
   * NOTICE: Implement 'Range' in quarter-final,
   *         you can skip it in preliminary.
   */
  RetCode Range(const PolarString& lower,
      const PolarString& upper,
      Visitor &visitor) override;

  void ReadyForWrite();
  void ReadyForRead();
  void ReadyForRange();

 private: 
  pthread_mutex_t mu_;
  FileLock* db_lock_;
  int keyFile;
  int valueFile;
  int64_t keyPos;
  int64_t valuePos;
  std::string path;
  bool readyForWrite;
  bool readyForRead;
  bool readyForRange;
  char* buf;
  char* buf4096;
  Map* map;
  int count;
};

}  // namespace polar_race

#endif  // ENGINE_RACE_ENGINE_RACE_H_
