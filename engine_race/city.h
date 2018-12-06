//
// Created by 王超 on 2018/12/6.
//


#include <stdlib.h>  // for size_t.
#include <stdint.h>

typedef uint8_t uint8;
typedef uint32_t uint32;
typedef uint64_t uint64;

// Hash function for a byte array.  Most useful in 32-bit binaries.
uint32 CityHash32(const char *buf, size_t len);


