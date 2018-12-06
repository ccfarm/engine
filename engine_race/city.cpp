#include "config.h"
#include "city.h"

#include <string.h>  // for memcpy and memset


static uint32 UNALIGNED_LOAD32(const char *p) {
    uint32 result;
    memcpy(&result, p, sizeof(result));
    return result;
}

#ifdef _MSC_VER

#include <stdlib.h>
#define bswap_32(x) _byteswap_ulong(x)
#define bswap_64(x) _byteswap_uint64(x)

#elif defined(__APPLE__)

// Mac OS X / Darwin features
#include <libkern/OSByteOrder.h>
#define bswap_32(x) OSSwapInt32(x)
#define bswap_64(x) OSSwapInt64(x)

#elif defined(__sun) || defined(sun)

#include <sys/byteorder.h>
#define bswap_32(x) BSWAP_32(x)
#define bswap_64(x) BSWAP_64(x)

#elif defined(__FreeBSD__)

#include <sys/endian.h>
#define bswap_32(x) bswap32(x)
#define bswap_64(x) bswap64(x)

#elif defined(__OpenBSD__)

#include <sys/types.h>
#define bswap_32(x) swap32(x)
#define bswap_64(x) swap64(x)

#elif defined(__NetBSD__)

#include <sys/types.h>
#include <machine/bswap.h>
#if defined(__BSWAP_RENAME) && !defined(__bswap_32)
#define bswap_32(x) bswap32(x)
#define bswap_64(x) bswap64(x)
#endif

#else

    #define	bswap_8(x)	((x) & 0xff)
    #define	bswap_16(x)	((bswap_8(x) << 8) | bswap_8((x) >> 8))
    #define	bswap_32(x)	((bswap_16(x) << 16) | bswap_16((x) >> 16))
    #define	bswap_64(x)	((bswap_32(x) << 32) | bswap_32((x) >> 32))

#endif

#ifdef WORDS_BIGENDIAN
#define uint32_in_expected_order(x) (bswap_32(x))
#define uint64_in_expected_order(x) (bswap_64(x))
#else
#define uint32_in_expected_order(x) (x)
#define uint64_in_expected_order(x) (x)
#endif

#if !defined(LIKELY)
#if HAVE_BUILTIN_EXPECT
#define LIKELY(x) (__builtin_expect(!!(x), 1))
#else
#define LIKELY(x) (x)
#endif
#endif

static uint32 Fetch32(const char *p) {
    return uint32_in_expected_order(UNALIGNED_LOAD32(p));
}

// Some primes between 2^63 and 2^64 for various uses.
static const uint64 k0 = 0xc3a5c85c97cb3127ULL;
static const uint64 k1 = 0xb492b66fbe98f273ULL;
static const uint64 k2 = 0x9ae16a3b2f90404fULL;

// Magic numbers for 32-bit hashing.  Copied from Murmur3.
static const uint32 c1 = 0xcc9e2d51;
static const uint32 c2 = 0x1b873593;

// A 32-bit to 32-bit integer hash copied from Murmur3.
static uint32 fmix(uint32 h)
{
    h ^= h >> 16;
    h *= 0x85ebca6b;
    h ^= h >> 13;
    h *= 0xc2b2ae35;
    h ^= h >> 16;
    return h;
}

static uint32 Rotate32(uint32 val, int shift) {
    // Avoid shifting by 32: doing so yields an undefined result.
    return shift == 0 ? val : ((val >> shift) | (val << (32 - shift)));
}

#undef PERMUTE3
#define swap(a,b) {a=a^b;b=a^b;a=a^b;}
#define PERMUTE3(a, b, c) do { swap(a, b); swap(a, c); } while (0)

static uint32 Mur(uint32 a, uint32 h) {
    // Helper from Murmur3 for combining two 32-bit values.
    a *= c1;
    a = Rotate32(a, 17);
    a *= c2;
    h ^= a;
    h = Rotate32(h, 19);
    return h * 5 + 0xe6546b64;
}

static uint32 Hash32Len13to24(const char *s, size_t len) {
    uint32 a = Fetch32(s - 4 + (len >> 1));
    uint32 b = Fetch32(s + 4);
    uint32 c = Fetch32(s + len - 8);
    uint32 d = Fetch32(s + (len >> 1));
    uint32 e = Fetch32(s);
    uint32 f = Fetch32(s + len - 4);
    uint32 h = len;

    return fmix(Mur(f, Mur(e, Mur(d, Mur(c, Mur(b, Mur(a, h)))))));
}

static uint32 Hash32Len0to4(const char *s, size_t len) {
    uint32 b = 0;
    uint32 c = 9;
    for (size_t i = 0; i < len; i++) {
        signed char v = s[i];
        b = b * c1 + v;
        c ^= b;
    }
    return fmix(Mur(b, Mur(len, c)));
}

static uint32 Hash32Len5to12(const char *s, size_t len) {
    uint32 a = len, b = len * 5, c = 9, d = b;
    a += Fetch32(s);
    b += Fetch32(s + len - 4);
    c += Fetch32(s + ((len >> 1) & 4));
    return fmix(Mur(c, Mur(b, Mur(a, d))));
}

__stdcall uint32 CityHash32(const char *s, size_t len) {
    if (len <= 24) {
        return len <= 12 ?
               (len <= 4 ? Hash32Len0to4(s, len) : Hash32Len5to12(s, len)) :
               Hash32Len13to24(s, len);
    }

    // len > 24
    uint32 h = len, g = c1 * len, f = g;
    uint32 a0 = Rotate32(Fetch32(s + len - 4) * c1, 17) * c2;
    uint32 a1 = Rotate32(Fetch32(s + len - 8) * c1, 17) * c2;
    uint32 a2 = Rotate32(Fetch32(s + len - 16) * c1, 17) * c2;
    uint32 a3 = Rotate32(Fetch32(s + len - 12) * c1, 17) * c2;
    uint32 a4 = Rotate32(Fetch32(s + len - 20) * c1, 17) * c2;
    h ^= a0;
    h = Rotate32(h, 19);
    h = h * 5 + 0xe6546b64;
    h ^= a2;
    h = Rotate32(h, 19);
    h = h * 5 + 0xe6546b64;
    g ^= a1;
    g = Rotate32(g, 19);
    g = g * 5 + 0xe6546b64;
    g ^= a3;
    g = Rotate32(g, 19);
    g = g * 5 + 0xe6546b64;
    f += a4;
    f = Rotate32(f, 19);
    f = f * 5 + 0xe6546b64;
    size_t iters = (len - 1) / 20;
    do {
        uint32 a0 = Rotate32(Fetch32(s) * c1, 17) * c2;
        uint32 a1 = Fetch32(s + 4);
        uint32 a2 = Rotate32(Fetch32(s + 8) * c1, 17) * c2;
        uint32 a3 = Rotate32(Fetch32(s + 12) * c1, 17) * c2;
        uint32 a4 = Fetch32(s + 16);
        h ^= a0;
        h = Rotate32(h, 18);
        h = h * 5 + 0xe6546b64;
        f += a1;
        f = Rotate32(f, 19);
        f = f * c1;
        g += a2;
        g = Rotate32(g, 18);
        g = g * 5 + 0xe6546b64;
        h ^= a3 + a1;
        h = Rotate32(h, 19);
        h = h * 5 + 0xe6546b64;
        g ^= a4;
        g = bswap_32(g) * 5;
        h += a4 * 5;
        h = bswap_32(h);
        f += a0;
        PERMUTE3(f, h, g);
        s += 20;
    } while (--iters != 0);
    g = Rotate32(g, 11) * c1;
    g = Rotate32(g, 17) * c1;
    f = Rotate32(f, 11) * c1;
    f = Rotate32(f, 17) * c1;
    h = Rotate32(h + g, 19);
    h = h * 5 + 0xe6546b64;
    h = Rotate32(h, 17) * c1;
    h = Rotate32(h + f, 19);
    h = h * 5 + 0xe6546b64;
    h = Rotate32(h, 17) * c1;
    return h;
}