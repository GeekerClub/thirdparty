// Copyright (C) 2017, For toft authors.
// Author: Pengyu Sun (sunpengyu@nbjl.nankai.edu.cn)
//
// Description: from https://github.com/fcambus/logswan/blob/master/deps/hll/hll.h

#include "hyperloglog.h"

namespace hll {

const char *invalid_hll_err = "-INVALIDOBJ Corrupted HLL object detected\r\n";

bool ParseHLLFromString(hllhdr* hll, std::string hll_str) {
    gunir::HyperLogLog hll_message;
    hll_message.ParseFromString(hll_str);
    memcpy(hll->magic,"HYLL",4);;
    hll->encoding = hll_message.encoding();
    for (int32_t i = 0; i < hll_message.card_size(); ++i) {
        hll->card[i] = hll_message.card(i);
    }
    for (int32_t i = 0; i < hll_message.registers_size(); ++i) {
        hll->registers[i] = hll_message.registers(i);
    }
    return true;
}

std::string hllSerializeToPBString(const hllhdr& hll) {
    std::string result;
    gunir::HyperLogLog hll_message;
    hll_message.set_encoding(hll.encoding);
    for (int32_t i = 0; i < 8; ++i) {
        hll_message.add_card(hll.card[i]);
    }
    //
    for (int32_t i = 0; i < HLL_REGISTERS; ++i) {
        hll_message.add_registers(hll.registers[i]);
    }
    hll_message.SerializeToString(&result);
    return result;
}

robj *createObject(int type, void *ptr) {
    robj *o = (robj*)zmalloc(sizeof(*o));
    o->type = type;
    o->encoding = 0;
    o->ptr = ptr;
    o->refcount = 1;

    /* Set the LRU to the current lruclock (minutes resolution), or
     * alternatively the LFU counter. */
    return o;
}

/* ========================= HyperLogLog algorithm  ========================= */

/* Our hash function is MurmurHash2, 64 bit version.
 * It was modified for Redis in order to provide the same result in
 * big and little endian archs (endian neutral). */
uint64_t MurmurHash64A (const void * key, int len, unsigned int seed) {
    const uint64_t m = 0xc6a4a7935bd1e995;
    const int r = 47;
    uint64_t h = seed ^ (len * m);
    const uint8_t *data = (const uint8_t *)key;
    const uint8_t *end = data + (len-(len&7));

    while(data != end) {
        uint64_t k;

#if (BYTE_ORDER == LITTLE_ENDIAN)
    #ifdef USE_ALIGNED_ACCESS
    memcpy(&k,data,sizeof(uint64_t));
    #else
        k = *((uint64_t*)data);
    #endif
#else
        k = (uint64_t) data[0];
        k |= (uint64_t) data[1] << 8;
        k |= (uint64_t) data[2] << 16;
        k |= (uint64_t) data[3] << 24;
        k |= (uint64_t) data[4] << 32;
        k |= (uint64_t) data[5] << 40;
        k |= (uint64_t) data[6] << 48;
        k |= (uint64_t) data[7] << 56;
#endif

        k *= m;
        k ^= k >> r;
        k *= m;
        h ^= k;
        h *= m;
        data += 8;
    }

    switch(len & 7) {
    case 7: h ^= (uint64_t)data[6] << 48;
    case 6: h ^= (uint64_t)data[5] << 40;
    case 5: h ^= (uint64_t)data[4] << 32;
    case 4: h ^= (uint64_t)data[3] << 24;
    case 3: h ^= (uint64_t)data[2] << 16;
    case 2: h ^= (uint64_t)data[1] << 8;
    case 1: h ^= (uint64_t)data[0];
            h *= m;
    };

    h ^= h >> r;
    h *= m;
    h ^= h >> r;
    return h;
}

/* Given a string element to add to the HyperLogLog, returns the length
 * of the pattern 000..1 of the element hash. As a side effect 'regp' is
 * set to the register index this element hashes to. */
int hllPatLen(unsigned char *ele, size_t elesize, long *regp) {
    uint64_t hash, bit, index;
    int count;

    /* Count the number of zeroes starting from bit HLL_REGISTERS
     * (that is a power of two corresponding to the first bit we don't use
     * as index). The max run can be 64-P+1 bits.
     *
     * Note that the final "1" ending the sequence of zeroes must be
     * included in the count, so if we find "001" the count is 3, and
     * the smallest count possible is no zeroes at all, just a 1 bit
     * at the first position, that is a count of 1.
     *
     * This may sound like inefficient, but actually in the average case
     * there are high probabilities to find a 1 after a few iterations. */
    hash = MurmurHash64A(ele,elesize,0xadc83b19ULL);
    //LOG(ERROR) << "HASH: " << hash;
    index = hash & HLL_P_MASK; /* Register index. */
    hash |= ((uint64_t)1<<63); /* Make sure the loop terminates. */
    bit = HLL_REGISTERS; /* First bit not used to address the register. */
    count = 1; /* Initialized to 1 since we count the "00000...1" pattern. */
    while((hash & bit) == 0) {
        count++;
        bit <<= 1;
    }
    *regp = (int) index;
    return count;
}

/* ================== Dense representation implementation  ================== */

/* "Add" the element in the dense hyperloglog data structure.
 * Actually nothing is added, but the max 0 pattern counter of the subset
 * the element belongs to is incremented if needed.
 *
 * 'registers' is expected to have room for HLL_REGISTERS plus an
 * additional byte on the right. This requirement is met by sds strings
 * automatically since they are implicitly null terminated.
 *
 * The function always succeed, however if as a result of the operation
 * the approximated cardinality changed, 1 is returned. Otherwise 0
 * is returned. */
int hllDenseAdd(uint8_t *registers, unsigned char *ele, size_t elesize) {
    //LOG(ERROR) << "IN dense add";
    uint8_t oldcount, count;
    long index;

    /* Update the register if this element produced a longer run of zeroes. */
    count = hllPatLen(ele,elesize,&index);
    HLL_DENSE_GET_REGISTER(oldcount,registers,index);
    /*do {
        uint8_t *_p = (uint8_t*) registers;
            unsigned long _byte = index*HLL_BITS/8;
            unsigned long _fb = index*HLL_BITS&7;
            unsigned long _fb8 = 8 - _fb;
            unsigned long b0 = _p[_byte];
            unsigned long b1 = _p[_byte+1];
            oldcount = ((b0 >> _fb) | (b1 << _fb8)) & HLL_REGISTER_MAX;
    } while(0);*/
    if (count > oldcount) {
        HLL_DENSE_SET_REGISTER(registers,index,count);
        return 1;
    } else {
        return 0;
    }
}

/* Compute SUM(2^-reg) in the dense representation.
 * PE is an array with a pre-computer table of values 2^-reg indexed by reg.
 * As a side effect the integer pointed by 'ezp' is set to the number
 * of zero registers. */
double hllDenseSum(uint8_t *registers, double *PE, int *ezp) {
    double E = 0;
    int j, ez = 0;

    /* Redis default is to use 16384 registers 6 bits each. The code works
     * with other values by modifying the defines, but for our target value
     * we take a faster path with unrolled loops. */
    if (HLL_REGISTERS == 16384 && HLL_BITS == 6) {
        uint8_t *r = registers;
        unsigned long r0, r1, r2, r3, r4, r5, r6, r7, r8, r9,
                      r10, r11, r12, r13, r14, r15;
        for (j = 0; j < 1024; j++) {
            /* Handle 16 registers per iteration. */
            r0 = r[0] & 63; if (r0 == 0) ez++;
            r1 = (r[0] >> 6 | r[1] << 2) & 63; if (r1 == 0) ez++;
            r2 = (r[1] >> 4 | r[2] << 4) & 63; if (r2 == 0) ez++;
            r3 = (r[2] >> 2) & 63; if (r3 == 0) ez++;
            r4 = r[3] & 63; if (r4 == 0) ez++;
            r5 = (r[3] >> 6 | r[4] << 2) & 63; if (r5 == 0) ez++;
            r6 = (r[4] >> 4 | r[5] << 4) & 63; if (r6 == 0) ez++;
            r7 = (r[5] >> 2) & 63; if (r7 == 0) ez++;
            r8 = r[6] & 63; if (r8 == 0) ez++;
            r9 = (r[6] >> 6 | r[7] << 2) & 63; if (r9 == 0) ez++;
            r10 = (r[7] >> 4 | r[8] << 4) & 63; if (r10 == 0) ez++;
            r11 = (r[8] >> 2) & 63; if (r11 == 0) ez++;
            r12 = r[9] & 63; if (r12 == 0) ez++;
            r13 = (r[9] >> 6 | r[10] << 2) & 63; if (r13 == 0) ez++;
            r14 = (r[10] >> 4 | r[11] << 4) & 63; if (r14 == 0) ez++;
            r15 = (r[11] >> 2) & 63; if (r15 == 0) ez++;

            /* Additional parens will allow the compiler to optimize the
             * code more with a loss of precision that is not very relevant
             * here (floating point math is not commutative!). */
            E += (PE[r0] + PE[r1]) + (PE[r2] + PE[r3]) + (PE[r4] + PE[r5]) +
                 (PE[r6] + PE[r7]) + (PE[r8] + PE[r9]) + (PE[r10] + PE[r11]) +
                 (PE[r12] + PE[r13]) + (PE[r14] + PE[r15]);
            r += 12;
        }
    } else {
        for (j = 0; j < HLL_REGISTERS; j++) {
            unsigned long reg;

            HLL_DENSE_GET_REGISTER(reg,registers,j);
            if (reg == 0) {
                ez++;
                /* Increment E at the end of the loop. */
            } else {
                E += PE[reg]; /* Precomputed 2^(-reg[j]). */
            }
        }
        E += ez; /* Add 2^0 'ez' times. */
    }
    *ezp = ez;
    return E;
}

/* ================== Sparse representation implementation  ================= */

/* Convert the HLL with sparse representation given as input in its dense
 * representation. Both representations are represented by SDS strings, and
 * the input representation is freed as a side effect.
 *
 * The function returns C_OK if the sparse representation was valid,
 * otherwise C_ERR is returned if the representation was corrupted. */
int hllSparseToDense(robj *o) {
    sds sparse = (char*)o->ptr, dense;
    struct hllhdr *hdr, *oldhdr = (struct hllhdr*)sparse;
    int idx = 0, runlen, regval;
    uint8_t *p = (uint8_t*)sparse, *end = p+sdslen((char*)sparse);

    /* If the representation is already the right one return ASAP. */
    hdr = (struct hllhdr*) sparse;
    if (hdr->encoding == HLL_DENSE) return 0;

    /* Create a string of the right size filled with zero bytes.
     * Note that the cached cardinality is set to 0 as a side effect
     * that is exactly the cardinality of an empty HLL. */
    dense = sdsnewlen(NULL,HLL_DENSE_SIZE);
    hdr = (struct hllhdr*) dense;
    *hdr = *oldhdr; /* This will copy the magic and cached cardinality. */
    hdr->encoding = HLL_DENSE;

    /* Now read the sparse representation and set non-zero registers
     * accordingly. */
    p += HLL_HDR_SIZE;
    while(p < end) {
        if (HLL_SPARSE_IS_ZERO(p)) {
            runlen = HLL_SPARSE_ZERO_LEN(p);
            idx += runlen;
            p++;
        } else if (HLL_SPARSE_IS_XZERO(p)) {
            runlen = HLL_SPARSE_XZERO_LEN(p);
            idx += runlen;
            p += 2;
        } else {
            runlen = HLL_SPARSE_VAL_LEN(p);
            regval = HLL_SPARSE_VAL_VALUE(p);
            while(runlen--) {
                HLL_DENSE_SET_REGISTER(hdr->registers,idx,regval);
                idx++;
            }
            p++;
        }
    }

    /* If the sparse representation was valid, we expect to find idx
     * set to HLL_REGISTERS. */
    if (idx != HLL_REGISTERS) {
        sdsfree((char*)dense);
        return C_ERR;
    }

    /* Free the old representation and set the new one. */
    sdsfree((char*)o->ptr);
    o->ptr = dense;
    return C_OK;
}

/* "Add" the element in the sparse hyperloglog data structure.
 * Actually nothing is added, but the max 0 pattern counter of the subset
 * the element belongs to is incremented if needed.
 *
 * The object 'o' is the String object holding the HLL. The function requires
 * a reference to the object in order to be able to enlarge the string if
 * needed.
 *
 * On success, the function returns 1 if the cardinality changed, or 0
 * if the register for this element was not updated.
 * On error (if the representation is invalid) -1 is returned.
 *
 * As a side effect the function may promote the HLL representation from
 * sparse to dense: this happens when a register requires to be set to a value
 * not representable with the sparse representation, or when the resulting
 * size would be greater than server.hll_sparse_max_bytes. */
int hllSparseAdd(robj *o, unsigned char *ele, size_t elesize) {
    //LOG(ERROR) << "IN sparse ADD";
    struct hllhdr *hdr;
    uint8_t oldcount, count, *sparse, *end, *p, *prev, *next;
    long index, first, span;
    long is_zero = 0, is_xzero = 0, is_val = 0, runlen = 0;

    /* Update the register if this element produced a longer run of zeroes. */
    count = hllPatLen(ele,elesize,&index);
    //LOG(ERROR) << "count: " << (uint32_t)count;

    /* If the count is too big to be representable by the sparse representation
     * switch to dense representation. */
    if (count > HLL_SPARSE_VAL_MAX_VALUE) goto promote;

    /* When updating a sparse representation, sometimes we may need to
     * enlarge the buffer for up to 3 bytes in the worst case (XZERO split
     * into XZERO-VAL-XZERO). Make sure there is enough space right now
     * so that the pointers we take during the execution of the function
     * will be valid all the time. */

    o->ptr = (char*)sdsMakeRoomFor((char*)o->ptr,3);

    /* Step 1: we need to locate the opcode we need to modify to check
     * if a value update is actually needed. */
    sparse = p = ((uint8_t*)o->ptr) + HLL_HDR_SIZE;
    end = p + sdslen((char*)o->ptr) - HLL_HDR_SIZE;

    first = 0;
    prev = NULL; /* Points to previos opcode at the end of the loop. */
    next = NULL; /* Points to the next opcode at the end of the loop. */
    span = 0;
    while(p < end) {
        long oplen;

        /* Set span to the number of registers covered by this opcode.
         *
         * This is the most performance critical loop of the sparse
         * representation. Sorting the conditionals from the most to the
         * least frequent opcode in many-bytes sparse HLLs is faster. */
        oplen = 1;
        if (HLL_SPARSE_IS_ZERO(p)) {
            span = HLL_SPARSE_ZERO_LEN(p);
        } else if (HLL_SPARSE_IS_VAL(p)) {
            span = HLL_SPARSE_VAL_LEN(p);
        } else { /* XZERO. */
            span = HLL_SPARSE_XZERO_LEN(p);
            oplen = 2;
        }
        /* Break if this opcode covers the register as 'index'. */
        if (index <= first+span-1) break;
        prev = p;
        p += oplen;
        first += span;
    }
    if (span == 0) return -1; /* Invalid format. */

    next = HLL_SPARSE_IS_XZERO(p) ? p+2 : p+1;
    if (next >= end) next = NULL;

    /* Cache current opcode type to avoid using the macro again and
     * again for something that will not change.
     * Also cache the run-length of the opcode. */
    if (HLL_SPARSE_IS_ZERO(p)) {
        is_zero = 1;
        runlen = HLL_SPARSE_ZERO_LEN(p);
    } else if (HLL_SPARSE_IS_XZERO(p)) {
        is_xzero = 1;
        runlen = HLL_SPARSE_XZERO_LEN(p);
    } else {
        is_val = 1;
        runlen = HLL_SPARSE_VAL_LEN(p);
    }

    /* Step 2: After the loop:
     *
     * 'first' stores to the index of the first register covered
     *  by the current opcode, which is pointed by 'p'.
     *
     * 'next' ad 'prev' store respectively the next and previous opcode,
     *  or NULL if the opcode at 'p' is respectively the last or first.
     *
     * 'span' is set to the number of registers covered by the current
     *  opcode.
     *
     * There are different cases in order to update the data structure
     * in place without generating it from scratch:
     *
     * A) If it is a VAL opcode already set to a value >= our 'count'
     *    no update is needed, regardless of the VAL run-length field.
     *    In this case PFADD returns 0 since no changes are performed.
     *
     * B) If it is a VAL opcode with len = 1 (representing only our
     *    register) and the value is less than 'count', we just update it
     *    since this is a trivial case. */

    // for goto
     int seqlen;
     int oldlen;
     int deltalen;
     int last;
     uint8_t seq[5], *n;
     int scanlen;

    if (is_val) {
        oldcount = HLL_SPARSE_VAL_VALUE(p);
        /* Case A. */
        if (oldcount >= count) return 0;

        /* Case B. */
        if (runlen == 1) {
            HLL_SPARSE_VAL_SET(p,count,1);
            goto updated;
        }
    }

    /* C) Another trivial to handle case is a ZERO opcode with a len of 1.
     * We can just replace it with a VAL opcode with our value and len of 1. */
    if (is_zero && runlen == 1) {
        HLL_SPARSE_VAL_SET(p,count,1);
        goto updated;
    }

    /* D) General case.
     *
     * The other cases are more complex: our register requires to be updated
     * and is either currently represented by a VAL opcode with len > 1,
     * by a ZERO opcode with len > 1, or by an XZERO opcode.
     *
     * In those cases the original opcode must be split into muliple
     * opcodes. The worst case is an XZERO split in the middle resuling into
     * XZERO - VAL - XZERO, so the resulting sequence max length is
     * 5 bytes.
     *
     * We perform the split writing the new sequence into the 'new' buffer
     * with 'newlen' as length. Later the new sequence is inserted in place
     * of the old one, possibly moving what is on the right a few bytes
     * if the new sequence is longer than the older one. */
    n = (uint8_t*)seq;
    last = first+span-1; /* Last register covered by the sequence. */
    int len;

    if (is_zero || is_xzero) {
        /* Handle splitting of ZERO / XZERO. */
        if (index != first) {
            len = index-first;
            if (len > HLL_SPARSE_ZERO_MAX_LEN) {
                HLL_SPARSE_XZERO_SET(n,len);
                n += 2;
            } else {
                HLL_SPARSE_ZERO_SET(n,len);
                n++;
            }
        }
        HLL_SPARSE_VAL_SET(n,count,1);
        n++;
        if (index != last) {
            len = last-index;
            if (len > HLL_SPARSE_ZERO_MAX_LEN) {
                HLL_SPARSE_XZERO_SET(n,len);
                n += 2;
            } else {
                HLL_SPARSE_ZERO_SET(n,len);
                n++;
            }
        }
    } else {
        /* Handle splitting of VAL. */
        int curval = HLL_SPARSE_VAL_VALUE(p);

        if (index != first) {
            len = index-first;
            HLL_SPARSE_VAL_SET(n,curval,len);
            n++;
        }
        HLL_SPARSE_VAL_SET(n,count,1);
        n++;
        if (index != last) {
            len = last-index;
            HLL_SPARSE_VAL_SET(n,curval,len);
            n++;
        }
    }

    /* Step 3: substitute the new sequence with the old one.
     *
     * Note that we already allocated space on the sds string
     * calling sdsMakeRoomFor(). */
     seqlen = n-seq;
     oldlen = is_xzero ? 2 : 1;
     deltalen = seqlen-oldlen;

     if (deltalen > 0 &&
         sdslen((char*)o->ptr)+deltalen > 3000) goto promote;
     if (deltalen && next) memmove(next+deltalen,next,end-next);
     sdsIncrLen((char*)o->ptr,deltalen);
     memcpy(p,seq,seqlen);
     end += deltalen;

updated:
    /* Step 4: Merge adjacent values if possible.
     *
     * The representation was updated, however the resulting representation
     * may not be optimal: adjacent VAL opcodes can sometimes be merged into
     * a single one. */
    p = prev ? prev : sparse;
    scanlen = 5; /* Scan up to 5 upcodes starting from prev. */
    while (p < end && scanlen--) {
        if (HLL_SPARSE_IS_XZERO(p)) {
            p += 2;
            continue;
        } else if (HLL_SPARSE_IS_ZERO(p)) {
            p++;
            continue;
        }
        /* We need two adjacent VAL opcodes to try a merge, having
         * the same value, and a len that fits the VAL opcode max len. */
        if (p+1 < end && HLL_SPARSE_IS_VAL(p+1)) {
            int v1 = HLL_SPARSE_VAL_VALUE(p);
            int v2 = HLL_SPARSE_VAL_VALUE(p+1);
            if (v1 == v2) {
                int len = HLL_SPARSE_VAL_LEN(p)+HLL_SPARSE_VAL_LEN(p+1);
                if (len <= HLL_SPARSE_VAL_MAX_LEN) {
                    HLL_SPARSE_VAL_SET(p+1,v1,len);
                    memmove(p,p+1,end-p);
                    sdsIncrLen((char*)o->ptr,-1);
                    end--;
                    /* After a merge we reiterate without incrementing 'p'
                     * in order to try to merge the just merged value with
                     * a value on its right. */
                    continue;
                }
            }
        }
        p++;
    }

    /* Invalidate the cached cardinality. */
    hdr = (hllhdr*)o->ptr;
    HLL_INVALIDATE_CACHE(hdr);
    return 1;

promote: /* Promote to dense representation. */
    if (hllSparseToDense(o) == -1) return -1; /* Corrupted HLL. */
    hdr = (hllhdr*)o->ptr;

    /* We need to call hllDenseAdd() to perform the operation after the
     * conversion. However the result must be 1, since if we need to
     * convert from sparse to dense a register requires to be updated.
     *
     * Note that this in turn means that PFADD will make sure the command
     * is propagated to slaves / AOF, so if there is a sparse -> dense
     * convertion, it will be performed in all the slaves as well. */
    int dense_retval = hllDenseAdd(hdr->registers, ele, elesize);
    // serverAssert(dense_retval == 1);
    return dense_retval;
}

/* Compute SUM(2^-reg) in the sparse representation.
 * PE is an array with a pre-computer table of values 2^-reg indexed by reg.
 * As a side effect the integer pointed by 'ezp' is set to the number
 * of zero registers. */
double hllSparseSum(uint8_t *sparse, int sparselen, double *PE, int *ezp, int *invalid) {
    double E = 0;
    int ez = 0, idx = 0, runlen, regval;
    uint8_t *end = sparse+sparselen, *p = sparse;

    while(p < end) {
        if (HLL_SPARSE_IS_ZERO(p)) {
            runlen = HLL_SPARSE_ZERO_LEN(p);
            idx += runlen;
            ez += runlen;
            /* Increment E at the end of the loop. */
            p++;
        } else if (HLL_SPARSE_IS_XZERO(p)) {
            runlen = HLL_SPARSE_XZERO_LEN(p);
            idx += runlen;
            ez += runlen;
            /* Increment E at the end of the loop. */
            p += 2;
        } else {
            runlen = HLL_SPARSE_VAL_LEN(p);
            regval = HLL_SPARSE_VAL_VALUE(p);
            idx += runlen;
            E += PE[regval]*runlen;
            p++;
        }
    }
    if (idx != HLL_REGISTERS && invalid) *invalid = 1;
    E += ez; /* Add 2^0 'ez' times. */
    *ezp = ez;
    return E;
}

/* ========================= HyperLogLog Count ==============================
 * This is the core of the algorithm where the approximated count is computed.
 * The function uses the lower level hllDenseSum() and hllSparseSum() functions
 * as helpers to compute the SUM(2^-reg) part of the computation, which is
 * representation-specific, while all the rest is common. */

/* Implements the SUM operation for uint8_t data type which is only used
 * internally as speedup for PFCOUNT with multiple keys. */
double hllRawSum(uint8_t *registers, double *PE, int *ezp) {
    double E = 0;
    int j, ez = 0;
    uint64_t *word = (uint64_t*) registers;
    uint8_t *bytes;

    for (j = 0; j < HLL_REGISTERS/8; j++) {
        if (*word == 0) {
            ez += 8;
        } else {
            bytes = (uint8_t*) word;
            if (bytes[0]) E += PE[bytes[0]]; else ez++;
            if (bytes[1]) E += PE[bytes[1]]; else ez++;
            if (bytes[2]) E += PE[bytes[2]]; else ez++;
            if (bytes[3]) E += PE[bytes[3]]; else ez++;
            if (bytes[4]) E += PE[bytes[4]]; else ez++;
            if (bytes[5]) E += PE[bytes[5]]; else ez++;
            if (bytes[6]) E += PE[bytes[6]]; else ez++;
            if (bytes[7]) E += PE[bytes[7]]; else ez++;
        }
        word++;
    }
    E += ez; /* 2^(-reg[j]) is 1 when m is 0, add it 'ez' times for every
                zero register in the HLL. */
    *ezp = ez;
    return E;
}

/* Return the approximated cardinality of the set based on the harmonic
 * mean of the registers values. 'hdr' points to the start of the SDS
 * representing the String object holding the HLL representation.
 *
 * If the sparse representation of the HLL object is not valid, the integer
 * pointed by 'invalid' is set to non-zero, otherwise it is left untouched.
 *
 * hllCount() supports a special internal-only encoding of HLL_RAW, that
 * is, hdr->registers will point to an uint8_t array of HLL_REGISTERS element.
 * This is useful in order to speedup PFCOUNT when called against multiple
 * keys (no need to work with 6-bit integers encoding). */
uint64_t hllCount(struct hllhdr *hdr, int *invalid) {
    double m = HLL_REGISTERS;
    double E, alpha = 0.7213/(1+1.079/m);
    int j, ez; /* Number of registers equal to 0. */

    /* We precompute 2^(-reg[j]) in a small table in order to
     * speedup the computation of SUM(2^-register[0..i]). */
    static int initialized = 0;
    static double PE[64];
    if (!initialized) {
        PE[0] = 1; /* 2^(-reg[j]) is 1 when m is 0. */
        for (j = 1; j < 64; j++) {
            /* 2^(-reg[j]) is the same as 1/2^reg[j]. */
            PE[j] = 1.0/(1ULL << j);
        }
        initialized = 1;
    }

    /* Compute SUM(2^-register[0..i]). */
    //LOG(ERROR) << "ENCODEING: " << (uint32_t)hdr->encoding << " DENSE: " << HLL_DENSE;
    if (hdr->encoding == HLL_DENSE) {
        E = hllDenseSum(hdr->registers,PE,&ez);
    } else if (hdr->encoding == HLL_SPARSE) {
        E = hllSparseSum(hdr->registers,
                         sdslen((sds)hdr)-HLL_HDR_SIZE,PE,&ez,invalid);
    } else if (hdr->encoding == HLL_RAW) {
        E = hllRawSum(hdr->registers,PE,&ez);
    } else {
        LOG(ERROR) << "Unknown HyperLogLog encoding in hllCount()";
        // serverPanic("Unknown HyperLogLog encoding in hllCount()");
    }

    /* Apply loglog-beta to the raw estimate. See:
     * "LogLog-Beta and More: A New Algorithm for Cardinality Estimation
     * Based on LogLog Counting" Jason Qin, Denys Kim, Yumei Tung
     * arXiv:1612.02284 */
    double zl = log(ez + 1);
    double beta = -0.370393911*ez +
                   0.070471823*zl +
                   0.17393686*pow(zl,2) +
                   0.16339839*pow(zl,3) +
                  -0.09237745*pow(zl,4) +
                   0.03738027*pow(zl,5) +
                  -0.005384159*pow(zl,6) +
                   0.00042419*pow(zl,7);

    E  = llroundl(alpha*m*(m-ez)*(1/(E+beta)));
    return (uint64_t) E;
}

/* Call hllDenseAdd() or hllSparseAdd() according to the HLL encoding. */

int hllAdd(robj *o, std::string key) {
    //LOG(ERROR) << "IN ADD";
    struct hllhdr *hdr = (hllhdr*)(o->ptr);
    switch(hdr->encoding) {
    case HLL_DENSE: return hllDenseAdd(hdr->registers,(unsigned char*)key.c_str(),key.length());
    case HLL_SPARSE: return hllSparseAdd(o,(unsigned char*)key.c_str(),key.length());
    default: return -1; /* Invalid representation. */
    }
}
/* Merge by computing MAX(registers[i],hll[i]) the HyperLogLog 'hll'
 * with an array of uint8_t HLL_REGISTERS registers pointed by 'max'.
 *
 * The hll object must be already validated via isHLLObjectOrReply()
 * or in some other way.
 *
 * If the HyperLogLog is sparse and is found to be invalid, C_ERR
 * is returned, otherwise the function always succeeds. */
int hllMerge(uint8_t *max, robj *hll) {
    struct hllhdr *hdr = (hllhdr*)hll->ptr;
    int i;

    if (hdr->encoding == HLL_DENSE) {
        uint8_t val;

        for (i = 0; i < HLL_REGISTERS; i++) {
            HLL_DENSE_GET_REGISTER(val,hdr->registers,i);
            if (val > max[i]) max[i] = val;
        }
    } else {
        uint8_t *p = (uint8_t*)hll->ptr, *end = p + sdslen((char*)hll->ptr);
        long runlen, regval;

        p += HLL_HDR_SIZE;
        i = 0;
        while(p < end) {
            if (HLL_SPARSE_IS_ZERO(p)) {
                runlen = HLL_SPARSE_ZERO_LEN(p);
                i += runlen;
                p++;
            } else if (HLL_SPARSE_IS_XZERO(p)) {
                runlen = HLL_SPARSE_XZERO_LEN(p);
                i += runlen;
                p += 2;
            } else {
                runlen = HLL_SPARSE_VAL_LEN(p);
                regval = HLL_SPARSE_VAL_VALUE(p);
                while(runlen--) {
                    if (regval > max[i]) max[i] = regval;
                    i++;
                }
                p++;
            }
        }
        if (i != HLL_REGISTERS) return C_ERR;
    }
    return C_OK;
}

bool Merge(robj *return_o, robj *hll) {
    struct hllhdr *hdr;

    uint8_t max[HLL_REGISTERS];
    memset(max,0,sizeof(max));

    if (hllMerge(max,return_o) == C_ERR) {
        LOG(ERROR) << "merge return_o error";
        return false;
    }
    if (hllMerge(max,hll) == C_ERR) {
        LOG(ERROR) << "merge hll error";
        return false;
    }

    hdr = (hllhdr*)return_o->ptr;
    for (int j = 0; j < HLL_REGISTERS; j++) {
        HLL_DENSE_SET_REGISTER(hdr->registers,j,max[j]);
    }
    HLL_INVALIDATE_CACHE(hdr);
    return true;
}

robj * hllCreate() {
    robj *o;
    struct hllhdr *hdr;
    sds s;
    uint8_t *p;
    int sparselen = HLL_HDR_SIZE +
                    (((HLL_REGISTERS+(HLL_SPARSE_XZERO_MAX_LEN-1)) /
                     HLL_SPARSE_XZERO_MAX_LEN)*2);
    int aux;

    /* Populate the sparse representation with as many XZERO opcodes as
     * needed to represent all the registers. */
    aux = HLL_REGISTERS;
    s = sdsnewlen(NULL,sparselen);
    p = (uint8_t*)s + HLL_HDR_SIZE;
    while(aux) {
        int xzero = HLL_SPARSE_XZERO_MAX_LEN;
        if (xzero > aux) xzero = aux;
        HLL_SPARSE_XZERO_SET(p,xzero);
        p += 2;
        aux -= xzero;
    }
    //serverAssert((p-(uint8_t*)s) == sparselen);

    /* Create the actual object. */
    o = createObject(0,s);
    hdr = (hllhdr*)o->ptr;
    memcpy(hdr->magic,"HYLL",4);
    hdr->encoding = HLL_SPARSE;
    hllSparseToDense(o);
    return o;
}

void hllFreeStringObject(robj *o) {
    if (o->encoding == 0) {
        sdsfree((char*)o->ptr);
    }
    zfree(o);
}

}
/*
using namespace gunir;
int main() {
    std::cout << "=== begin ===" << std::endl;;
    robj *o = HLLCreate();
    int num = 0;
    //HLLCreate(&hll);

    std::cout << "magic: " << ((hllhdr*)o->ptr)->magic << std::endl;
    std::cout << "encoding: " << (uint32_t)(((hllhdr*)o->ptr)->encoding) << std::endl;
    num = hllCount((hllhdr*)o->ptr, NULL);
    std::cout << "count: " << num << std::endl;

    std::string key1 = "1";
    Add(o, key1);
    std::cout << "===========" << std::endl;
    num = hllCount((hllhdr*)o->ptr, NULL);
    std::cout << "count: " << num << std::endl;
    std::string key2 = "2";
    Add(o, key2);

    std::string key = "";
    for (int i =0; i < 100; ++i) {
        key = key + 'a';
        Add(o, key);
    }

    num = hllCount((hllhdr*)o->ptr, NULL);
    std::cout << "count: " << num << std::endl;


    robj *o_merge = HLLCreate();
    for (int i =0; i < 100; ++i) {
        key = key + 'a';
        Add(o_merge, key);
    }

    Merge(o, o_merge);
    num = hllCount((hllhdr*)o->ptr, NULL);
    std::cout << "count: " << num << std::endl;

    std::string str = SerializeToPBString(*((hllhdr*)o->ptr));
    std::cout << "serialize string size: " << str.size() << std::endl;

    robj *o_form = HLLCreate();
    ParseHLLFromString((hllhdr*)o_form->ptr, str);

    num = hllCount((hllhdr*)o_form->ptr, NULL);
    std::cout << "count from string: " << num << std::endl;

    std::cout << "=== finish ===" << std::endl;
    return 0;
}*/
