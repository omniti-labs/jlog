/*
 * Copyright (c) 2016, Circonus, Inc. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *    * Redistributions of source code must retain the above copyright
 *      notice, this list of conditions and the following disclaimer.
 *    * Redistributions in binary form must reproduce the above
 *      copyright notice, this list of conditions and the following
 *      disclaimer in the documentation and/or other materials provided
 *      with the distribution.
 *    * Neither the name Circonus, Inc. nor the names
 *      of its contributors may be used to endorse or promote products
 *      derived from this software without specific prior written
 *      permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


#ifndef JLOG_NULL_COMPRESSION_PROVIDER_H
#define JLOG_NULL_COMPRESSION_PROVIDER_H

#include "jlog_compress.h"

#ifndef min
#define min(a,b) ((a) < (b) ? (a) : (b))
#endif

void jlog_null_compression_provider_init() {
}

size_t jlog_null_compression_provider_compress_bound(int source_size)
{
  return source_size;
}

int jlog_null_compression_provider_compress(const char *source, char *dest, int source_size, int max_dest_size) 
{
  memcpy(dest, source, min(max_dest_size, source_size));
  return min(max_dest_size, source_size);
}

int jlog_null_compression_provider_decompress(const char *source, char *dest, int compressed_size, int max_decompressed_size) 
{
  memcpy(dest, source, min(compressed_size, max_decompressed_size));
  return 0;
}

static struct jlog_compression_provider jlog_null_compression_provider = {
  .init = jlog_null_compression_provider_init,
  .compress_bound = jlog_null_compression_provider_compress_bound,
  .compress = jlog_null_compression_provider_compress,
  .decompress = jlog_null_compression_provider_decompress
};

#endif
