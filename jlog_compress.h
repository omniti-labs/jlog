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

#ifndef _JLOG_COMPRESS_H
#define _JLOG_COMPRESS_H

#include "jlog.h"
#include "jlog_config.h"

struct jlog_compression_provider {
  /**
   * providers that require initialization should do so here
   */
  void (*init)();

  /**
   * returns the required size of the destination buffer to compress into when dealing with a source size
   */
  size_t (*compress_bound)(const int source_size);

  /**
   * returns the number of bytes written into dest or zero on error
   */
  int (*compress)(const char *source, char *dest, int sourceSize, int max_dest_size);

  /**
   * returns the number of bytes decompressed into dest buffer or < 0 on error
   */
  int (*decompress)(const char *source, char *dest, int compressed_size, int max_decompressed_size);
};


/**
 * set the provider to the chosen type.  will return 0 on success, or < 0 on error
 */
int jlog_set_compression_provider(const jlog_compression_provider_choice jcp);

/**
 * will allocate into 'dest' the required size based on source_bytes.  It's up to caller to free dest.
 */
int jlog_compress(const char *source, const size_t source_bytes, char **dest, size_t *dest_bytes);

/**
 * reverse the compression
 */
int jlog_decompress(const char *source, const size_t source_bytes, char *dest, size_t dest_bytes);


#endif
