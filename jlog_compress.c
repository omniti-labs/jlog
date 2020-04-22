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


#include <stdio.h>
#include "jlog_config.h"
#include "jlog_compress.h"
#include "jlog_private.h"
#include "fassert.h"

#include "jlog_null_compression_provider.h"
#include "jlog_lz4_compression_provider.h"

static struct jlog_compression_provider *provider = &jlog_null_compression_provider;

#define unlikely(x)    __builtin_expect(!!(x), 0)

int 
jlog_set_compression_provider(const jlog_compression_provider_choice jcp) 
{
  switch(jcp) {
  case JLOG_COMPRESSION_NULL:
    provider = &jlog_null_compression_provider;
    break;
  case JLOG_COMPRESSION_LZ4:
    {
#ifdef HAVE_LZ4_H      
      provider = &jlog_lz4_compression_provider;
#else
      fprintf(stderr, "lz4 not detected on system, cannot set");
      return -1;
#endif
      break;      
    }
  };
  return 0;
}

int 
jlog_compress(const char *source, const size_t source_bytes, char **dest, size_t *dest_bytes)
{
  FASSERT(NULL, dest != NULL, "jlog_compress: dest pointer is NULL");
  size_t required = provider->compress_bound(source_bytes);

  if (*dest_bytes < required) {
    /* incoming buffer not large enough, must allocate */
    *dest = malloc(required);
    FASSERT(NULL, *dest != NULL, "jlog_compress: malloc failed");
  }

  int rv = provider->compress(source, *dest, source_bytes, required);
  if (rv > 0) {
#ifdef DEBUG
    fprintf(stderr, "Compressed %d bytes into %d bytes\n", source_bytes, rv);
#endif
    *dest_bytes = rv;
    return 0;
  }
  return rv;
}

int 
jlog_decompress(const char *source, const size_t source_bytes, char *dest, size_t dest_bytes)
{
  if (unlikely(dest == NULL)) {
    return -1;
  }

  int rv = provider->decompress(source, dest, source_bytes, dest_bytes);
  if (rv >= 0) {
#ifdef DEBUG
    fprintf(stderr, "Compressed %d bytes into %d bytes\n", source_bytes, rv);
#endif
    return 0;
  }
  return rv;
}
