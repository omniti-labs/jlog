#include <stdio.h>

#include "jlog_config.h"

#include <sys/types.h>
#include <sys/stat.h>
#if HAVE_FCNTL_H
#include <fcntl.h>
#endif
#if HAVE_TIME_H
#include <time.h>
#endif
#if HAVE_SYS_TIME_H
#include <sys/time.h>
#endif
#if HAVE_LIBGEN_H
#include <libgen.h>
#endif
#if HAVE_UNISTD_H
#include <unistd.h>
#endif
#if HAVE_SYS_PARAM_H
#include <sys/param.h>
#endif
#if HAVE_STDBOOL_H
#include <stdbool.h>
#else
#include "fakebool.h"
#endif

#include "fassert.h"

static const char bn[MAXPATHLEN];

static int fd = -1;

void fassertxsetpath(const char *path)
{
  if ( path == NULL || path[0] == 0 )
    memset((void *)&bn[0], 0, sizeof(bn));
  else
    {
      size_t leen = strlen(path);
      if ( leen >= MAXPATHLEN )
	return;
      memcpy((void *)&bn[0], (void *)path, leen);
    }
}

const char *fassertxgetpath(void)
{
  return &bn[0];
}

static int openit(void)
{
  if ( bn[0] == 0 )
    return -1;
  size_t leen = strlen(bn) + 24;
  if ( leen >= MAXPATHLEN )
    return -1;
  char *fn = (char *)calloc(leen+1, sizeof(char));
  if ( fn == NULL )
    return -1;
  (void)snprintf(fn, leen, "%s%cfassert%ld", bn, IFS_CH, time(NULL));
  int xfd = open(fn, O_CREAT|O_EXCL|O_WRONLY, 0644);
  free((void *)fn);
  return xfd;
}

void fassertx(bool tf, int ln, const char *fn, const char *str)
{
  char s[512 + 32 + 512 + 32 + 17];
  char s2[31];
  char s3[512 + 10];

  if ( tf == true )
    return;
  if ( fd < 0 )
    {
      fd = openit();
      if ( fd < 0 )
	return;
    }
  size_t leenrun = 0;
  size_t leen = 0;
  if ( str != NULL && str[0] != '\0' )
    {
      leen = strlen(str);
      if ( leen > 512 )
	leen = 512;
    }
  else
    {
      str = "(no extra info)";
      leen = strlen(str);
    }
  memset((void *)&s[0], 0, sizeof(s));
  memset((void *)&s2[0], 0, sizeof(s2));
  memset((void *)&s3[0], 0, sizeof(s3));
  time_t tx = 0;
  (void)time(&tx);
  (void)snprintf(s2, sizeof(s2)-1, "%ld: ", tx);
  size_t leen2 = strlen(s2);
  (void)memcpy((void *)&s[0], (void *)&s2[0], leen2);
  leenrun += leen2;
  if ( ln > 0 )
    {
      memset((void *)&s2[0], 0, sizeof(s2));
      (void)snprintf(s2, sizeof(s2)-1, "line=%d ", ln);
      leen2 = strlen(s2);
      memcpy((void *)&s[leenrun], (void *)&s2[0], leen2);
      leenrun += leen2;
    }
  if ( fn != NULL && fn[0] != '\0' )
    {
      // this is awful, but we must have writable memory, and it is
      // likely that arg3 is a string literal, so overwriting part
      // of it would be really bad
      char *xfn = strdup(fn);
      if ( xfn == NULL )
	return;			/* out of memory, nothing to do */
      char *bn = basename(xfn);
      (void)snprintf(s3, sizeof(s3)-1, "file=%s ", bn);
      free((void *)xfn);
      leen2 = strlen(s3);
      (void)memcpy((void *)&s[leenrun], (void *)&s3[0], leen2);
      leenrun += leen2;
    }
  memcpy((void *)&s[leenrun], str, leen);
  leenrun += leen;
  char nl = '\n';
  memcpy((void *)&s[leenrun], (void *)&nl, 1);
  leenrun++;
  (void)write(fd, s, leenrun);
}
