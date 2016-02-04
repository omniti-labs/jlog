#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <time.h>
#include <libgen.h>
#include <unistd.h>

#include "fassert.h"

static int fd = -1;

static int openit(void)
{
  char fn[128];

  memset(fn, 0, sizeof(fn));
  (void)snprintf(fn, sizeof(fn)-1, "../ernie.fassert%ld", time(NULL));
  int xfd = open(fn, O_CREAT|O_EXCL|O_WRONLY, 0644);
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
  memset(s, 0, sizeof(s));
  memset(s2, 0, sizeof(s2));
  memset(s3, 0, sizeof(s3));
  time_t tx = 0;
  (void)time(&tx);
  (void)snprintf(s2, sizeof(s2)-1, "%ld: ", tx);
  size_t leen2 = strlen(s2);
  (void)memcpy((void *)&s[0], (void *)&s2[0], leen2);
  leenrun += leen2;
  if ( ln > 0 )
    {
      memset(s2, 0, sizeof(s2));
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
  memcpy((void *)&s[leenrun], &nl, 1);
  leenrun++;
  (void)write(fd, s, leenrun);
}
