#ifndef GETOPT_LONG_H
#define GETOPT_LONG_H

#ifdef __cplusplus
extern "C" {
#endif

extern char *optarg;
extern int optind;
extern int opterr;
extern int optopt;

struct option {
  const char *name;
  int has_arg;
  int *flag;
  int val;
};

#define no_argument             0
#define required_argument       1
#define optional_argument       2

extern int getopt_long (int argc, char *const *argv, const char *shortopts,
                        const struct option *longopts, int *longind);

#if defined(REPLACE_GETOPT) && defined(WIN32)
extern int getopt(int nargc, char * const *nargv, const char *options);
#endif

#ifdef __cplusplus
}  /* Close scope of 'extern "C"' declaration which encloses file. */
#endif

#endif
