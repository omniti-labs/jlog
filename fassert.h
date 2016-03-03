#ifndef _FASSERT_H_
#define _FASSERT_H_

/*
extern void  fassertx(bool tf, int ln, const char *fn, const char *str);
extern void  fassertxend(void);
extern void  fassertxsetpath(const char *path);
extern const char *fassertxgetpath(void);

#define FASSERT(A,B)  { bool tf = (A); fassertx(tf, __LINE__, __FILE__, \
                                                (const char *)(B)); }

*/

#define FASSERT(A,B)  { int tf = (A); if ( tf == 0 ) \
                                        fprintf(stderr, "%s: %s %d\n", \
                                          (const char *)(B), __FILE__, \
                                                __LINE__); }

#endif
