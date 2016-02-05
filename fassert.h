#ifndef _FASSERT_H_
#define _FASSERT_H_

extern void  fassertx(bool tf, int ln, const char *fn, const char *str);
extern void  fassertxsetpath(const char *path);
extern const char *fassertxgetpath(void);

#define FASSERT(A,B)  { bool tf = (A); fassertx(tf, __LINE__, __FILE__, \
                                                (const char *)(B)); }

#endif
