#ifndef _FAKEBOOL_H_
#define _FAKEBOOL_H_

// for systems where <stdbool.h> is not available

#define true  1
#define false 0
typedef int   bool;

#endif
