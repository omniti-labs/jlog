dnl
dnl $ Id: $
dnl

PHP_ARG_WITH(jlog, jlog,[  --with-jlog[=DIR]    With jlog support])


if test "$PHP_JLOG" != "no"; then
  if test "$PHP_JLOG" == "yes"; then
    PHP_JLOG="/opt/ecelerity"
  fi
  export CPPFLAGS="$CPPFLAGS $INCLUDES -DHAVE_JLOG"
  
  PHP_ADD_INCLUDE(..)
  PHP_ADD_INCLUDE(../..)
  PHP_ADD_INCLUDE(.)
  export OLD_CPPFLAGS="$CPPFLAGS"
  export CPPFLAGS="$CPPFLAGS $INCLUDES -DHAVE_JLOG"
  AC_CHECK_HEADER([jlog.h], [], AC_MSG_ERROR('jlog.h' header not found))
  export CPPFLAGS="$OLD_CPPFLAGS"
  PHP_SUBST(JLOG_SHARED_LIBADD)

  PHP_ADD_LIBRARY_WITH_PATH(jlog, $PHP_JLOG/lib64, JLOG_SHARED_LIBADD)


  PHP_SUBST(JLOG_SHARED_LIBADD)
  AC_DEFINE(HAVE_JLOG, 1, [ ])
  PHP_NEW_EXTENSION(jlog, jlog.c , $ext_shared)

fi

