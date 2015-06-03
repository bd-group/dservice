#!/bin/sh
cd iie/mm/fuse;
GIT_SHA1=`(git show-ref --head --hash=8 2> /dev/null || echo 00000000) | head -n1`
GIT_DIRTY=`git diff --no-ext-diff . 2> /dev/null | wc -l`
BUILD_ID=`uname -n`"-"`date +%s`
test -f version.h || touch version.h
(cat version.h | grep SHA1 | grep $GIT_SHA1) && \
(cat version.h | grep DIRTY | grep $GIT_DIRTY) && exit 0 # Already up-to-date
echo "#define MMFS_GIT_SHA1 \"$GIT_SHA1\"" > version.h
echo "#define MMFS_GIT_DIRTY \"$GIT_DIRTY\"" >> version.h
echo "#define MMFS_BUILD_ID \"$BUILD_ID\"" >> version.h
