#!/bin/bash

set -e

# this is for macOS
[ -x /usr/local/bin/gsed ] && sed=/usr/local/bin/gsed || sed=sed

lasthash=`git log --decorate=full --all --pretty=format:'%h %d'  |grep 'refs/tags'|head -1|awk '{print $1}'`
lasttag=`git describe --tags $lasthash`

if [ "$#" -ne 1 ]
then
	echo ""
	echo "Usage: $0 <new version>"
	echo ""
	echo "Previous version: ${lasttag}" 
	exit 1
fi

newtag="$1"

if [[ $newtag =~ [0-9]+[.][0-9]+$ ]] ; then
	 echo "New version: $newtag" 
else
	echo "Invalid version: $newtag, aborting"
	exit 1
fi

if [ `git tag|grep $newtag` ] ; then
	echo "$newtag already exists, aborting"
	exit 2
fi

GITTOP=`git rev-parse --show-toplevel`


lastdate=`git log -1 --format=%ci`

if [ `git status --short|wc -l` -ne 0 ] ; then
	git status --short
	echo "It seems there are not commited changes, please check"
fi

echo ""
#read -p "Press [Enter] key to push :"
git push 

echo ""
#read -p "Press [Enter] key to tag ${newtag}: "
git tag -a $newtag -m "Version ${newtag}"

echo ""
#read -p "Press [Enter] key to push tags: "
git push --tags

