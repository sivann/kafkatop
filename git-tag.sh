#!/bin/bash


# Detect sed type by checking version
# GNU sed reports "GNU sed" in --version output
if sed --version 2>&1 | grep -q GNU; then
    SED_TYPE="gnu"
else
    SED_TYPE="bsd"
fi
echo "Detected sed type: $SED_TYPE"

# Function to handle sed differences between GNU sed and BSD sed
sed_inplace() {
    if [ "$SED_TYPE" = "gnu" ]; then
        # GNU sed
        sed -i "$@"
    else
        # BSD sed (macOS default)
        sed -i '' "$@"
    fi
}

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
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


echo "$newtag" > tag.txt
sed_inplace "s/^VERSION.*/VERSION='$newtag'/" kafkatop.py
sed_inplace "s/^version = .*/version = \"$newtag\"/" pyproject.toml
git diff
git add tag.txt
git add kafkatop.py
git add pyproject.toml
read -p "Press [Enter] key commit changes:"
git commit -m "$newtag in tag.txt, kafkatop.py, and pyproject.toml"
git push 

echo ""
#read -p "Press [Enter] key to tag ${newtag}: "
git tag -a $newtag -m "Version ${newtag}"

echo ""
#read -p "Press [Enter] key to push tags: "
git push --tags

