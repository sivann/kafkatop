#!/bin/bash

# sivann 2023
# Helps create multiplatform-pex

PYTHON_MINVER="3.9"
commit_hash=$(git rev-parse --short HEAD)
git_desc=$(git describe)
echo "Starting $0 on $(pwd), git tag: $git_desc"

#sed -i "s/^VERSION=.*/VERSION=$(cat tag.txt)/" kafkatop.py
sed -i "s/^VERSION=.*/VERSION=${git_desc}/" kafkatop.py

echo "Gathering python platform info using python:"
python3 --version

rm -fr venv
python3 -m venv venv
. venv/bin/activate
if [[ -z "$VIRTUAL_ENV" ]] ; then
    echo "not running in virtualenv, exiting"
    exit 1
fi

# Create a venv from the 1st python in the path to get started with pex and detect platforms
python3 -m pip install --upgrade pip
pip install pex

echo "Make sure all required python3's are in path"
echo ""
echo "*** Creating pex with for following platforms:"
pex3 interpreter inspect --interpreter-constraint "CPython>=${PYTHON_MINVER}" --verbose --indent 4  > pexinspect.json
cat pexinspect.json | jq -r  .platform

# Iterate over platforms:
deactivate

# Iterate over python platforms and fetch dependencies in a wheel format under wh/ for each platform
rm -fr venv-* wh-*
mkdir wh
echo ""
echo "*** Gathering platform requirements."
cat pexinspect.json | jq -c '.' |while read x; 
do
    cppath=$(echo "$x" | jq -r .path) 
    cpversion=$(echo "$x" | jq -r .version)
    platform=$(echo "$x" | jq -r .platform)
    echo ""
    echo "*******************************"
    echo "*** Gathering platform requirements for python: $cpversion, platform: $platform, interpreter: $cppath"
    echo "*******************************"
    
    $cppath -m venv venv-${cpversion}
    . venv-${cpversion}/bin/activate
    python3 -m pip install --upgrade pip
    pip install pex
    pip wheel -r requirements.txt --wheel-dir wh # -${cpversion} # can be shared among verions
    deactivate
    echo $platform >> platforms.tmp
done


# Now create the multiplatform binary pex using the downloaded wheel files (under wh/)
pexfn="kafkatop-${git_desc}-$(uname -m).pex"
#pexfn="kafkatop-$(uname -m).pex"
rm -f "$pexfn"

platforms_args=$(cat pexinspect.json | jq .platform |  sed -e 's/^/--platform /' | tr '\n' ' ')
echo ""
rm -f makepex.*
echo '. venv/bin/activate' > makepex.$$
echo "pex . --disable-cache -o $pexfn -c kafkatop.py"' --python-shebang "#!/usr/bin/env python3" -f wh --resolve-local-platforms'  $platforms_args >> makepex.$$
chmod +x  makepex.$$
echo "*** Now running the following to create the multi-platform pex: ./makepex.$$"
cat makepex.$$
./makepex.$$

# Now kafkatop has been created, create release and release notes for GitHub (releasebody.md)
arch=$(uname -m)
echo "Created $pexfn"
cp -f "$pexfn" kafkatop
tar zcf kafkatop-release.tar.gz kafkatop
ls -lh $pexfn
echo -e "Kafkatop version ${git_desc}\n" > releasebody.md
echo -e "\n\n" >> releasebody.md
echo -e "\n\nThis is a multi-platform binary release (pex), that can run in any **x86_64** linux distribution with a compatible Python version.\n\n**How to run**: download the zip file, extract kafkatop and run it. \n\n**Requires** one of the following Python versions in your path:\n" >> releasebody.md

cat pexinspect.json | jq -r  .version | cut -d. -f1,2 | sort -uV | sed 's/^/\- /' >> releasebody.md

