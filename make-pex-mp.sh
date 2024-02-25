#!/bin/bash

# Helps create multiplatform-pex

sed -i "s/^VERSION=.*/VERSION=$(cat tag.txt)/" kafkatop.py

echo "Gathering python platform info"
rm -fr venv
python3 -m venv venv
. venv/bin/activate
if [[ -z "$VIRTUAL_ENV" ]] ; then
    echo "not running in virtualenv, exiting"
    exit 1
fi

python3 -m pip install --upgrade pip
pip install pex

echo "Make sure all required python3's are in path"
echo ""
echo "*** Creating pex with for following platforms:"
pex3 interpreter inspect --interpreter-constraint "CPython>=3.9" --verbose --indent 4 | jq -r  .platform

# Iterate over platforms:
pex3 interpreter inspect --interpreter-constraint "CPython>=3.9" --verbose --indent 4 > platforms.json
deactivate

rm -fr venv-* wh-*
mkdir wh
echo ""
echo "*** Gathering platform requirements."
cat platforms.json | jq -c '.' |while read x; 
do
    cppath=$(echo "$x" | jq -r .path) 
    cpversion=$(echo "$x" | jq -r .version)
    platform=$(echo "$x" | jq -r .platform)
    echo ""
    echo "*** Gathering platform requirements for python: $cpversion, platform: $platform, interpreter: $cppath"
    
    $cppath -m venv venv-${cpversion}
    . venv-${cpversion}/bin/activate
    python3 -m pip install --upgrade pip
    pip install pex
    pip wheel -r requirements.txt --wheel-dir wh # -${cpversion} # can be shared among verions
    deactivate
done


pexfn="kafkatop-$(cat tag.txt)-$(uname -m).pex"
rm -f "$pexfn"

platforms_args=$(cat platforms.json | jq .platform |  sed -e 's/^/--platform /' | tr '\n' ' ')
echo ""
rm -f makepex.*
echo '. venv/bin/activate' > makepex.$$
echo "pex . --disable-cache -o $pexfn -c kafkatop.py"' --python-shebang "#!/usr/bin/env python3" -f wh --resolve-local-platforms'  $platforms_args >> makepex.$$
chmod +x  makepex.$$
echo "*** Now running the following to create the multi-platform pex: ./makepex.$$"
cat makepex.$$
./makepex.$$

echo "Created $pexfn"
ls -lh $pexfn
