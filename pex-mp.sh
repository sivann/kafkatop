#!/bin/bash

# Helps create multiplatform-pex

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
echo "Creating pex with for following platforms:"
pex3 interpreter inspect --all --interpreter-constraint "CPython>=3.9" --verbose --indent 4 | jq -r  .platform

# Iterate over platforms:
pex3 interpreter inspect --all --interpreter-constraint "CPython>=3.9" --verbose --indent 4 > platforms.json
deactivate

rm -fr venv-* wh-*
mkdir wh
echo "Gathering platform requirements."
cat platforms.json | jq -c '.' |while read x; 
do
    cppath=$(echo "$x" | jq -r .path) 
    cpversion=$(echo "$x" | jq -r .version)
    platform=$(echo "$x" | jq -r .platform)
    echo "Gathering platform requirements for python: $cpversion, platform: $platform, interpreter: $cppath"
    
    $cppath -m venv venv-${cpversion}
    . venv-${cpversion}/bin/activate
    python3 -m pip install --upgrade pip
    pip install pex
    pip wheel -r requirements.txt --wheel-dir wh # -${cpversion} # can be shared among verions
    deactivate
done



platforms_args=$(cat platforms.json | jq .platform |  sed -e 's/^/--platform /' | tr '\n' ' ')
echo ""
echo "Now run the following to create the multi-platform pex:"
echo pex . --disable-cache -o kafkatop -c kafkatop --python-shebang '#!/usr/bin/env python3' -f wh --resolve-local-platforms  $platforms_args >> makepex.$$
./makepex.$$

# If you just run the above it doesn't run(!) it needs to expand the platforms_args manually

#pex . --disable-cache -o kafkatop -c kafkatop --python-shebang '#!/usr/bin/env python3' --repo wh --no-pypi --no-build --resolve-local-platforms --platform "manylinux_2_34_x86_64-cp-3.10.8-cp310" --platform "manylinux_2_34_x86_64-cp-3.11.8-cp311" --platform "manylinux_2_34_x86_64-cp-3.9.18-cp39"
