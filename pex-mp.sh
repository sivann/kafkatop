#!/bin/bash

if [[ -z "$VIRTUAL_ENV" ]] ; then
    echo "not running in virtualenv, exiting"
    exit 1
fi

echo "Make sure all required python3's are in path"
echo ""
echo "Creating pex with for following platforms:"
pex3 interpreter inspect --all --verbose --indent 4 |jq -r  .platform | egrep  'cp-3.[1,8,9][0-9]*'
echo ""

platforms=$(pex3 interpreter inspect --all --verbose --indent 4 |jq -r  .platform | egrep  'cp-3.[1,8,9][0-9]*'| sed -e 's/^/--platform /' | tr '\n' ' ')
pex . --disable-cache -o kafka-lagstats -c kafka-lagstats.py --python-shebang '#!/usr/bin/env python3'
