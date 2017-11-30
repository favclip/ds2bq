#!/bin/sh -eux

targets=`find . -type f \( -name '*.go' -and -not -iwholename '*vendor*' \)`
packages=$(go list ./...)

# Apply tools
export PATH=$(pwd)/build-cmd:$PATH
which goimports golint staticcheck gosimple unused
go generate $packages
goimports -w $targets
go tool vet $targets
golint $packages
staticcheck $packages
gosimple $packages
unused $packages

goapp test $packages $@
