#!/bin/bash
path="$(pwd)"
declare -a mocks=(
"tracker/service"
"tracker/proxy"
)
for i in "${mocks[@]}"
do
   parts=($(echo $i | tr '/' "\n"))
   index=$((${#parts[@]}-2))
   pkg="${parts[index]}"
   mockgen -package="mock$pkg" -destination="$path/mocks/${i}.go" -source="$path/internal/${i}.go"
done;