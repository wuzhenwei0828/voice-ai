#!/bin/bash
FILE_PATH=$(readlink --canonicalize --no-newline $BASH_SOURCE)
FILE_PATH=$(cd "$(dirname "$FILE_PATH")"; pwd)
cd $FILE_PATH

set -e

help_string=".sh [-t|-p] [-f]"
help() { echo "Usage: $help_string"; exit 0; }

while getopts tpf opt
do
    case $opt in
        t)
            test=true
            ;;
        p)
            prod=true
            ;;
        f)
            option="--no-cache"
            ;;
        ?)
            help
            ;;
    esac
done

#镜像仓库地址
repository="harbor.cowarobot.cn"
#名字空间
namespace="softrepo"
#项目名称
packagename="voice-ai"
publish="test"

if [[ $prod ]]; then
    publish="prod"
    namespace="softpro"
fi

if [[ $test ]]; then
    namespace="softrepo"
fi

if [[ ! -n $platform ]];then
    platform=`arch`
    echo "auto select arch:${platform}"
fi

case $platform in
"arm64")
    platform="linux/arm64"
    ;;
"x86_64"|"amd64")
    platform="linux/amd64"
    ;;
*)
    echo "unknown cpu-arch ${platform}"
    exit
    ;;
esac

branch=$(git rev-parse --abbrev-ref HEAD)
branch=$(printf '%s' "$branch" | sed 's#[/[:space:]]#-#g')
commitid=$(git rev-parse --short HEAD)
datetime=$(date +%Y%m%d)

imagename=$repository/$namespace/$packagename
version=$publish.$branch.$commitid.$datetime

if [[ $prod ]]; then
    docker build ${option:-} --platform=$platform --network=host --target runtime \
        -t $imagename:$version .
    docker push $imagename:$version

    docker tag $imagename:$version $imagename:${publish}_latest
    docker push $imagename:${publish}_latest

    docker tag $imagename:$version $imagename:$CI_COMMIT_SHA
    docker push $imagename:$CI_COMMIT_SHA
else
    docker build ${option:-} --platform=$platform --network=host --target runtime \
        -t $imagename:$version .
    docker tag $imagename:$version $imagename:${publish}_latest

    echo "push to dst registry"
    docker push $imagename:$version
    docker push $imagename:${publish}_latest
    docker rmi $imagename:$version
fi

echo "built image: $imagename:$version"
