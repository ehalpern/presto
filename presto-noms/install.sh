#!/bin/bash
set -x

presto=~/presto
prestoetc=~/presto/etc

mkdir -p ~/bootstrap; cd ~/bootstrap
aws s3 sync s3://bucketdb-bootstrap .

mkdir -p ~/go/bin; tar -xf noms.tar.gz -C ~/go/bin

rm -fr ${presto}/plugin/presto-noms-*
rm -fr ${presto}/plugin/noms
unzip -o presto-noms.zip -d ${presto}/plugin
mv -f ${presto}/plugin/presto-noms-* ${presto}/plugin/noms

rm -fr ${prestoetc}
cp -r etc ${prestoetc}
ln -s ${prestoetc}/config.single.properties ${prestoetc}/config.properties
${presto}/bin/launcher restart
