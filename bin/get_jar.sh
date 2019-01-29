#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

SCRIPT_DIR=$(cd $(dirname $0); pwd)
OUTPUT_DIR=$SCRIPT_DIR/..
com=$1
shift

function show_usage() {
cat << EOS
$0 command
command:
list: get version list from maven repository

get:  get
      get jar from maven repository

      source: that is where to get jer files from
              case file:   read as version file
              case dir:    gather jar files from local directory
              not defined: use versions.sh as version file

      version file is for setting of yosegi
      if one or some versions are not exists, release version are gotten.

      version is described by file name as ${module}/latest/version/${version}

      hdfs_lib: target hdfs directory to storage yosegi lib.

EOS
  exit 0
}

jars_dir=$OUTPUT_DIR/jars
tmp_dir=$jars_dir/tmp
mvn_url=https://repo1.maven.org/maven2/jp/co/yahoo/yosegi
module_settings=(
  'yosegi yosegi'
  'yosegi_hive yosegi-hive'
)


function get_version_list() {
  for ((i=0; ${#module_settings[*]}>$i; i++)); do
    local tmp=(${module_settings[$i]})
    local category=${tmp[0]}
    local module=${tmp[1]}
    echo $category

    curl -s $mvn_url/$module/maven-metadata.xml \
      | grep "<version>" \
      | sed -E 's/ *<version> *//g' \
      | sed -E 's/ *<\/version> *//g'
    echo
  done
  exit 0
}

function get_jars_from_mvn() {
  local target_dir=$1
  get_release_version yosegi $yosegi; yosegi=$release_version
  get_release_version yosegi_hive $yosegi_hive; yosegi_hive=$release_version
  sed "s/\$yosegi_version/$yosegi/g" $SCRIPT_DIR/pom.xml.template | \
  sed "s/\$yosegi_hive_version/$yosegi_hive/g" >  $jars_dir/pom.xml

  mvn dependency:copy-dependencies -DoutputDirectory=$tmp_dir -f $jars_dir/pom.xml

  gathering_jars $tmp_dir $target_dir

  rm -r $tmp_dir
  rm $jars_dir/pom.xml
}

function get_release_version() {
  local category=$1
  local version=$2
  if [ ! -z $version ]; then
    release_version=$version
    return
  fi

  for ((i=0; ${#module_settings[*]}>$i; i++)); do
    local tmp=(${module_settings[$i]})
    local module=${tmp[1]}
    if [ ${tmp[0]} == $category ]; then
      release_version=$(curl -s $mvn_url/$module/maven-metadata.xml \
        | grep "<release>" \
        | sed -E 's/ *<release> *//g' \
        | sed -E 's/ *<\/release> *//g')
      return
    fi
  done
}


function copy_jars() {
  local category=$1
  local src_dir=$2
  local target_dir=$3
  local version_ext=$4
  local version

  for fn in ${jars[@]}; do
    src=$(find $src_dir -name $fn-[0-9.]*.jar | sort | tail -n 1)
    version=$(echo $src | sed 's/.*'$fn-'\([0-9.]*\)\.jar/\1/')$version_ext
    for v in $version latest; do
      mkdir -p $target_dir/$category/$v
      cp $src $target_dir/$category/$v/${fn}.jar
    done
  done

  local version_dir=$target_dir/$category/latest/version
  if [ -d $version_dir ]; then rm -r $version_dir; fi
  mkdir -p $version_dir
  touch $version_dir/$version
}

function gathering_jars() {
  local src_dir=$1
  local target_dir=$2
  local version_ext=$3
  if [ -z $version_ext ]; then version_ext=''; fi

  jars=(
    yosegi
    yosegi-hive
  )
  copy_jars yosegi $src_dir $target_dir $version_ext
}

function get_jars() {
  while getopts i:o:l:h OPT
  do
    case $OPT in
      h)  show_usage ;;
      \?) show_usage ;;
    esac
  done
  shift $((OPTIND - 1))

  mkdir -p $jars_dir

  get_jars_from_mvn $jars_dir
}

case "$com" in
  "" ) show_usage ;;
  "list" ) get_version_list ;;
  "get"  ) get_jars $* ;;
  * ) echo "$com is not command"; show_usage ;;
esac

