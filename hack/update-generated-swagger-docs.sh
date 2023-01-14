#!/usr/bin/env bash

# Copyright 2015 The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This script generates `types_swagger_doc_generated.go` files for API group
# versions. That file contains functions on API structs that return
# the comments that should be surfaced for the corresponding API type
# in our API docs.

set -o errexit
set -o nounset
set -o pipefail

KUBE_ROOT=$(dirname "${BASH_SOURCE[0]}")/..
source "${KUBE_ROOT}/hack/lib/init.sh"

# Generates types_swagger_doc_generated file for the given group version.
# $1: Name of the group version
# $2: Path to the directory where types.go for that group version exists. This
# is the directory where the file will be generated.
gen_types_swagger_doc() {
  local group_version=$1
  local gv_dir=$2
  local TMPFILE
  TMPFILE="${TMPDIR:-/tmp}/types_swagger_doc_generated.$(date +%s).go"

  echo "Generating swagger type docs for ${group_version} at ${gv_dir}"

  {
    echo -e "$(cat hack/boilerplate/boilerplate.generatego.txt)\n"
    echo "package ${group_version##*/}"
    cat <<EOF

// This file contains a collection of methods that can be used from go-restful to
// generate Swagger API documentation for its models. Please read this PR for more
// information on the implementation: https://github.com/emicklei/go-restful/pull/215
//
// TODOs are ignored from the parser (e.g. TODO(andronat):... || TODO:...) if and only if
// they are on one line! For multiple line or blocks that you want to ignore use ---.
// Any context after a --- is ignored.
//
// Those methods can be generated by using hack/update-generated-swagger-docs.sh

// AUTO-GENERATED FUNCTIONS START HERE. DO NOT EDIT.
EOF
  } > "${TMPFILE}"

  _output/bin/genswaggertypedocs -s \
    "${gv_dir}/types.go" \
    -f - \
    >>  "${TMPFILE}"

  echo "// AUTO-GENERATED FUNCTIONS END HERE" >> "${TMPFILE}"

  gofmt -w -s "${TMPFILE}"
  mv "${TMPFILE}" "${gv_dir}/types_swagger_doc_generated.go"
}

IFS=" " read -r -a GROUP_VERSIONS <<< "meta/v1 meta/v1beta1 ${KUBE_AVAILABLE_GROUP_VERSIONS}"

# To avoid compile errors, remove the currently existing files.
for group_version in "${GROUP_VERSIONS[@]}"; do
  rm -f "$(kube::util::group-version-to-pkg-path "${group_version}")/types_swagger_doc_generated.go"
done

# Ensure we have the latest genswaggertypedocs built
hack/make-rules/build.sh ./cmd/genswaggertypedocs

# Regenerate files.
for group_version in "${GROUP_VERSIONS[@]}"; do
  gen_types_swagger_doc "${group_version}" "$(kube::util::group-version-to-pkg-path "${group_version}")"
done