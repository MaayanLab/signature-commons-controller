#!/usr/bin/env bash

INPUT=input
OUTPUT=output

(
  ls "${INPUT}" | grep -E '_(up|down)\.txt$' | while read file; do
    echo "$(basename ${file} .txt)		$(sed -z 's/\n/\t/g' ${INPUT}/${file})"
  done
) | sed 's/[[:space:]]*$//' \
  > ${OUTPUT}/example.data.gmt
