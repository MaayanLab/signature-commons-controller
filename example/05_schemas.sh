#!/usr/bin/env bash

INPUT=input
OUTPUT=output

cat "${INPUT}/example.schemas.jsonld" | jq -c '.[]' > "${OUTPUT}/example.schemas.jsonl"
