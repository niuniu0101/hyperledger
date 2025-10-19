#!/bin/bash

# for i in {0..59}; do mkdir -p "node$i"; done

for i in {0..59}; do rm -rf "node$i"; done
ls -d node*