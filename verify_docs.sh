#!/bin/bash

set -e

npx embedme README.md --verify
npx embedme docs/tutorial.md --verify 
npx embedme docs/reference.md --verify 
