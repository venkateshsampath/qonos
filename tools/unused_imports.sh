#!/bin/sh

#snakefood sfood-checker detects even more unused imports
! pyflakes qonos/ | grep "imported but unused"
