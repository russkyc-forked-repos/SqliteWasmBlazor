#!/usr/bin/env python3
import sys
import re

lines = sys.stdin.readlines()
if not lines:
    sys.exit(0)

# Find all keys
k_nums = []
for line in lines:
    for match in re.finditer(r'~k\.(\d+)', line):
        k_nums.append(int(match.group(1)))
        
min_k = min(k_nums) if k_nums else -1

scores = []
for i, line in enumerate(lines):
    score = 50
    if "FileState(" in line or "FileVer(" in line:
        score = 100
    elif "splitEqs(" in line:
        score = 100
    elif "GlobalKey" in line:
        if f"~k.{min_k}" in line or "~k " in line or "~k)" in line:
            score = 10
        else:
            score = 90
    elif "~k." in line and "=" in line:
        if f"~k.{min_k}" in line:
            score = 5
        else:
            score = 15
    scores.append((score, i))

scores.sort(key=lambda x: x[0])
for _, idx in scores:
    print(idx)
