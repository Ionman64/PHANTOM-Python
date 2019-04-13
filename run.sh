#!/bin/bash

repos=("https://github.com/Ionman64/PHANTOM-Python"  "https://github.com/mochodek/py-ccflex") 

repos_dir="./repos"
output_dir="./output"

rm -rf $repos_dir
rm -rf $output_dir

mkdir $output_dir
mkdir "${output_dir}/logs"
mkdir "${output_dir}/time-series"
mkdir "${output_dir}/features"

for repo in "${repos[@]}"
do
    name=$(python -c "print('${repo}'.split('/')[-1])")
    echo $name
    python main.py --clone $repo "${repos_dir}/${name}"
    python main.py -f "${repos_dir}/${name}" "${output_dir}/logs/git-${name}.log" "${output_dir}/features/features-${name}.csv" "${output_dir}/time-series/ts-${name}.csv"
done

 