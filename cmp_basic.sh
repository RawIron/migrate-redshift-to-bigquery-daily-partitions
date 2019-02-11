bq_files=$(ls -1 rs_*)

stats=""
for f in $bq_files; do
	stats="$stats ${f#*_}"
done

for s in $stats; do
	meld rs_$s bq_$s
done
