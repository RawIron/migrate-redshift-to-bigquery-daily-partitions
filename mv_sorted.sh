files=$(ls -1 *.sorted)

for f in $files; do
	mv $f ${f%.sorted}
done
