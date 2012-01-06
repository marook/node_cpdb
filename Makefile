
all:

clean:
	find '.' -name '*~' -type f -exec rm {} \;

publish: clean
	npm publish .
