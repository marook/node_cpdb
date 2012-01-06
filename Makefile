
all:

test: target/latest-dbTest.js

clean:
	rm -rf target
	find '.' -name '*~' -type f -exec rm {} \;

publish: clean test
	npm publish .

target:
	mkdir "$@"

target/latest-dbTest.js: target test/dbTest.js lib/node_cpdb.js
	node test/dbTest.js
	touch "$@"
