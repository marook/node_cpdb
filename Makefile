
.PHONY: all
all:

.PHONY: test
test: target/latest-dbTest.js

.PHONY: clean
clean:
	rm -rf target
	find '.' -name '*~' -type f -exec rm {} \;

.PHONY: publish
publish: clean test
	npm publish .

target:
	mkdir "$@"

.PHONY: dbTestUtils_dependencies
dbTestUtils_dependencies: test/dbTestUtils.js lib/node_cpdb.js

target/latest-dbTest.js: target test/dbTest.js dbTestUtils_dependencies
	node test/dbTest.js
	touch "$@"
