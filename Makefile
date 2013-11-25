MOCHA_OPTS= --check-leaks
REPORTER = spec

test: test-unit test-integration

test-unit:
	@NODE_ENV=test ./node_modules/.bin/mocha \
						--reporter $(REPORTER) \
						$(MOCHA_OPTS) \
						test/unit/**

test-integration:
	@NODE_ENV=test node test/integration/runner.js

test-load:
	@NODE_ENV=test ./node_modules/.bin/mocha \
						--reporter $(REPORTER) \
						$(MOCHA_OPTS) \
						test/load/**