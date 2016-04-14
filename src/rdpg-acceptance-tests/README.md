RDPG Acceptance Tests
=====================

This project contains acceptance/smoke tests for the [Reliable Distributed Postgres project](https://github.com/starkandwayne/rdpg-boshrelease) (RDPG).

Prerequisites
-------------

```
go get ./...
```

Running
-------

Create a `config.json` file and set the path to it in `$CONFIG`, then run the test suites.

```
mkdir tmp
cat > tmp/config.json <<EOS
{}
EOS
export CONFIG=$PWD/tmp/config.json
```

Then run tests:

```
./bin/test-acceptance
```

Update godeps
-------------

```
godep save ./...
```
