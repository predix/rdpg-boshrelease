# postgres-smoke-tests

This is a placeholder README until I get an opportunity to write a more thorough one (and the frequency of changes 
slows down).

##Preconditions
The test makes the assumption that the service broker which exposes a Postgres service is running and has been
'created' in Cloud Foundry. Also, the assumption is made that service access has been enabled for 'all orgs' for
the service you wish to test.  
Additionally, a path to a JSON configuration file needs to be passed to the program's environment as CONFIG_PATH,
and as such, this config needs to be written beforehand. A sample config file exists in the assets directory, and
is used by the (eloquently named) "boshlite_noterrand_test" bash script in the bin directory as the config file to pass
to the program. Therefore, if you use that script to launch, edit that config file explicitly.  

_TODO: Explain what the fields in the config file do in at least some detail_

##Running the Tests
Running the test suite as a normal go program (i.e. not a bosh errand) _should_ be as simple as running the bash
script "boshlite_noterrand_test" found in the bin directory, given that the aforementioned preconditions have been
met.  

The other launch script "test" performs the same tasks, except it does not automatically pass in the CONFIG_PATH
environment variable, allowing the user to provide their own as either an absolute path or a relative path from
the service directory of this project.  For example:
```CONFIG_PATH="PATH_TO_DIR" bin/test```
