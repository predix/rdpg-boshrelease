# smoke-tests BOSH Service Job

[Official BOSH Release Documentation](http://bosh.io/docs/create-release.html)

What follows is an explanation about the pieces contained within this BOSH job
and what the development workflow is.

## Workflow

Dynamic data from BOSH properties should only be injected to `templates/config/`
files and the environment definition file located at `templates/shell/env.rb`.

Otherwise you edit the `templates/shell/functions` if you need to make any behavior
tweaks, see the description of this below.

## templates/bin/control

The script should never be modified, it's sole purpose
is to be called by the monit daemon to *start*, *stop* and *monitor* the job's
binary executable.

## templates/bin/smoke-tests

By default this file is setup to exec an binary/script stored within the package
path of the same name as the job. If you do not have a package with a binary
that you are running directly which represents the job itself then delete the
`user exec` line within the while loop and replace it with code to do what the
job needs to do. For a simple example, to run a command every minute you would
place that command followed by a second command 'sleep 60' within the while loop.

If the binary/script for the job you are running requires special handling
in order to gracefully shut down place this logic within the `graceful_stop()`
function in the `templates/shell/functions` file.

## templates/shell/env.rb

This script file is where you will set up all of the environment and prepare
directory hierarchies, etc... This file is where you assign variables from
BOSH manifest properties.

## templates/config/

This directory is optional, if your binary requires any configuration files you
should create them in here with a `.erb` extension. This allows you to use
BOSH manifest properties within the configuration file templates.

## templates/shell/functions

This script contains the start and stop functionality of the job's binary
executable. The `shell/env` will be loaded before this functions file is
loaded. Any and all customization and functionality of the starting / stopping
of the BOSH job should be done in this file.

## monit

This is a monit configuration file which specifies how to start, stop and monitor
(pidfile to watch) the job's binary executable.

## spec

This is the BOSH service job specification file, it is used to declare:

* The name of the job.
* The listing of package names required by the job.
* The mapping of files to their final destination within the runtime job path.
* A listing of :`properties:`, where each property specifies it's
  `description:` and `default:` value if any.

