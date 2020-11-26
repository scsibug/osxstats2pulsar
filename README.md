= Overview

This is just a hack to send health information from my MacBook Pro to a local Pulsar server.

Information is collected on a configurable basis (default 30 sec), and includes:

* Networking
  * input/output packets/sec
  * input/output bytes/sec
* Backlight
  * percentage brightness
* Disk
  * read/write operations/sec
  * read/write bytes/sec
* CPU
  * CPU/integrated graphics/system agent power usage in watts
  * ratio of actual to nominal CPU frequency
  * ratio of active cores
  * average number of active cores
  * intgegrated GPU active ratio
* GPU
  * average clock frequency (if dedicated graphics running)
  * GPU busy ratio
* SMC
  * fan speed in RPM
  * CPU die temperature
  * GPU die temperature
* System info
  * hardware model identifier
  * EFI version
  * kernel OS version
  * system boot time
  * 1, 5, 15 minute load averages
* Script info
  * version of this script
  * status (starting, running, exited)
  * script startup time

Each of these categories is written to a Pulsar topic, in Avro format.

= Installation

Copy this directory into root's home (```/var/root/```).

== Python setup

Using pyenv, install Python 3.8.6, and install the ```pulsar-client``` module via pip.

Set that version as root's default using:

```
pyenv global 3.8.6
```

== Autorun with Launchd

Copy ```osxstats2pulsar.plist``` into ```/Library/LaunchDaemons```.

Configure service for automatic launching with:
```lunchctl load -w /Library/LaunchDaemons/osxstats2pulsar.plist```.

Verify the process is running with the command:
```launchctl list | grep osxstats```

Example output:

```
# launchctl list | grep osxstats
837	0	local.osxstats2pulsar
``

If the first number is non-zero, that is the PID of the currently running process.