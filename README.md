# I24-video-ingest
###### Video ingestions pipeline management software.
###### Author: William Barbour, Ph.D.; Vanderbilt University
###### Version: 1.01 (dev)
###### Status: Development

- - -

## 1) Introduction

This video ingestion pipeline leverages GStreamer and GStreamer Daemon (developed by RidgeRun) to control video
streaming tasks on demand. The current core functionality is 1) the ability to start/stop persistent multi-file 
recording, 2) taking retroactive video snapshots on command, 3) taking still image snapshots from cameras, and 4)
logging hardware and stream performance over time.

The setup of the video ingestion pipeline is based on configuration files. The configuration file is specified in the
command line call to the pipeline and is parsed out as one of the first operations during instantiation. A sample config
file is provided in './config/sample.config'; it lays out the file structure, syntax, parameters, and options. That
information is also repeated in this file in Section 3.

Specification of the configuration file, as well as the other command line usage is discussed in Section 2. The config
file and the destination root directory for storing the video ingestion session files are the only required options.


## 2) Installation

This repository contains only pure Python code, which does not itself require installation or setup. However, it does
have additional Python dependencies, which are included in 'requirements.txt'. They can be installed using
```~/I24-video-ingest$ pip3 install requirements.txt```. *Note that installation of psutil may require an OS-level
dependency ([see here first if errors](https://github.com/giampaolo/psutil/issues/1143)).*

There are also numerous OS-level dependencies:
- GStreamer Daemon ([installation and depenency instructions](https://developer.ridgerun.com/wiki/index.php?title=GStreamer_Daemon_-_Building_GStreamer_Daemon))
- Interpipe plugin for GStreamer ([installation instructions](https://developer.ridgerun.com/wiki/index.php?title=GstInterpipe_-_Building_and_Installation_Guide))
- pygstc, Python API for GStreamer Daemon (this should install with GStreamer Daemon, but if it does not, try forcing
its install [using this method](https://developer.ridgerun.com/wiki/index.php?title=GStreamer_Daemon_-_Python_API#Getting_Started))

In the future, a list of shell commands may be provided for installation of all dependencies.


## 3) Usage

##### Command line usage:
`pipeline_management.py -c <config-file> -r <session-root-directory> [-t] [-m <resource-monitor-interval>] [-v] [-h]`
###### Options:
- `-c/--config_file`: (required for run) relative or absolute file path for session config file
- `-r/--root_directory`: (required for run) location in which to make the session directory where files are stored
- `-t`: run startup tests, which include running an image and video snapshot
- `-m/--resource_monitor_interval`: number of seconds between resource monitor logging (unspecified = monitor off)
- `-v`: print version and author information, then exit
- `-h/--help`: print usage information, then exit

##### To allow detachment from command line:
`nohup python3 pipeline_management.py [OPTIONS] &`

##### To find PID after detaching:
`ps ax | grep pipeline_management.py`

##### To kill after finding PID:
`kill PID`

## 4) Configuration files

General configuration file information is as follows. Additional parameter-specific information is below and can be 
found in the sample markdown file (./config/sample.config).

1. Blank lines and those beginning with '#' are disregarded by configuration file parser.
1. Configuration values are parsed out as key:value pairs, delineated by '==' and without spaces.
   1. Spaces are tolerated in values, but are generally not preferred.
   1. The double equals key:value separator allows single equals ('=') to be present in values such as URL.
   1. Boolean values should be given as case-insensitive 'true'/'false'.
   1. Quotation marks are not needed for keys or values.
   1. Leading/trailing spaces are stripped when parsing key:value pairs.
1. Configuration blocks are denoted by double underscore ('__') on each side of block name.
   1. Configuration blocks are: 'CAMERA', 'IMAGE-SNAPSHOT', 'VIDEO-SNAPSHOT', 'PERSISTENT-RECORDING'.
   1. Configuration values are assumed to be inside the preceding block until another block is started.
   1. Some configuration blocks can be repeated multiple time for multiple instances: CAMERA.
   1. For repeated/multiple blocks, they will be read into configuration in the listed order.

```
__CAMERA__
# name should be generic with no collisions (optional, default=camera%d)
name==camera0
# description will be written to file metadata for convenience
description==test bench camera 1
# authentication is 'user:password' format
rtsp_authentication==root:password
# address is IP:port/...
rtsp_address==192.168.0.124:554/axis-media/media.amp
# (optional) sets up logging of camera stream progress - values 'pregressreport' and 'appsink' trigger, all others don't
# note that this reporting seems to have a significant CPU impact (i.e., one full thread)
# 'progressreport' - logs a bus message generated by the progressreport element
# 'appsink' - keeps track of and logs a frame count received through appsink triggers
report==appsink
# (optional) interval (in seconds, if 'progressreport'; in frames, if 'appsink') to periodically log camera progress
report_interval==300
```
```
__IMAGE-SNAPSHOT__
# Required enable declaration (use case-insensitive 'true'/'false')
enable==true
```
```
__VIDEO-SNAPSHOT__
# Required enable declaration (use case-insensitive 'true'/'false')
enable==true
# (optional) Length of historical video buffer in seconds to be held for snapshot; default=60 seconds
#            Not to be confused with live length of snapshot
buffer_time==30
# (optional) Default duration in seconds of video snapshot for cases when duration is not given in trigger.
default_duration==20
```
```
__PERSISTENT-RECORDING__
# Required enable declaration (use case-insensitive 'true'/'false')
enable==true
# (optional) File naming template/convention for segmented recording per camera
# Filename template must contain '%d' to denote segment number, which does not roll over at max files.
# Camera name must be denoted in filename or implied directory (max once for each) using '{cam_name}' in the template
# Any directories implied in filename template will be created (including those containing camera name '{cam_name}')
# Relative directories (inside session directory) must be started with './', otherwise interpreted as absolute
# Default: './recording/record_{cam_name}_%05d.mp4'
recording_filename==./recording/record_{cam_name}_%05d.mp4
# (optional) Maximum amount of video time in minutes contained in each segment of file; default=15 minutes
segment_time==15
# (optional) Maximum number of segment files, per camera, kept in storage location (0 = no limit); default=0
maximum_segment_files==0
# (optional) Maximum estimated recording size in GB, per camera; automatically translated to number of segment files
# assumes 5.5 MB/s bitrate per camera (in parameters.py)
# this parameter overrides `maximum_segment_files`; default is not active
# maximum_camera_storage==1000
```

## 5) Future development

There are still some lingering issues related ot logging and progress reporting that would be helpful to complete.
A better way of handling interrupts from detached processes is needed. Future releases will also focus on making the 
management code interactive remotely, as well as from other code running on the same machine; this will be used to 
trigger snapshots, start/stop recording, etc.

On the horizon is the need to pipe frames directly into computer vision code, for instance across a named pipe. The
GStreamer buffer objects appear to be accessible through the appsink new-sample signal handling.
