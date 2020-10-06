#I24-video-ingest
######Video ingestions pipeline management software.
######Author: William Barbour, Ph.D.; Vanderbilt University
######Version: 0.6
######Status: Development

##Usage

#####Command line usage:
pipeline_management.py [-v] [-t] -c <config-file> -r <session-root-directory> -m <resource-monitor-interval>
######Options:
- ```-v```: print version and author information, then exit
- ```-h/--help```: print usage information, then exit
- ```-t```: run startup tests, which include running an image and video snapshot
- ```-c/--config_file```: relative or absolute file path for session config file
- ```-r/--root_directory```: location in which to make the session directory where files are stored
- ```-m/--resource_monitor_interval```: number of seconds between resource monitor logging (unspecified = monitor off)

#####To allow detachment from command line:
```nohup python3 pipeline_management.py [OPTIONS] &```

#####To find PID after detaching:
```ps ax | grep test.py```

#####To kill after finding PID:
```kill PID```

