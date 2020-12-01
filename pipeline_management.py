__author__ = "William Barbour, Ph.D.; Vanderbilt University"
__credits__ = ["Daniel Work, Ph.D.; Vanderbilt University",
               "Derek Gloudemans; Vanderbilt University",
               "RidgeRun, LLC",]
__version__ = "1.02 (dev)"
__maintainer__ = "William Barbour"
__status__ = "Development"

from parameters import *
import utilities

from pygstc.gstc import *
from pygstc.logger import *
import logbook
from logbook.queues import MultiProcessingHandler, MultiProcessingSubscriber
import psutil

import time
import datetime
from traceback import print_exc
import subprocess
import multiprocessing
from collections import OrderedDict
import os
import sys
import getopt
import signal


class PipelineEntity(object):
    """
    Abstraction class for a single pipeline that is constructed by the GStreamer Daemon client.
    """
    def __init__(self, client, name, description):
        """
        Creates the pipeline within GStreamer Daemon, but does not start it.
        :param client: GStreamer Daemon client to handle commands
        :param name: pipeline name (used for referencing within GStreamer Daemon
        :param description: pipeline description for construction
        :return: None
        """
        self._name = name
        self._description = description
        self._client = client
        print("Creating pipeline {} with description {}.".format(self._name, self._description))
        self._client.pipeline_create(self._name, self._description)
    
    def get_name(self):
        return self._name

    def play(self):
        self._client.pipeline_play(self._name)
        logbook.debug("Played pipeline: {}".format(self._name))
    
    def stop(self):
        self._client.pipeline_stop(self._name)
        logbook.debug("Stopped pipeline: {}".format(self._name))
    
    def delete(self):
        self._client.pipeline_delete(self._name)
        logbook.debug("Deleted pipeline: {}".format(self._name))
    
    def eos(self):
        self._client.event_eos(self._name)
        logbook.debug("EOS'd pipeline: {}".format(self._name))
    
    def set_property(self, element_name, property_name, property_value):
        self._client.element_set(self._name, element_name, property_name, property_value)
        logbook.debug("Set {} property to {}; element {} inside pipeline {}".format(
            property_name, property_value, element_name, self._name))

    def listen_to(self, sink):
        # interpipesrc element is named according to parameters.PIPE_SOURCE_NAME_FORMATTER
        self._client.element_set(self._name, PIPE_SOURCE_NAME_FORMATTER.format(self._name), 'listen-to', sink)
        logbook.debug("Set {} pipeline listening to {}".format(self._name, sink))


class GstdManager:
    """
    Manager class for starting and stopping GStreamer Daemon.
    """
    def __init__(self, gst_log=None, gstd_log=None, force_mkdir=False, gst_debug_level=5, 
                 tcp_enable=True, tcp_address='127.0.0.1', tcp_port=5000, num_tcp_ports=1,
                 http_enable=False, http_address='127.0.0.1', http_port=5001):
        # check input arguments
        if gst_log is not None:
            print("> GStreamer log file: {}".format(gst_log))
            logdir = os.path.split(gst_log)[0]
            if not os.path.exists(logdir):
                if force_mkdir is True:
                    print(">> Directory does not exist. Creating {}")
                    os.mkdir(logdir)
                else:
                    raise OSError("'gst_log' directory does not exist. Create or force with `force_mkdir`.")
        if gstd_log is not None:
            print("> GStreamer Daemon log file: {}".format(gstd_log))
            logdir = os.path.split(gst_log)[0]
            if not os.path.exists(logdir):
                if force_mkdir is True:
                    print(">> Directory does not exist. Creating {}")
                    os.mkdir(logdir)
                else:
                    raise OSError("'gstd_log' directory does not exist. Create or force with `force_mkdir`.")
        if type(gst_debug_level) is not int or gst_debug_level > 9 or gst_debug_level < 0:
            raise AttributeError("Provide integer [0, 9] for `gst_debug_level`.")
        # assemble arguments
        self.gstd_args = ['gstd']
        if gst_log is not None:
            self.gstd_args += ['--gst-log-filename', gst_log]
        if gstd_log is not None:
            self.gstd_args += ['--gstd-log-filename', gstd_log]
        self.gstd_args += ['--gst-debug-level', str(gst_debug_level)]
        if tcp_enable is True:
            self.gstd_args += ['--enable-tcp-protocol', '--tcp-address', tcp_address, 
                               '--tcp-base-port', str(tcp_port), '--tcp-num-ports', str(num_tcp_ports)]
        if http_enable is True:
            self.gstd_args += ['--enable-http-protocol', '--http-address', http_address, '--http-port', str(http_port)]
        print("Ready to start GStreamer Daemon.\nShell: {}".format(self.gstd_args))

    def start(self, restart=True):
        self.stop()
        print("Starting GStreamer Daemon...")
        gstd_proc = subprocess.run(self.gstd_args, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                   universal_newlines=True)
        if gstd_proc.returncode == 0:
            print("Success.")
        elif gstd_proc.returncode == 1:
            print("Error starting GStreamer Daemon with command {}".format(self.gstd_args))
            if restart is False:
                print("Gstreamer Daemon may already be running. Consider stopping or setting `restart=True`.")
    def stop(self):
        print("Attempting to kill GStreamer Daemon.")
        for i in range(3):
            try:
                gstd_stop = subprocess.run(['gstd', '--kill'], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                           universal_newlines=True, timeout=5)
                print("GStreamer Daemon stopped.")
                break
            except subprocess.TimeoutExpired:
                print("Retrying GStreamer Daemon kill.")
        else:
            print("Couldn't kill GStreamer Daemon.")
            return
        if 'no running gstd found' in gstd_stop.stderr.lower():
            print("No running GStreamer Daemon for STOP command.")


class IngestSession:
    """
    Manager class for video ingestion. This should run continuously, with triggers setting up and executing various
        components/functions. Capabilities are:
    -
    """
    def __init__(self, session_root_directory, session_config_file):
        """
        Construction a video ingest session that will handle camera streaming, recording, snapshots, etc. The session
            should be persistent and not left open without continual handling. Allowing it to detach from Python would
            require command line interaction with GStreamer Deamon (e.g., `gstd -k` to kill everything).
        :param session_root_directory: root directory at which to place session directory
        :param session_config_file: configuration file describing session elements and parameters
        :return: None
        """
        # determine parent PID
        self.pid = multiprocessing.current_process().pid
        # determine session number and root directory
        self.this_session_number = self._next_session_number(session_root_directory)
        session_relative_directory = DEFAULT_SESSION_DIRECTORY_FORMAT.format(self.this_session_number)
        self.session_absolute_directory = os.path.join(session_root_directory, session_relative_directory)
        # one last check for directory consistency
        if DEFAULT_SESSION_DIRECTORY_FORMAT.format(self.this_session_number) in os.listdir(session_root_directory):
            raise FileExistsError("""Directory overwrite conflict! 
            Check parameters.SESSION_DIRECTORY_FORMAT and self._next_session_number().""")
        os.mkdir(self.session_absolute_directory)
        self.session_log_directory = os.path.join(self.session_absolute_directory, 'logs')
        os.mkdir(self.session_log_directory)

        # set up logging that will be used by all processes
        # multiprocessing.set_start_method('spawn')
        self.logqueue = multiprocessing.Queue(-1)
        self.handler, self.sub = None, None     # initialize these to None; they'll be set in self._setup_logging()
        self._setup_logging()
        logbook.notice("Session parent PID: {}".format(self.pid))
        logbook.notice("Next session number according to session root directory: {}".format(self.this_session_number))
        logbook.notice("Session directory created: {}".format(self.session_absolute_directory))
        logbook.notice("Session logging directory created: {}".format(self.session_log_directory))
        # fill configuration variables
        # self.camera_config is a list of dictionaries; others are just a dictionary
        # camera configuration retains the order in which the cameras were listed in the config file
        logbook.notice("Parsing configuration file.")
        self.camera_config, self.image_snap_config, self.video_snap_config, self.recording_config = \
            self._parse_config_file(session_config_file)
        config_copy_file = self._copy_config_file(session_config_file)
        logbook.notice("Copying configuration file to {}".format(config_copy_file))
        # write the session header file, which includes derivative configuration information
        header_file = self._write_session_header_file()
        logbook.notice("Wrote session header/info file to {}".format(header_file))
        # instantiate GstD manager to run GStreamer Daemon in the background
        logbook.notice("Initializing GStreamer Daemon manager.")
        self.manager = None
        self.initialize_gstd()
        # instantiate the GstD Python connection client
        logbook.notice("Initializing GStreamer Daemon Python client.")
        self.client = None
        self.initialize_gstd_client()

        # locations to store pipelines {pipeline_name: PipelineEntity, ...}
        # pre-define names for certain pipelines that will be references later for control
        # camera pipelines are assumed to be named the same as the specified camera name
        self.pipelines_cameras = OrderedDict()          # use OrderedDict to preserve order during snapshots
        self.camera_progress_reporters = []             # names of cameras that send progress reporter bus messages
        self.camera_counters_to_start = []              # camera frame counter processes that need to be connected
        self.frame_count = {}                           # frame counts for each camera (if requested); key=cam_name
        self.image_encoder_name = 'image_encode'
        self.pipelines_video_enc = {}
        self.pipelines_video_buffer = {}
        self.persistent_record_name = 'record_h264'
        self.pipelines_video_rec = {}
        self.video_snap_name = 'snap_video'
        self.image_snap_name = 'snap_image'
        self.pipelines_snap = {}

        # location to store continually-running processes that need to be stopped on exit
        self.detached_processes = []

    def _setup_logging(self, file_level=logbook.DEBUG, stderr_level=logbook.NOTICE):
        """
        Sets up multiprocess logging so that parallel processes can also write to log. Currently implemented loggers are
            timed-rotating-file (turns over each day and keeps only a few files), stderr messages.
        :param file_level: logging level to record to file
        :param stderr_level: logging level to report in stderr
        :return: None
        """
        self.handler = MultiProcessingHandler(self.logqueue)
        self.handler.push_application()
        target_handlers = logbook.NestedSetup([
            logbook.NullHandler(),
            logbook.StderrHandler(level=stderr_level,
                                  format_string='{record.time:%Y-%m-%d %H:%M:%S}|{record.level_name}|{record.message}'),
            logbook.TimedRotatingFileHandler(filename=os.path.join(self.session_log_directory, 'manager.log'),
                                             level=file_level, bubble=True,
                                             date_format='%Y-%m-%d', timed_filename_for_current=True,
                                             backup_count=5, rollover_format='{basename}-{timestamp}{ext}')
        ])
        self.sub = MultiProcessingSubscriber(self.logqueue)
        self.logctl = self.sub.dispatch_in_background(target_handlers)
        logbook.notice("Logger setup complete")

    def _next_session_number(self, session_root_directory):
        """
        Checks what directories present in `session_root_directory` match the SESSION_DIRECTORY_FORMAT and calculates
            the next session number. Does not wrap around to zero.
        :param session_root_directory: Root directory where session-specific directories will be placed.
        :return: largest session number found in `session_root_directory` plus 1
        """
        root_list_dir = os.listdir(session_root_directory)
        present_matches = [0]
        for i in range(0, 99999):
            if DEFAULT_SESSION_DIRECTORY_FORMAT.format(i) in root_list_dir:
                present_matches.append(i)
        return max(present_matches) + 1

    def _parse_config_file(self, config_file):
        """
        Reads and parses configuration file into config blocks and key/value pairs. Each config return is of the form
            { key: value, key: value, ... }, where all keys and values are strings (types handled downstream).
        :param config_file:
        :return: camera_config, image_snap_config, video_snap_config, recording_config
        """
        camera_config, image_snap_config, video_snap_config, recording_config = utilities.parse_config_file(
            config_file=config_file)
        # log configs then return them
        logbook.notice("Camera configuration:", camera_config)
        logbook.notice("Image snapshot configuration:", image_snap_config)
        logbook.notice("Video snapshot configuration:", video_snap_config)
        logbook.notice("Persistent recording configuration:", recording_config)
        return camera_config, image_snap_config, video_snap_config, recording_config

    def _copy_config_file(self, config_file):
        """
        Copies the given configuration file to a new file inside the session directory, for future reference.
            The new file is called "this_session.config" and located inside self.session_absolute_directory.
        :param config_file: the original configuration file as defined by the user
        :return: absolute file path of config file copy
        """
        copy_filename = os.path.join(self.session_absolute_directory, "_SESSION_CONFIG.config")
        with open(config_file, 'r') as config_orig:
            with open(copy_filename, 'w') as config_copy:
                for line in config_orig:
                    config_copy.write(line)
        return copy_filename

    def _write_session_header_file(self):
        """
        Writes high-level information to header file in session directory.
        :return: header filename
        """
        header_filename = os.path.join(self.session_absolute_directory, DEFAULT_SESSION_INFO_FILENAME)
        with open(header_filename, 'w') as f:
            f.write("SESSION #{}".format(self.this_session_number))
            f.write("\nINFORMATIONAL/HEADER FILE")
            f.write("\n")
            f.write("-" * 50)
            # directory information
            f.write("\nDirectory (absolute): {}".format(self.session_absolute_directory))
            # time information
            timenow = datetime.datetime.now()
            f.write("\nSession initialization time (local): {}".format(timenow))
            utctimenow = datetime.datetime.utcnow()
            unix_utctimenow = (utctimenow - datetime.datetime(year=1970, month=1, day=1)).total_seconds()
            f.write("\nSession initialization time (UTC): {}".format(utctimenow))
            f.write("\nSession initialization time (UNIX): {}".format(unix_utctimenow))
            f.write("\nParent process ID: {}".format(self.pid))
            f.write("\n")
            f.write("-" * 50)
            # camera information
            f.write("\nNumber of cameras initialized: {}".format(len(self.camera_config)))
            for cc in self.camera_config:
                f.write("\n{}: {}".format(cc['name'], cc['rtsp_address']))
            f.write("-" * 50)
            f.write("\nRecording segment duration: {}".format(
                float(self.recording_config.get('segment_time', DEFAULT_RECORDING_SEGMENT_DURATION))))
            f.write("\nRecording file name format: {}".format(
                self.recording_config.get('recording_filename', DEFAULT_RECORDING_FILENAME)))
            f.write("-" * 50)
        return header_filename

    def initialize_gstd(self):
        """
        Start GStreamer Daemon process on the machine through its command line interface.
        :return: None
        """
        # TODO: pass in connection parameters
        self.manager = GstdManager(gst_log=os.path.join(self.session_log_directory, 'gst.log'),
                                   gstd_log=os.path.join(self.session_log_directory, 'gstd.log'),
                                   gst_debug_level=9, tcp_enable=True, http_enable=False)
        self.manager.start()

    def initialize_gstd_client(self, num_retry=3):
        """
        Establish connection to GStreamer Daemon running on the system. Set up to retry connection due to some random
            connection issues that seem to resolve on retry.
        :param num_retry: Number of times to retry Gstd client connection before giving up.
        :return: None
        """
        # TODO: pass in connection parameters or connection mode
        gstd_py_logger = CustomLogger(logname='ingest_log', loglevel='INFO',
                                      logfile=os.path.join(self.session_log_directory, 'pygstc.log'))
        for i in range(num_retry):
            try:
                self.client = GstdClient(ip='localhost', port=5000, logger=gstd_py_logger)
                self.client.debug_threshold(threshold='DEBUG')
                self.client.debug_enable(enable=True)
                # TODO: Gst log still not working correctly
                break
            except GstcError:
                time.sleep(1)
                if i == num_retry - 1:
                    print_exc()
                else:
                    logbook.warn("Connection failure #{}. Retry connecting to Gstd.".format(i + 1))
        else:
            logbook.critical("Problem with Gstreamer Daemon.")
            raise RuntimeError("Could not contact Gstd after {} attempts.".format(num_retry))
        self.client.debug_enable(True)

    def _bus_reader_worker(self, pipeline, bus_filters):
        """
        Perpetually reads the bus of a specific pipeline and logs messages. Meant to be run in detached process.
            Only qos+element messages are filtered through right now.
        :return: None
        """
        logbook.notice("Bus reader worker process started for pipeline {} with filters {}".format(
            pipeline, bus_filters))
        self.client.bus_filter(pipe_name=pipeline, filter=bus_filters)
        while True:
            bus_message = self.client.bus_read(pipe_name=pipeline)
            logbook.info("Bus message: {}".format(bus_message))

    def start_bus_readers(self, pipes, filters):
        """
        Starts a bus reader process for each pipeline. Adds these to the processes that need to be stopped on exit.
        :param pipes: list of pipeline names for which to start bus readers.
        :return: None
        """
        if type(pipes) not in (tuple, list) or type(filters) not in (tuple, list):
            logbook.error("Arguments `pipes` and `filters` must be iterable (i.e., tuple, list). Disregarding command.")
            return
        if len(pipes) != len(filters):
            logbook.error("Arguments `pipes` and `filters` must be the same length. Got {} and {}.".format(
                len(pipes), len(filters)))
        readers = []
        logbook.notice("Starting bus readers for pipelines: {}".format(pipes))
        for pipe, filter in zip(pipes, filters):
            br = multiprocessing.Process(target=self._bus_reader_worker, args=(pipe, filter))
            br.daemon = True
            readers.append(br)
        for reader in readers:
            reader.start()
        self.detached_processes += readers

    def _appsink_frame_counter(self, camera_name, reporting_interval):
        """
        Connects to the appsink element within a camera stream pipeline and counts frames that are signaled.
            Reporting is triggered by frame counts, not strictly by time. So if frames aren't streaming, no reports.
        :param camera_name: name of the camera/pipeline to connect
        :param reporting_interval: number of seconds between log reports - translated to frame count assuming 30fps
        """
        while True:
            self.client.signal_connect(pipe_name=camera_name, element='{}_appsink'.format(camera_name),
                                       signal='new-sample')
            self.frame_count[camera_name] += 1
            # assume 30 frames per second
            if self.frame_count[camera_name] % reporting_interval == 0:
                logbook.info("FRAMES: Camera {} frame count = {}".format(camera_name, self.frame_count))

    def check_validity_recording_file_name_formatter(self):
        # check the validity of the recording directory and filename
        file_location = self.recording_config.get('recording_filename', DEFAULT_RECORDING_FILENAME)
        # split path location into directory and filename
        file_dir, file_name = os.path.split(file_location)
        if file_dir.startswith('./'):
            file_dir = os.path.join(self.session_absolute_directory, file_dir[2:])
        else:
            logbook.warning("Absolute directory implied for persistent recording location.")
        # check that the file number formatter is present
        if '%d' not in file_name and not any(['%0{}d'.format(i) in file_name for i in range(10)]):
            logbook.critical("Problem with recording configuration.")
            raise AttributeError("Need to include '%d' or '%0Nd' (N:0-9) in  recording filename template.")
        # check if session number in filename format
        if '{session_num}' in file_dir:
            logbook.info("Recording file name formatter will have session number in directory.")
        if '{session_num}' in file_name:
            logbook.info("Recording file name formatter will have session number in each file name.")
        # check if we need to create camera-specific directories, or just one directory
        if '{cam_name}' in file_dir:
            logbook.info("Recording file name formatter will have camera name in directories.")
        elif '{cam_name}' in file_name:
            logbook.info("Recording file name formatter will have camera name in each file name.")
        else:
            # didn't find a camera name placeholder in either the file_dir or the file_name
            logbook.critical("Problem with recording configuration.")
            raise AttributeError("Need to camera name placeholder ('{cam_name}') in recording filename template.")

    def get_recording_file_name_formatters(self):
        """
        Takes the recording filename formatter string in the recording configuration section and formats it with the
            camera names (required) and the session number (optional) for directories and filenames.
        :return: list of tuples of (directory, filename_formatter); e.g., (/data/session_5/recording, s5_cam0_%d.mp4)
        """
        directory_file_formatters = []
        # get the recording filename formatter from the config
        file_location = self.recording_config.get('recording_filename', DEFAULT_RECORDING_FILENAME)
        unformat_dir, unformat_file = os.path.split(file_location)
        # check if this is a relative or absolute file path
        if unformat_dir.startswith('./'):
            unformat_dir = os.path.join(self.session_absolute_directory, unformat_dir[2:])
        # check if session number needs to be formatted into either directory or file
        if '{session_num}' in unformat_dir:
            unformat_dir = unformat_dir.format(session_num=self.this_session_number)
        if '{session_num}' in unformat_file:
            unformat_file = unformat_file.format(session_num=self.this_session_number)
        # now put the camera names into the directories and files
        for cam_name in self.pipelines_cameras.keys():
            if '{cam_name}' in unformat_dir:
                fd = unformat_dir.format(cam_name=cam_name)
            else:
                fd = unformat_dir
            if '{cam_name}' in unformat_file:
                ff = unformat_file.format(cam_name=cam_name)
            else:
                ff = unformat_file
            directory_file_formatters.append((cam_name, fd, ff))
        # return the list of formatted (directory, file) tuples; file formatter still has %d indicator in it
        return directory_file_formatters

    def get_recording_file_stats(self):
        """
        Walk the directory(s) where the persistent recording is taking place and report the number of files and size.
        :return: number of total files, total size of all files
        """
        directories_files = self.get_recording_file_name_formatters()
        size = 0
        num = 0
        for cam_name, rdir, rfile in directories_files:
            if os.path.exists(rdir):
                for filename in os.listdir(rdir):
                    size += os.path.getsize(os.path.join(rdir, filename))
                    num += 1
        return num, size

    def get_current_resource_stats(self, get_cpu, get_memory, get_network, get_disk):
        """
        Fetches current hardware resource statistics.
        :param get_cpu: T/F fetch CPU stats (1-, 5-, 15- min CPU % avg, (cpu_1, _2, _3, ..., _N current %) )
        :param get_memory: T/F fetch memory stats (available memory, total memory)
        :param get_network: T/F fetch network stats (total bytes sent, total bytes received)
        :param get_disk: T/F fetch disk stats (used, free, total bytes) for disk where session directory is located
        :return: cpu, memory, network, disk stats
        """
        stat_cpu, stat_mem, stat_net, stat_dsk = None, None, None, None
        if get_cpu is True:
            try:
                n_cpu = psutil.cpu_count()
                per_cpu = psutil.cpu_percent(interval=0.5, percpu=True)
                stat_cpu = tuple([ld / n_cpu * 100 for ld in psutil.getloadavg()]) + (tuple(per_cpu),)
            except:
                logbook.warning("Problem with CPU resource fetch.")
        if get_memory is True:
            try:
                mem_vals = psutil.virtual_memory()
                stat_mem = (mem_vals.available, mem_vals.total)
            except:
                logbook.warning("Problem with memory resource fetch.")
        if get_network is True:
            try:
                net_vals = psutil.net_io_counters(pernic=False, nowrap=True)
                stat_net = (net_vals.bytes_sent, net_vals.bytes_recv)
            except:
                logbook.warning("Problem with network resource fetch.")
        if get_disk is True:
            try:
                dsk_vals = psutil.disk_usage(path=self.session_absolute_directory)
                stat_dsk = (dsk_vals.used, dsk_vals.free, dsk_vals.total)
            except:
                logbook.warning("Problem with disk resource fetch.")
        # TODO: future -- implement temperature sensor measurements
        return stat_cpu, stat_mem, stat_net, stat_dsk

    def _resource_monitor_worker(self, log_interval, get_cpu, get_memory, get_network, get_disk, get_recording_dir):
        """
        Periodically fetches and logs system resource stats.
        :param log_interval: number of seconds between subsequent resource fetches
        :param get_cpu: T/F fetch CPU stats (1-, 5-, 15- min CPU % avg, (cpu_1, _2, _3, ..., _N current %) )
        :param get_memory: T/F fetch memory stats (available memory, total memory)
        :param get_network: T/F fetch network stats (total bytes sent, total bytes received)
        :param get_disk: T/F fetch disk stats (used, free, total bytes) for disk where session directory is located
        :param get_recording_dir: T/F fetch file stats for recording directory (num total files, total bytes)
        :return: None
        """
        logbook.notice("Resource monitor started successfully.")
        while True:
            cpu, mem, net, dsk = self.get_current_resource_stats(get_cpu=get_cpu, get_memory=get_memory,
                                                                 get_network=get_network, get_disk=get_disk)
            # TODO: logging channel args here don't work (propagate into records)
            if get_cpu is True:
                logbook.info("CPU: {}".format(cpu), channel='Resources')
            if get_memory is True:
                logbook.info("MEMORY: {}".format(mem), channel='Resources')
            if get_network is True:
                logbook.info("NETWORK: {}".format(net), channel='Resources')
            if get_disk is True:
                logbook.info("DISK: {}".format(dsk), channel='Resources')
            if get_recording_dir is True:
                rec = self.get_recording_file_stats()
                logbook.info("RECORDING: {}".format(rec), channel='Resources')
            time.sleep(log_interval)

    def start_resource_monitor(self, log_interval=30, get_cpu=True, get_memory=True, get_network=True, get_disk=True,
                               get_recording_dir=True):
        """
        Starts a separate (detached) process to periodically fetch and log system resource stats.
        :param log_interval: number of seconds between subsequent resource fetches
        :param get_cpu: T/F fetch CPU stats (1-, 5-, 15- min CPU % avg, (cpu_1, _2, _3, ..., _N current %) )
        :param get_memory: T/F fetch memory stats (available memory, total memory)
        :param get_network: T/F fetch network stats (total bytes sent, total bytes received)
        :param get_disk: T/F fetch disk stats (used, free, total bytes) for disk where session directory is located
        :param get_recording_dir: T/F fetch file stats for recording directory (num total files, total bytes)
        :return: None
        """
        if log_interval < 5:
            logbook.error("Invalid value for `log_interval`. Must be >= 5 seconds. Disregarding command.")
            return
        # run this once for stats that need to be called once before they're meaningful (may or may not be applicable)
        self.get_current_resource_stats(get_cpu=get_cpu, get_memory=get_memory,
                                        get_network=get_network, get_disk=get_disk)
        monitor = multiprocessing.Process(target=self._resource_monitor_worker,
                                          args=(log_interval, get_cpu, get_memory, get_network,
                                                get_disk, get_recording_dir))
        monitor.daemon = True
        logbook.notice("Starting resource monitor process.")
        monitor.start()
        self.detached_processes.append(monitor)
        return None

    def _construct_camera_pipelines(self):
        """
        # ----------------------------------------------------------------------------------------------------------
        # Each camera pipeline is independent, and constructed as follows.
        #
        #  rtspsrc --> rtph264depay --> h264parse --> progressreport (optional) --> queue --> interpipesink
        #
        # ----------------------------------------------------------------------------------------------------------
        """
        for single_camera_config in self.camera_config:
            cam_name = single_camera_config['name']
            if cam_name in self.pipelines_cameras:
                logbook.critical("Problem with camera configuration.")
                raise AttributeError("Camera name collision. Check configuration file.")
            # determine connection method and assemble URI
            if 'rtsp_authentication' in single_camera_config and 'rtsp_address' in single_camera_config:
                cam_connect = 'rtsp://{}@{}'.format(single_camera_config['rtsp_authentication'],
                                                    single_camera_config['rtsp_address'])
                cam_source = 'rtspsrc location={}'.format(cam_connect)
            else:
                # only RTSP implemented right now
                logbook.critical("Problem with camera configuration.")
                raise AttributeError("Need rtsp_authentication and rtsp_address in each camera config block.")
            logbook.info("Source for camera={}: {}".format(cam_name, cam_source))
            cam_sink = 'interpipesink name={} forward-events=true forward-eos=true sync=false'.format(
                PIPE_SINK_NAME_FORMATTER.format(cam_name))
            # default no reporting; this will get overwritten if reporting is requested
            pd = '{} ! rtph264depay ! h264parse ! queue ! {}'.format(cam_source, cam_sink)
            # check if reporting was requested
            if 'report' in single_camera_config and single_camera_config['report'] in ('progressreport', 'appsink'):
                interval = int(single_camera_config.get('report_interval', DEFAULT_CAMERA_REPORTING_INTERVAL))
                if single_camera_config['report'] == 'progressreport':
                    # setting do-query=false so reporting uses metadata
                    # silent=true so not stdout
                    report_element = 'progressreport update-freq={} do-query=false silent=true'.format(interval)
                    self.camera_progress_reporters.append(cam_name)
                else:
                    # report method was 'appsink'
                    # TODO: one of these settings seems to be creating a memory leak
                    # TODO: try identity element for frame counting instead?
                    appsink_element = 'appsink name={} max-buffers=100 wait-on-eos=false emit-signals=true drop=true'.format(
                        '{}_appsink'.format(cam_name))
                    report_element = 'tee name=t t. ! queue ! {} t.'.format(appsink_element)
                    self.camera_counters_to_start.append((cam_name, interval))
                pd = '{} ! rtph264depay ! h264parse ! {} ! queue ! {}'.format(cam_source, report_element, cam_sink)
                logbook.info("Progress logging for camera={} every {} seconds".format(cam_name, interval))
            else:
                logbook.info("No progress logging for camera={}.".format(cam_name))
            cam = PipelineEntity(self.client, cam_name, pd)
            self.pipelines_cameras[cam_name] = cam
            # initialize frame counter for this camera, even if there's no reporting
            self.frame_count[cam_name] = 0

    def _construct_persistent_recording_pipeline(self):
        """
        # ----------------------------------------------------------------------------------------------------------
        # Persistent recording pipeline is a single pipeline definition, but contains multiple sub-pipelines
        #   - this way all the sub-pipelines start at the same time with one command
        # ----------------------------------------------------------------------------------------------------------
        #
        # interpipesrc (cam0) --> splitmuxsink
        #
        # interpipesrc (cam1) --> splitmuxsink
        #
        #    |     |     |
        #    V     V     V
        #
        #
        # ----------------------------------------------------------------------------------------------------------
        """
        # construct the recording pipeline for all the cameras by adding them all to the same description
        pd = ''
        for cam_name in self.pipelines_cameras.keys():
            # listen to camera sink; no need to name this interpipesrc because it won't be changed
            pd += ' interpipesrc format=time allow-renegotiation=false listen-to={} ! '.format(
                PIPE_SINK_NAME_FORMATTER.format(cam_name))
            # name camera-specific filesink with pipeline name and camera name
            pd += 'splitmuxsink name={} async-finalize=true muxer-pad-map=x-pad-map,video=video_0'.format(
                PIPE_CAMERA_FILESINK_NAME_FORMATTER.format(self.persistent_record_name, cam_name))
        record_h264 = PipelineEntity(self.client, self.persistent_record_name, pd)
        # check that the recording file name formatter is valid
        self.check_validity_recording_file_name_formatter()
        # get the directories and filename formatters for recording
        directory_file_formatters = self.get_recording_file_name_formatters()
        # narrow down to set of directories
        create_directories = set([fdr for cn, fdr, ffl in directory_file_formatters])
        # create the necessary directories if they don't exist
        try:
            for create_dir in create_directories:
                if not os.path.exists(create_dir):
                    logbook.notice("Making directory for persistent recording: {}".format(create_dir))
                    os.mkdir(create_dir)
        except OSError as e:
            # FATAL EXCEPTION
            logbook.critical("Problem creating persistent recording directories. This is a fatal exception.")
            print_exc()
            raise e
        # maximum segment time for multi-segment recording; number of minutes * 60 s/min * 1e9 ns/s
        max_file_time_mins = float(self.recording_config.get('segment_time', DEFAULT_RECORDING_SEGMENT_DURATION))
        max_file_time_ns = int(max_file_time_mins * 60 * 1e9)
        # maximum number of files, per camera, that are kept in storage
        max_num_files = int(self.recording_config.get('maximum_segment_files', DEFAULT_NUMBER_STORED_SEGMENTS))
        # if maximum storage space specified, convert and replace max_num_files (automatic override)
        if 'maximum_camera_storage' in self.recording_config:
            max_storage_mb = float(self.recording_config['maximum_camera_storage']) * 1024
            # convert storage space to recording time
            max_storage_mins = max_storage_mb / ESTIMATED_CAMERA_BITRATE / 60
            # convert to number of files and truncate decimal
            max_num_files = int(max_storage_mins / max_file_time_mins)
            print("Maximum number of files set from maximum storage config value: {}".format(max_num_files))
        else:
            print("Maximum number of files set directly from config value: {}".format(max_num_files))
        # set filesink (splitmuxsink element) properties for location and file management
        for cam_name, file_dir, file_name in directory_file_formatters:
            cam_full_location = os.path.join(file_dir, file_name)
            print("Setting file path for camera {} to {}".format(cam_name, cam_full_location))
            record_h264.set_property(PIPE_CAMERA_FILESINK_NAME_FORMATTER.format(self.persistent_record_name, cam_name),
                                     'location', cam_full_location)
            record_h264.set_property(PIPE_CAMERA_FILESINK_NAME_FORMATTER.format(self.persistent_record_name, cam_name),
                                     'max-size-time', str(max_file_time_ns))
            record_h264.set_property(PIPE_CAMERA_FILESINK_NAME_FORMATTER.format(self.persistent_record_name, cam_name),
                                     'max-files', str(max_num_files))
        self.pipelines_video_rec[self.persistent_record_name] = record_h264

    def _construct_buffered_video_snapshot_pipeline(self):
        """
        # ----------------------------------------------------------------------------------------------------------
        # Video buffers are independent pipelines, each constructed as follows.
        #
        #  interpipesrc (camaera) --> queue (FIFO config) --> interpipesink
        #
        # ----------------------------------------------------------------------------------------------------------
        # Video snapshot pipeline records from all buffers, and is constructed as follows.
        #
        #                                    mp4mux
        #                                   __________
        #                                  /          \
        #  interpipesrc (buffer-cam0) --> | video_0    |
        #                                 |            |
        #  interpipesrc (buffer-cam1) --> | video_1    |
        #        |   |   |   |            |            |  --> filesink
        #        V   V   V   V            |            |
        #  interpipesrc (buffer-camN) --> | video_N    |
        #                                  \          /
        #                                   ----------
        # ----------------------------------------------------------------------------------------------------------
        """
        # Length of historical video buffer; number of seconds * 1e9 ns/s
        min_buffer_time = float(self.video_snap_config.get('buffer_time', DEFAULT_BUFFER_TIME)) * 1e9
        # Set max time at 105% min time
        overflow_time = min_buffer_time * 1.05
        # Set buffer memory overflow for safety; assume 2x camera bitrate (in parameters.py) * 1024^2 B/MB
        overflow_size = overflow_time * 2 * ESTIMATED_CAMERA_BITRATE * 1024 * 1024

        buffer_name_format = 'buffer_h264_{}'       # not going to need to access these later
        for cam_name, _ in self.pipelines_cameras.items():
            buffer_name = buffer_name_format.format(cam_name)
            buffer_source = 'interpipesrc name={} format=time listen-to={}'.format(
                PIPE_SOURCE_NAME_FORMATTER.format(buffer_name), PIPE_SINK_NAME_FORMATTER.format(cam_name))
            # Partial queue definition; other parameters set after pipeline creation.
            qname = 'fifo_queue_{}'.format(cam_name)
            # Set leaky=2 for FIFO; silent=true for no events; disable number of buffers limit.
            queue_def = 'queue name={} max-size-buffers=0 leaky=2 silent=true flush-on-eos=false'.format(qname)
            buffer_sink = 'interpipesink name={} forward-events=true forward-eos=true sync=false'.format(
                PIPE_SINK_NAME_FORMATTER.format(buffer_name))
            buffer_def = '{} ! {} ! {}'.format(buffer_source, queue_def, buffer_sink)
            new_buffer = PipelineEntity(self.client, buffer_name, buffer_def)
            # set buffer properties; first convert values to integers then strings
            new_buffer.set_property(qname, 'min-threshold-time', str(int(min_buffer_time)))
            new_buffer.set_property(qname, 'max-size-time', str(int(overflow_time)))
            new_buffer.set_property(qname, 'max-size-bytes', str(int(overflow_size)))
            self.pipelines_video_buffer[buffer_name] = new_buffer

        # Video snapshot - connects to queue-buffers from each camera, muxes, and file-sinks
        # ----------------------------------------------------------------------------------------------------------
        logbook.notice("CREATING VIDEO SNAPSHOT PIPELINE")
        pd = ''
        for ci, cam_name in enumerate(self.pipelines_cameras.keys()):
            this_buffer_name = buffer_name_format.format(cam_name)
            pd += ' interpipesrc format=time allow-renegotiation=false listen-to={} ! '.format(
                PIPE_SINK_NAME_FORMATTER.format(this_buffer_name))
            pd += 'snapmux.video_{}'.format(ci)
        # file location will be set later when the snapshot is triggered
        pd += ' mp4mux name=snapmux ! filesink name={}'.format(
            PIPE_SINGLE_FILESINK_NAME_FORMATTER.format(self.video_snap_name))
        snap_video = PipelineEntity(self.client, self.video_snap_name, pd)
        self.pipelines_snap[self.video_snap_name] = snap_video

    def _construct_image_snapshot_pipeline(self):
        """
        # ----------------------------------------------------------------------------------------------------------
        # H.264 to still image transcoder is camera-generic and constructed as follows.
        #
        #  interpipesrc --> avdec_h264 --> jpegenc --> interpipesink
        #
        # ----------------------------------------------------------------------------------------------------------
        # Image snapshot pipeline listens to still image encoder and is constructed as follows.
        #
        #  interpipesrc --> filesink
        #
        # ----------------------------------------------------------------------------------------------------------
        """
        # source 'listen-to' parameter set at an arbitrary camera for now; changed during snapshot
        encoder_source = 'interpipesrc name={} format=time listen-to={}_sink'.format(
            PIPE_SOURCE_NAME_FORMATTER.format(self.image_encoder_name), list(self.pipelines_cameras.keys())[0])
        # not using jpegenc snapshot parameter (sends EOS after encoding a frame) because of H.264 key frames
        encoder_type = 'jpegenc quality=95'
        encoder_sink = 'interpipesink name={} '.format(PIPE_SINK_NAME_FORMATTER.format(self.image_encoder_name))
        encoder_sink += 'forward-events=true forward-eos=true sync=false async=false enable-last-sample=false drop=true'
        encoder_def = '{} ! avdec_h264 ! {} ! {}'.format(encoder_source, encoder_type, encoder_sink)
        image_encoder = PipelineEntity(self.client, self.image_encoder_name, encoder_def)
        self.pipelines_video_enc[self.image_encoder_name] = image_encoder

        # image snapshot - connects to only one camera at a time via image_encode pipeline and dumps a frame to file
        # ----------------------------------------------------------------------------------------------------------
        logbook.notice("CREATING IMAGE SNAPSHOT PIPELINE")
        snap_source = 'interpipesrc name={} format=time listen-to={} num-buffers=1'.format(
            PIPE_SOURCE_NAME_FORMATTER.format(self.image_snap_name),
            PIPE_SINK_NAME_FORMATTER.format(self.image_encoder_name))
        # file location will be set later when the snapshot is triggered
        snap_sink = 'filesink name={}'.format(PIPE_SINGLE_FILESINK_NAME_FORMATTER.format(self.image_snap_name))
        snap_image = PipelineEntity(self.client, self.image_snap_name, '{} ! {}'.format(snap_source, snap_sink))
        self.pipelines_snap[self.image_snap_name] = snap_image

    def construct_pipelines(self):
        """
        Construct all pipelines based on the configuration variables for this session.
        :return: None
        """
        # TODO: need to think about interpipe parameters more (i.e., forwarding EOS, sync, etc.)
        try:
            # Create camera pipelines
            # ----------------------------------------------------------------------------------------------------------
            logbook.notice("CREATING CAMERA PIPELINES")
            self._construct_camera_pipelines()

            # H.264 recording via MPEG4 container mux to parallel streams
            # ----------------------------------------------------------------------------------------------------------
            if len(self.recording_config) > 0 and self.recording_config.get('enable', 'false').lower() == 'true':
                logbook.notice("CREATING PERSISTENT RECORDING PIPELINES")
                self._construct_persistent_recording_pipeline()

            # Camera FIFO historical video buffers for video snapshot capability
            # ----------------------------------------------------------------------------------------------------------
            if len(self.video_snap_config) > 0 and self.video_snap_config.get('enable', 'false').lower() == 'true':
                logbook.notice("CREATING BUFFER PIPELINES")
                self._construct_buffered_video_snapshot_pipeline()

            # H.264 to still image transcoder for image snapshot capability
            # ----------------------------------------------------------------------------------------------------------
            if len(self.image_snap_config) > 0 and self.image_snap_config.get('enable', 'false').lower() == 'true':
                logbook.notice("CREATING STILL IMAGE ENCODER PIPELINE")
                self._construct_image_snapshot_pipeline()

        except (GstcError, GstdError) as e:
            logbook.critical("Failure during pipeline construction.")
            print_exc()
            self.deconstruct_all_pipelines()
            self.stop_all_processes()
            self.kill_gstd()
            raise e

    def start_cameras(self):
        """
        Start each camera stream pipeline. Raises error if unsuccessful. Also starts progress reporter bus readers.
        :return: None
        """
        try:
            logbook.notice("Starting camera streams.")
            for pipeline_name, pipeline in self.pipelines_cameras.items():
                logbook.notice("Starting {}.".format(pipeline_name))
                self.client.pipeline_verbose(pipe_name=pipeline_name, value=True)
                pipeline.play()
            time.sleep(5)
            logbook.notice("Camera streams initialized.")
            # start bus readers for rtspsrc elements
            if False:
                for pipeline_name in self.pipelines_cameras.keys():
                    self.client.pipeline_verbose(pipe_name=pipeline_name, value=True)
                self.start_bus_readers(pipes=list(self.pipelines_cameras.keys()),
                                       filters=['element' for _ in self.pipelines_cameras])
            # check if there are any progress reporter bus readers to connect
            if len(self.camera_progress_reporters) > 0:
                self.start_bus_readers(pipes=self.camera_progress_reporters,
                                       filters=['element' for _ in self.camera_progress_reporters])
                logbook.notice("Automatically started bus readers for progress reports on cameras {}.".format(
                    self.camera_progress_reporters))
            # check if there are any appsink frame counters to start
            if len(self.camera_counters_to_start) > 0:
                for cam_name, interval in self.camera_counters_to_start:
                    fc = multiprocessing.Process(target=self._appsink_frame_counter, args=(cam_name, interval))
                    fc.daemon = True
                    fc.start()
                    self.detached_processes.append(fc)
                self.camera_counters_to_start = []
        except (GstcError, GstdError) as e:
            logbook.critical("Could not initialize camera streams.")
            print_exc()
            self.stop_all_pipelines()
            self.deconstruct_all_pipelines()
            self.stop_all_processes()
            self.kill_gstd()
            raise e

    def start_buffers(self):
        """
        Start the video buffer pipelines so they start holding backlog. Does nothing if buffer pipelines aren't
            constructed during pipeline construction.
        :return: None
        """
        if self.video_snap_config.get('enable', 'false').lower() == 'false':
            logbook.notice("Buffer start called but no buffer pipelines constructed.")
            return
        try:
            logbook.notice("Starting camera stream buffers for video snapshot.")
            for pipeline_name, pipeline in self.pipelines_video_buffer.items():
                logbook.notice("Starting {}.".format(pipeline_name))
                pipeline.play()
            time.sleep(1)
            logbook.notice("Camera stream buffers initialized.")
            logbook.notice("Buffers will reach capacity in {} seconds.".format(
                self.video_snap_config.get('buffer_time', DEFAULT_BUFFER_TIME)))
        except (GstcError, GstdError) as e:
            logbook.error("Could not initialize camera stream buffers.")
            print_exc()

    def start_persistent_recording_all_cameras(self):
        """
        Sets the persistent recording filename from the configuration file and starts the recording.
        :return: recording locations (with numbering formatter %0Nd) if successful; otherwise None
        """
        # get the location for the recordings
        # already checked for camera formatter and file number formatter in config parser
        # also already made the necessary directories
        # not allowed to change this persistent recording location for consistency across start/stops
        recording_directories_files = self.get_recording_file_name_formatters()
        fns = [os.path.join(fdr, ffn) for cam_name, fdr, ffn in recording_directories_files]
        # start the whole recording pipeline
        logbook.notice("Starting recording.")
        try:
            self.pipelines_video_rec[self.persistent_record_name].play()
            time.sleep(5)
            logbook.notice("Persistent recording pipeline playing.")
        except (GstcError, GstdError):
            logbook.error("Couldn't play persistent recording pipeline.")
            print_exc()
            return None
        return fns

    def stop_persistent_recording_all_cameras(self):
        """
        Send EOS to recording pipeline, then stop it.
        :return: None
        """
        try:
            logbook.notice("Sending EOS to persistent recording pipeline.")
            self.pipelines_video_rec[self.persistent_record_name].eos()
            logbook.notice("Waiting for recording to wrap up after EOS.")
            time.sleep(5)
            logbook.notice("Stopping persistent recording pipeline.")
            self.pipelines_video_rec[self.persistent_record_name].stop()
        except (GstcError, GstdError) as e:
            logbook.error("Problem with stopping persistent recording.")
            print_exc()

    def _image_snapshot_worker(self, camera_list, snap_abs_dir, snap_fn):
        """
        Executes the image snapshot given the final camera list and file location information.
            Meant to run in non-blocking mp.Process.
        :param camera_list: list of camera names to snapshot (list of strings assembled in calling function)
        :param snap_abs_dir: absolute directory for snapshot storage (optional '{xyz}' formatters)
        :param snap_fn: snapshot file name (optional '{xyz}' formatters)
        :return: list of successful image snapshot filenames, if any (list can be empty)
        """
        # get the image snap and image encode pipelines
        snapimg_pipeline = self.pipelines_snap[self.image_snap_name]
        encode_img_pipeline = self.pipelines_video_enc[self.image_encoder_name]
        fns = []
        # run each camera independently
        # first check the directory existence and create if necessary
        for camera_name in camera_list:
            try:
                # add in camera name to directory if needed
                snap_abs_fmt_dir = snap_abs_dir.format(
                    cam_name=camera_name, datetime_local=datetime.datetime.isoformat(datetime.datetime.now()),
                    datetime_utc=datetime.datetime.isoformat(datetime.datetime.utcnow()),
                    datetime_unix=str(time.time())[:-3])
                # make the directory if it doesn't exist
                if not os.path.exists(snap_abs_fmt_dir):
                    logbook.notice("Making directory: {}".format(snap_abs_fmt_dir))
                    os.mkdir(snap_abs_fmt_dir)
                # join the formatted absolute directory and the formatted (if applicable) filename
                snap_abs_fmt_fn = os.path.join(snap_abs_fmt_dir, snap_fn.format(
                    cam_name=camera_name, datetime_local=datetime.datetime.isoformat(datetime.datetime.now()),
                    datetime_utc=datetime.datetime.isoformat(datetime.datetime.utcnow()),
                    datetime_unix=str(time.time())[:-3]))
                # set the location of the filesink in the image snap pipeline
                logbook.info("Setting location of {} pipeline filesink to {}.".format(self.image_snap_name,
                                                                                      snap_abs_fmt_fn))
                snapimg_pipeline.set_property(PIPE_SINGLE_FILESINK_NAME_FORMATTER.format(self.image_snap_name),
                                              'location', snap_abs_fmt_fn)
            except (OSError, GstcError, GstdError):
                logbook.error("Problem setting up directory and setting filesink location.")
                print_exc()
                continue
            try:
                # set the encoding pipeline to listen to the appropriate camera
                logbook.info("Setting encoding interpipe to listen to {}.".format(camera_name))
                encode_img_pipeline.listen_to(PIPE_SINK_NAME_FORMATTER.format(camera_name))
                # play the image encoder pipeline and let it spin up (needs a key frame for proper H.264 decoding)
                logbook.info("Playing image encoder.")
                encode_img_pipeline.play()
                time.sleep(IMAGE_ENCODE_SPIN_UP)
                # run the snap image pipeline for a bit, not sure if this time matters much
                snapimg_pipeline.play()
                time.sleep(IMAGE_SNAP_EXECUTE_TIME)
                # TODO: need EOS for encode or snap pipeline? Memory impact?
                # TODO: something is going on with bad encoding in images
                snapimg_pipeline.eos()
                snapimg_pipeline.stop()
                encode_img_pipeline.eos()
                encode_img_pipeline.stop()
                fns.append(snap_abs_fmt_fn)
            except (GstcError, GstdError):
                logbook.error("Problem with encoding/snapshot pipeline for camera {}.".format(camera_name))
                print_exc()
                continue
        logbook.notice("Image snapshot worker process complete.")
        logbook.notice("Snapshots: {}".format(fns))
        return fns

    def take_image_snapshot(self, file_relative_location=None, file_absolute_location=None, cameras='all', join=False):
        """
        Takes a still image snapshot of each camera specified. They are taken one at a time in order to avoid spinning
            up numerous H.264->still transcoding pipelines. Failure of one snapshot will not prevent the others.
        :param file_relative_location: location inside session directory to store snapshots; if more than one camera
            is specified, then '{cam_name}' placeholder must be in directory or filename portion for camera name; other
            valid placeholders are '{datetime_local}' = ISO format local datetime, '{datetime_utc}' = ISO format UTC
            datetime, and '{datetime_unix}' = UNIX timestamp
        :param file_absolute_location: same as relative location, but setting this != None automatically overrides it;
            see `file_relative_location` for valid placeholder descriptions
        :param cameras: cameras to snapshot; 'all'=all cameras; list/tuple of camera names; ','-sep. str of camera names
        :param join: T/F wait for snapshot to complete (i.e., call multiprocessing.Process.join())
        :return: None
        """
        # check if video snapshot pipeline was constructed
        if self.image_snap_name not in self.pipelines_snap or self.image_encoder_name not in self.pipelines_video_enc:
            logbook.error("Image snapshot pipeline or encoder pipeline wasn't constructed. Ignoring command.")
            return None
        # extract the camera list from the given parameter
        if cameras == 'all':
            camlist = list(self.pipelines_cameras.keys())
        elif type(cameras) in (list, tuple):
            camlist = cameras
        elif type(cameras) is str:
            # if it's just a single camera we'll still get a list
            camlist = cameras.split(',')
        else:
            logbook.error("""Got an invalid type for argument `cameras`. 
            Need list/tuple of str or comma-delineated str or 'all'.""")
            return None
        # check that all of the camera names are correct
        if not all([cam in self.pipelines_cameras for cam in camlist]):
            logbook.error("""One or more of the cameras specified for snapshot is not valid. 
            Available: {}. Specified: {}. Ignoring command.""".format(self.pipelines_cameras.keys(), cameras))
            return None
        logbook.notice("Snapping cameras: {}".format(camlist))
        # set the directory and filename from the absolute or relative parameters given
        if file_absolute_location is None:
            if file_relative_location is None:
                snap_dir, snap_fn = os.path.split(DEFAULT_IMAGE_SNAPSHOT_FILENAME)
            else:
                snap_dir, snap_fn = os.path.split(file_relative_location)
            # check for './' at the start of the filename and compensate
            if snap_dir.startswith('./'):
                snap_abs_dir = os.path.join(self.session_absolute_directory, snap_dir[2:])
            elif snap_dir.startswith('/'):
                # TODO: this isn't right - '/...' implies absolute path
                snap_abs_dir = os.path.join(self.session_absolute_directory, snap_dir[1:])
            else:
                snap_abs_dir = os.path.join(self.session_absolute_directory, snap_dir)
        else:
            snap_abs_dir, snap_fn = os.path.split(file_absolute_location)
        logbook.info("Final video snap directory: {}".format(snap_abs_dir))
        # check if there are multiple cameras and make sure the placeholder is included
        if len(camlist) > 1 and ('{cam_name}' not in snap_fn and '{cam_name}' not in snap_abs_dir):
            logbook.error(">1 camera requested for image snap, but '{cam_name}' not in filename. Ignoring command.")
            return None

        imgsnap = multiprocessing.Process(target=self._image_snapshot_worker,
                                          args=(camlist, snap_abs_dir, snap_fn))
        imgsnap.daemon = True
        logbook.notice("Starting image snapshot worker process.")
        try:
            imgsnap.start()
            if join is True:
                logbook.notice("Process started, waiting for completion (join=True).")
                imgsnap.join()
            else:
                logbook.notice("Process started, exiting blocking function.")
        except multiprocessing.ProcessError:
            logbook.error("Problem starting image snap worker process.")
            print_exc()
        return None

    def _video_snapshot_worker(self, duration, snapshot_file_absolute_location):
        """
        Executes the video snapshot given the final duration and file location. Meant to run in non-blocking mp.Process.
        :param duration: duration of video snapshot in seconds
        :param snapshot_file_absolute_location: absolute file path for video snapshot (only one arg bc muxed video file)
        :return: snapshot_file_absolute_location if successful
        """
        try:
            snapvid_pipeline = self.pipelines_snap[self.video_snap_name]
            logbook.info("Setting filesink location of video snapshot pipeline.")
            snapvid_pipeline.set_property(PIPE_SINGLE_FILESINK_NAME_FORMATTER.format(self.video_snap_name),
                                          'location', snapshot_file_absolute_location)
            logbook.info("Playing {} pipeline.".format(self.video_snap_name))
            snapvid_pipeline.play()
            logbook.info("Waiting for {} seconds of recording time...".format(duration))
            time.sleep(duration)
            logbook.info("Sending EOS and stop to {} pipeline.".format(self.video_snap_name))
            snapvid_pipeline.eos()
            snapvid_pipeline.stop()
            logbook.info("Video snapshot complete to {}.".format(snapshot_file_absolute_location))
            return snapshot_file_absolute_location
        except (GstdError, GstcError):
            logbook.error("Problem with video snapshot.")
            print_exc()
            return None

    def take_video_snapshot(self, duration=None, file_relative_location=None, file_absolute_location=None, join=False):
        """
        Takes a snapshot of video from each camera, beginning with the buffered backlog of video. This allows the
            video snapshot trigger to grab video from a little while ago. Buffer length specified in config file.
        :param duration: duration of video snapshot in seconds (min=5; max=3600); order of precendence:
            1) function args, 2) config file, 3) parameters.py
        :param file_relative_location: relative location (directory + filename) inside session storage directory; valid
            filename placeholders are '{datetime_local}' = ISO format local datetime, '{datetime_utc}' = ISO format UTC
            datetime, and '{datetime_unix}' = UNIX timestamp
        :param file_absolute_location: (overrides relative location) absolute directory + filename; see
            `file_relative_location` parameter description for filename placeholders
        :param join: T/F wait for snapshot to complete (i.e., call multiprocessing.Process.join())
        :return: None
        """
        # check if video snapshot pipeline was constructed
        if self.video_snap_name not in self.pipelines_snap:
            logbook.error("Video snapshot pipeline wasn't constructed. Ignoring command.")
            return None
        # get the recording duration from the appropriate source
        if duration is None:
            snap_duration = int(self.video_snap_config.get('default_duration', DEFAULT_VIDEO_SNAP_DURATION))
        else:
            snap_duration = duration
        # check duration limits
        if snap_duration > 3600:
            logbook.error("Video snapshot duration is too high. Limit = 1 hour (3600 seconds). Ignoring command.")
            return None
        elif snap_duration < 5:
            logbook.error("Video snapshot duration is too short. Minimum = 5 seconds. Ignoring command.")
            return None
        # decide directory from relative vs. absolute
        if file_absolute_location is None:
            if file_relative_location is None:
                snap_dir, snap_fn = os.path.split(DEFAULT_VIDEO_SNAPSHOT_FILENAME)
            else:
                snap_dir, snap_fn = os.path.split(file_relative_location)
            # check for './' at the start of the filename and compensate
            if snap_dir.startswith('./'):
                snap_abs_dir = os.path.join(self.session_absolute_directory, snap_dir[2:])
            elif snap_dir.startswith('/'):
                # TODO: this isn't right - '/...' implies absolute path
                snap_abs_dir = os.path.join(self.session_absolute_directory, snap_dir[1:])
            else:
                snap_abs_dir = os.path.join(self.session_absolute_directory, snap_dir)
        else:
            snap_abs_dir, snap_fn = os.path.split(file_absolute_location)
        logbook.info("Final video snap directory: {}".format(snap_abs_dir))
        # make the directory, if needed, then set the file location
        try:
            if not os.path.exists(snap_abs_dir):
                logbook.notice("Video snapshot directory doesn't exist. Making it now.")
                os.mkdir(snap_abs_dir)
        except OSError:
            logbook.error("Problem making directory.")
            print_exc()
            return None
        # add in the optional placeholders/formatters
        snap_abs_fn = os.path.join(snap_abs_dir, snap_fn).format(
            datetime_local=datetime.datetime.isoformat(datetime.datetime.now()),
            datetime_utc=datetime.datetime.isoformat(datetime.datetime.utcnow()),
            datetime_unix=str(time.time())[:-3])
        logbook.notice("Final video snap location: {}".format(snap_abs_fn))

        vidsnap = multiprocessing.Process(target=self._video_snapshot_worker,
                                          args=(snap_duration, snap_abs_fn))
        vidsnap.daemon = True
        logbook.notice("Starting video snapshot worker process.")
        try:
            vidsnap.start()
            if join is True:
                logbook.notice("Process started, waiting for completion (join=True).")
                vidsnap.join()
            else:
                logbook.notice("Process started, exiting blocking function.")
        except multiprocessing.ProcessError:
            logbook.error("Problem starting video snap worker process.")
            print_exc()
        return None

    def stop_all_pipelines(self):
        """
        Sends EOS signal to recording and encoder pipelines, then calls stop command on all instantiated pipelines.
        :return: None
        """
        logbook.notice("Sending EOS for relevant pipelines and stopping all pipelines. This will take a few seconds.")
        # send the end of stream (EOS) signals first
        for group in (self.pipelines_video_rec, self.pipelines_video_enc):
            for pipeline_name, pipeline in group.items():
                try:
                    logbook.info("Sending EOS to {}.".format(pipeline_name))
                    pipeline.eos()
                except:
                    logbook.warning("Couldn't EOS {}.".format(pipeline_name))
                    print_exc()
        logbook.info("Waiting a bit for EOS to take effect.")
        time.sleep(10)
        # now stop each pipeline
        for group in (self.pipelines_snap, self.pipelines_video_rec, self.pipelines_video_enc,
                      self.pipelines_video_buffer, self.pipelines_cameras):
            for pipeline_name, pipeline in group.items():
                try:
                    logbook.info("Stopping {}.".format(pipeline_name))
                    pipeline.stop()
                except:
                    logbook.warning("Couldn't stop {}.".format(pipeline_name))

    def deconstruct_all_pipelines(self):
        """
        Calls the pipeline delete command on all instantiated pipelines.
        :return: None
        """
        logbook.notice("Deconstructing all pipelines.")
        for group in (self.pipelines_snap, self.pipelines_video_rec, self.pipelines_video_enc,
                      self.pipelines_video_buffer, self.pipelines_cameras):
            for pipeline_name, pipeline in group.items():
                try:
                    logbook.info("Deleting {} pipeline.".format(pipeline_name))
                    pipeline.delete()
                    logbook.info("Deleted {}.".format(pipeline_name))
                except (GstcError, GstdError):
                    logbook.warning("Exception while deleting {}.".format(pipeline_name))
                    print_exc()

    def stop_all_processes(self):
        """
        Stops all persistent/detached processes from session. These should have been added to self.detached_processes.
            Also stops the logbook queue via its controller at self.logctl.
        :return: None
        """
        self.logctl.stop()
        for proc in self.detached_processes:
            proc.terminate()

    def kill_gstd(self):
        """
        Stops the GstdManager that was instantiated for this IngestSession.
        return: None
        """
        logbook.notice("Stopping Gstreamer Daemon.")
        self.manager.stop()


def sigterm_handler(signum, frame):
    logbook.notice("Received SIGTERM or SIGINT. Exiting with code 0 and shutting down processes.")
    sys.exit(0)

def main(argv):
    """
    Primary routine for video ingestion.
    :param argv: command line arguments read using sys.argv[1:]
    """
    usage = """
    pipeline_management.py [-v] [-t] -c <config-file> -r <session-root-directory> -m <resource-monitor-interval>
    -v: print version and author information, then exit
    -h/--help: print usage information, then exit
    -t: run startup tests, which include running an image and video snapshot
    -c/--config_file: relative or absolute file path for session config file
    -r/--root_directory: location in which to make the session directory where files are stored
    -m/--resource_monitor_interval: number of seconds between resource monitor logging (unspecified = monitor off)
    """
    try:
        opts, args = getopt.getopt(argv, 'vhtc:r:m:',
                                   ['help', 'config_file=', 'root_directory=', 'resource_monitor_interval='])
    except getopt.GetoptError:
        print("Usage:", usage)
        print_exc()
        sys.exit(2)
    config_file = None
    root_directory = None
    startup_test = False
    monitor_interval = None
    for opt, arg in opts:
        if opt == '-v':
            print("Video ingestions pipeline management software.")
            print("Author: {}".format(__author__))
            print("Version: {}".format(__version__))
            print("Status: {}".format(__status__))
            sys.exit()
        elif opt in ('-h', '--help'):
            print("Usage:", usage)
            sys.exit()
        elif opt == '-t':
            startup_test = True
        elif opt in ('-c', '--config_file'):
            config_file = arg
        elif opt in ('-r', '--root_directory'):
            root_directory = arg
        elif opt in ('-m', '--resource_monitor_interval'):
            monitor_interval = int(arg)
    if config_file is None or root_directory is None:
        print("Must supply both config file and session root directory.")
        print("Usage:", usage)
        sys.exit(2)

    # connect sigterm_handler to SIGTERM and SIGINT signals
    signal.signal(signal.SIGTERM, sigterm_handler)
    # signal.signal(signal.SIGKILL, sigterm_handler)
    signal.signal(signal.SIGINT, sigterm_handler)

    session = IngestSession(session_root_directory=root_directory, session_config_file=config_file)
    try:
        # start resource monitor if requested
        if monitor_interval is not None:
            session.start_resource_monitor(log_interval=monitor_interval)
        time.sleep(5)

        # construct and start pipelines
        session.construct_pipelines()
        session.start_cameras()
        session.start_buffers()

        # run startup test of image and video snapshots if requested
        if startup_test is True:
            print("Running startup tests...")
            os.mkdir(os.path.join(session.session_absolute_directory, 'startup_test'))
            if session.video_snap_config.get('enable', 'false').lower() == 'true':
                time.sleep(float(session.video_snap_config.get('buffer_time', DEFAULT_BUFFER_TIME)))
                session.take_video_snapshot(file_relative_location='startup_test/vidsnap.mp4',
                                            join=True)
                print(">>Video snapshot complete.")
            else:
                print(">>Video snapshot not enabled.")
            if session.image_snap_config.get('enable', 'false').lower() == 'true':
                session.take_image_snapshot(cameras='all', file_relative_location='startup_test/imgsnap_{cam_name}.jpg',
                                            join=True)
                print(">>Image snapshot complete.")
            else:
                print(">>Image snapshot not enabled.")
            print("Startup tests complete.")

        # start persistent recording
        session.start_persistent_recording_all_cameras()
        time.sleep(15)

        # infinite loop, take image snapshots if enabled in config file
        while True:
            if session.image_snap_config.get('enable', 'false').lower() == 'true':
                # use default filename
                session.take_image_snapshot(cameras='all')
            time.sleep(1200)

    except KeyboardInterrupt:
        print_exc()
    finally:
        logbook.notice("Shutdown initiated.")
        session.stop_persistent_recording_all_cameras()
        session.stop_all_processes()
        session.stop_all_pipelines()
        session.deconstruct_all_pipelines()
        session.kill_gstd()
        logbook.notice("Shutdown complete.")


if __name__ == '__main__':
    main(sys.argv[1:])
