from parameters import *

import time
import datetime
from pygstc.gstc import *
from pygstc.logger import *
from traceback import print_exc
import subprocess
import os
import warnings


class PipelineEntity(object):
    """

    """
    def __init__(self, client, name, description):
        self._name = name
        self._description = description
        self._client = client
        print("Creating pipeline {} with description {}.".format(self._name, self._description))
        self._client.pipeline_create(self._name, self._description)
    
    def get_name(self):
        return self._name

    def play(self):
        print("Playing pipeline: " + self._name)
        self._client.pipeline_play(self._name)
    
    def stop(self):
        print("Stopping pipeline: " + self._name)
        self._client.pipeline_stop(self._name)
    
    def delete(self):
        print("Deleting pipeline: " + self._name)
        self._client.pipeline_delete(self._name)
    
    def eos(self):
        print("Sending EOS to pipeline: " + self._name)
        self._client.event_eos(self._name)
    
    def set_file_location(self, location):
        print("Setting " + self._name + " pipeline recording/snapshot location to " + location)
        filesink_name = "filesink_" + self._name
        self._client.element_set(self._name, filesink_name, 'location', location)
    
    def set_property(self, element_name, property_name, property_value):
        print("Setting {} property to {}; element {} inside pipeline {}".format(property_name, property_value, element_name, self._name))
        self._client.element_set(self._name, element_name, property_name, property_value)
    
    def listen_to(self, sink):
        print(self._name + " pipeline listening to " + sink)
        self._client.element_set(self._name, self._name + '_src', 'listen-to', sink)


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
        gstd_proc = subprocess.run(self.gstd_args, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, universal_newlines=True)
        if gstd_proc.returncode == 0:
            print("Success.")
        elif gstd_proc.returncode == 1:
            print("Error starting GStreamer Daemon with command {}".format(self.gstd_args))
            if restart is False:
                print("Gstreamer Daemon may already be running. Consider stopping or setting `restart=True`.")
    def stop(self):
        gstd_stop = subprocess.run(['gstd', '--kill'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
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

        :param session_root_directory:
        :param session_config_file:
        :return: None
        """
        self.this_session_number = self._next_session_number(session_root_directory)
        print("Next session number according to session root directory: {}".format(self.this_session_number))
        session_relative_directory = DEFAULT_SESSION_DIRECTORY_FORMAT.format(self.this_session_number)
        self.session_absolute_directory = os.path.join(session_root_directory, session_relative_directory)
        if DEFAULT_SESSION_DIRECTORY_FORMAT.format(self.this_session_number) in os.listdir(session_root_directory):
            raise FileExistsError("""Directory overwrite conflict! 
            Check parameters.SESSION_DIRECTORY_FORMAT and self._next_session_number().""")
        print("Making session directory: {}".format(self.session_absolute_directory))
        os.mkdir(self.session_absolute_directory)
        # fill configuration variables
        # self.camera_config is a list of dictionaries; others are just a dictionary
        print("Parsing configuration file.")
        self.camera_config, self.image_snap_config, self.video_snap_config, self.recording_config = \
            self._parse_config_file(session_config_file)
        print("Copying configuration file to session directory.")
        self._copy_config_file(session_config_file)
        # write the session header file, which includes derivative configuration information
        self._write_session_header_file()
        # instantiate GstD manager to run GStreamer Daemon in the background
        print("Initializing GStreamer Daemon manager.")
        self.manager = None
        self.initialize_gstd()
        # instantiate the GstD Python connection client
        print("Initializing GStreamer Daemon Python client.")
        self.client = None
        self.initialize_gstd_client()

        self.camera_sink_name_formatter = '{}_sink'
        self.pipelines_cameras = {}
        self.image_encoder_name = 'image_encode'
        self.pipelines_video_enc = {}
        self.pipelines_video_buffer = {}
        self.persistent_record_name = 'record_h264'
        self.pipelines_video_rec = {}
        self.video_snap_name = 'snap_video'
        self.image_snap_name = 'snap_image'
        self.pipelines_snap = {}


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
        camera_config = []
        image_snap_config = []
        video_snap_config = []
        recording_config = []
        block_mapping = {'__CAMERA__': camera_config,
                         '__IMAGE-SNAPSHOT__': image_snap_config,
                         '__VIDEO-SNAPSHOT__': video_snap_config,
                         '__PERSISTENT-RECORDING__': recording_config}
        # open configuration file and parse it out
        with open(config_file, 'r') as f:
            current_block = None
            block_destination = None
            for line in f:
                # ignore empty lines and comment lines
                if line is None or len(line.strip()) == 0 or line[0] == '#':
                    continue
                strip_line = line.strip()
                if len(strip_line) > 2 and strip_line[:2] == '__' and strip_line[-2:] == '__':
                    # this is a configuration block line
                    # first check if this is the first one or not
                    if block_destination is not None and len(current_block) > 0:
                        # add the block to its destination if it's non-empty
                        block_destination.append(current_block)
                    # reset current block to empty and set its destination
                    current_block = {}
                    block_destination = block_mapping[strip_line]
                elif '=' in strip_line:
                    pkey, pval = strip_line.split('==')
                    current_block[pkey.strip()] = pval.strip()
                else:
                    raise AttributeError("""Got a line in the configuration file that isn't a block header nor a 
                    key=value.\nLine: {}""".format(strip_line))
            # add the last block of the file (if it's non-empty)
            if block_destination is not None and len(current_block) > 0:
                block_destination.append(current_block)

        # check number of configuration blocks for these configs
        if len(image_snap_config) > 1:
            raise AttributeError("More than one configuration block found for __IMAGE-SNAPSHOT__.")
        elif len(image_snap_config) == 1:     # had one config block
            image_snap_config = image_snap_config[0]
        if len(video_snap_config) > 1:
            raise AttributeError("More than one configuration block found for __VIDEO-SNAPSHOT__.")
        elif len(video_snap_config) == 1:     # had one config block
            video_snap_config = video_snap_config[0]
        if len(recording_config) > 1:
            raise AttributeError("More than one configuration block found for __PERSISTENT-RECORDING__.")
        elif len(recording_config) == 1:     # had one config block
            recording_config = recording_config[0]
        # log configs then return them
        print("Camera configuration:", camera_config)
        print("Image snapshot configuration:", image_snap_config)
        print("Video snapshot configuration:", video_snap_config)
        print("Persistent recording configuration:", recording_config)
        return camera_config, image_snap_config, video_snap_config, recording_config

    def _copy_config_file(self, config_file):
        """
        Copies the given configuration file to a new file inside the session directory, for future reference.
            The new file is called "this_session.config" and located inside self.session_absolute_directory.
        :param config_file: the original configuration file as defined by the user
        :return: absolute file path of config file copy
        """
        copy_filename = os.path.join(self.session_absolute_directory, "this_session.config")
        with open(config_file, 'r') as config_orig:
            with open(copy_filename, 'w') as config_copy:
                for line in config_orig:
                    config_copy.write(line)
        return copy_filename

    def _write_session_header_file(self):
        with open(os.path.join(self.session_absolute_directory, '_SESSION_INFO.txt'), 'w') as f:
            f.write("SESSION #{}".format(self.this_session_number))
            f.write("\nINFORMATIONAL/HEADER FILE\n")
            f.write("-" * 50)
            # directory information
            f.write("\nDirectory (absolute): {}".format(self.session_absolute_directory))
            # time information
            timenow = datetime.datetime.now()
            unix_timenow = (timenow - datetime.datetime(year=1970, month=1, day=1)).total_seconds()
            f.write("\nSession initialization time (local): {} (UNIX: {})".format(timenow, unix_timenow))
            utctimenow = datetime.datetime.utcnow()
            unix_utctimenow = (utctimenow - datetime.datetime(year=1970, month=1, day=1)).total_seconds()
            f.write("\nSession initialization time (UTC): {} (UNIX: {})".format(utctimenow, unix_utctimenow))
            # camera information
            # TODO: put in config information for cameras, recording, etc

    def initialize_gstd(self):
        """

        :return: None
        """
        # TODO: pass in connection parameters
        self.manager = GstdManager(gst_log='/home/dev/Documents/ingest_pipeline/log/gst.log',
                                   gstd_log='/home/dev/Documents/ingest_pipeline/log/gstd.log',
                                   gst_debug_level=5, tcp_enable=True, http_enable=False)
        self.manager.start()

    def initialize_gstd_client(self, num_retry=3):
        """
        Establish connection to GStreamer Daemon running on the system. Set up to retry connection due to some random
            connection issues that seem to resolve on retry.
        :param num_retry: Number of times to retry Gstd client connection before giving up.
        :return: None
        """
        # TODO: pass in connection parameters or connection mode
        gstd_py_logger = CustomLogger(logname="ingest", logfile="/home/dev/Documents/ingest_pipeline/log/pygstc.log",
                                      loglevel="DEBUG")
        for i in range(num_retry):
            try:
                self.client = GstdClient(ip='localhost', port=5000, logger=gstd_py_logger)
                break
            except GstcError:
                print("Problem connecting to Gstd.")
                print_exc()
                time.sleep(1)
        else:
            raise GstcError("Could not contact Gstd after {} attempts.".format(num_retry))
        self.client.debug_enable(True)

    def _construct_camera_pipelines(self):
        for single_camera_config in self.camera_config:
            cam_name = single_camera_config['name']
            if cam_name in self.pipelines_cameras:
                raise AttributeError("Camera name collision. Check configuration file.")
            if 'rtsp_authentication' in single_camera_config and 'rtsp_address' in single_camera_config:
                cam_connect = 'rtsp://{}@{}'.format(single_camera_config['rtsp_authentication'],
                                                    single_camera_config['rtsp_address'])
                cam_source = 'rtspsrc location={}'.format(cam_connect)
            else:
                raise AttributeError("Need rtsp_authentication and rtsp_address in each camera config block.")
            print("Source for camera={}: {}".format(cam_name, cam_source))
            cam_sink = 'interpipesink name={} forward-events=true forward-eos=true sync=false'.format(
                self.camera_sink_name_formatter.format(cam_name))
            pd = '{} ! rtph264depay ! h264parse ! queue ! {}'.format(cam_source, cam_sink)
            cam = PipelineEntity(self.client, cam_name, pd)
            self.pipelines_cameras[cam_name] = cam

    def _construct_persistent_recording_pipeline(self):
        # maximum segment time for multi-segment recording; number of minutes * 60 s/min * 1e9 ns/s
        max_file_time_mins = float(self.recording_config.get('segment_time', DEFAULT_RECORDING_SEGMENT_DURATION))
        max_file_time_ns = int(max_file_time_mins * 60 * 1e9)
        # maximum number of files, per camera, that are kept in storage
        max_num_files = int(self.recording_config.get('maximum_segment_files', DEFAULT_NUMBER_STORED_SEGMENTS))
        # if maximum storage space specified, convert and replace max_num_files
        if 'maximum_camera_storage' in self.recording_config:
            max_storage_mb = float(self.recording_config['maximum_camera_storage']) * 1024
            # convert storage space to recording time
            max_storage_mins = max_storage_mb / ESTIMATED_CAMERA_BITRATE / 60
            # convert to number of files and truncate decimal
            max_num_files = int(max_storage_mins / max_file_time_mins)
            print("Maximum number of files set from maximum storage config value: {}".format(max_num_files))
        else:
            print("Maximum number of files set directly from config value: {}".format(max_num_files))
        # construct the recording pipeline for all the cameras by adding them all to the same description
        pd = ''
        for cam_name in self.pipelines_cameras.keys():
            pd += ' interpipesrc format=time allow-renegotiation=false listen-to={}_sink ! '.format(cam_name)
            pd += 'splitmuxsink name=multisink_{} async-finalize=true muxer-pad-map=x-pad-map,video=video_0'.format(
                cam_name)
        record_h264 = PipelineEntity(self.client, self.persistent_record_name, pd)
        # check the validity of the recording directory and filename
        file_location = self.recording_config.get('recording_filename', DEFAULT_RECORDING_FILENAME)
        # split path location into directory and filename
        file_dir, file_name = os.path.split(file_location)
        file_dir = os.path.join(self.session_absolute_directory, file_dir)
        # check that the file number formatter is present
        if '%d' not in file_name and not any(['%0{}d'.format(i) in file_name for i in range(10)]):
            raise AttributeError("Need to include '%d' or '%0Nd' (N:0-9) in  recording filename template.")
        # check if we need to create camera-specific directories, or just one directory
        if '{}' in file_dir:
            create_dirs = [file_dir.format(cam_name) for cam_name in self.pipelines_cameras.keys()]
        elif '{}' in file_name:
            create_dirs = [file_dir]
        else:
            raise AttributeError("Need to camera name placeholder ('{}') in recording filename template.")
        # create the necessary directories if they don't exist
        try:
            for create_dir in create_dirs:
                if not os.path.exists(create_dir):
                    print("Making directory for persistent recording: {}".format(create_dir))
                    os.mkdir(create_dir)
        except OSError as e:
            print("Problem creating persistent recording directories.")
            print_exc()
            raise e  # re-raise exception
        for cam_name in self.pipelines_cameras.keys():
            cam_dir = (file_dir if '{}' not in file_dir else file_dir.format(cam_name))
            cam_file = (file_name if '{}' not in file_name else file_name.format(cam_name))
            cam_full_location = os.path.join(cam_dir, cam_file)
            print("Setting file path for camera {} to {}".format(cam_name, cam_full_location))
            record_h264.set_property('multisink_{}'.format(cam_name), 'location', cam_full_location)
            record_h264.set_property('multisink_{}'.format(cam_name), 'max-size-time', str(max_file_time_ns))
            record_h264.set_property('multisink_{}'.format(cam_name), 'max-files', str(max_num_files))
        self.pipelines_video_rec[self.persistent_record_name] = record_h264

    def _construct_buffered_video_snapshot_pipeline(self):
        # Video buffers to hold recent data for snapshot
        # ----------------------------------------------------------------------------------------------------------
        # Length of historical video buffer; number of seconds * 1e9 ns/s
        min_buffer_time = float(self.video_snap_config.get('buffer_time', DEFAULT_BUFFER_TIME)) * 1e9
        # Set max time at 105% min time
        overflow_time = min_buffer_time * 1.05
        # Set buffer memory overflow for safety; assume 2x camera bitrate (in parameters.py) * 1024^2 B/MB
        overflow_size = overflow_time * 2 * ESTIMATED_CAMERA_BITRATE * 1024 * 1024

        buffer_name_format = 'buffer_h264_{}'
        for cam_name, _ in self.pipelines_cameras.items():
            buffer_name = buffer_name_format.format(cam_name)
            buffer_source = 'interpipesrc format=time listen-to={}_sink'.format(cam_name)
            # Partial queue definition; other parameters set after pipeline creation.
            qname = 'fifo_queue_{}'.format(cam_name)
            # Set leaky=2 for FIFO; silent=true for no events; disable number of buffers limit.
            queue_def = 'queue name={} max-size-buffers=0 leaky=2 silent=true flush-on-eos=false'.format(qname)
            buffer_sink = 'interpipesink name={}_sink forward-events=true forward-eos=true sync=false'.format(
                buffer_name)
            buffer_def = '{} ! {} ! {}'.format(buffer_source, queue_def, buffer_sink)
            new_buffer = PipelineEntity(self.client, buffer_name, buffer_def)
            # set buffer properties; first convert values to integers then strings
            new_buffer.set_property(qname, 'min-threshold-time', str(int(min_buffer_time)))
            new_buffer.set_property(qname, 'max-size-time', str(int(overflow_time)))
            new_buffer.set_property(qname, 'max-size-bytes', str(int(overflow_size)))
            self.pipelines_video_buffer[buffer_name] = new_buffer

        # Video snapshot - connects to queue-buffers from each camera, muxes, and file-sinks
        # ----------------------------------------------------------------------------------------------------------
        print("CREATING VIDEO SNAPSHOT PIPELINE")
        pd = ''
        # TODO: these probably need to be ordered into mp4mux
        for ci, cam_name in enumerate(self.pipelines_cameras.keys()):
            # TODO: fix confusion with listen-to and interpipesink name
            pd += ' interpipesrc format=time allow-renegotiation=false listen-to={}_sink ! '.format(
                buffer_name_format.format(cam_name))
            pd += 'snapmux.video_{}'.format(ci)
        # file location will be set later when the snapshot is triggered
        pd += ' mp4mux name=snapmux ! filesink name={}_filesink'.format(self.video_snap_name)
        snap_video = PipelineEntity(self.client, self.video_snap_name, pd)
        self.pipelines_snap[self.video_snap_name] = snap_video

    def _construct_image_snapshot_pipeline(self):
        # H.264 -> still image transcoder
        # ----------------------------------------------------------------------------------------------------------
        # source 'listen-to' parameter set at an arbitrary camera for now; changed during snapshot
        encoder_source = 'interpipesrc name={}_src format=time listen-to={}_sink'.format(
            self.image_encoder_name, self.pipelines_cameras.keys()[0])
        # TODO: JPEG encoding configuration? Quality?
        # TODO: selectable image encoder (e.g., PNG)?
        encoder_type = 'jpegenc'
        encoder_sink = 'interpipesink name={}_sink forward-events=true forward-eos=true sync=false async=false enable-last-sample=false drop=true'.format(
            self.image_encoder_name)
        encoder_def = '{} ! avdec_h264 ! {} ! {}'.format(encoder_source, encoder_type, encoder_sink)
        image_encoder = PipelineEntity(self.client, self.image_encoder_name, encoder_def)
        self.pipelines_video_enc[self.image_encoder_name] = image_encoder

        # image snapshot - connects to only one camera at a time via image_encode pipeline and dumps a frame to file
        # ----------------------------------------------------------------------------------------------------------
        print("CREATING IMAGE SNAPSHOT PIPELINE")
        snap_source = 'interpipesrc name={}_src format=time listen-to={} num-buffers=1'.format(
            self.image_snap_name, self.image_encoder_name)
        # file location will be set later when the snapshot is triggered
        snap_sink = 'filesink name={}_filesink'.format(self.image_snap_name)
        snap_image = PipelineEntity(self.client, self.image_snap_name, '{} ! {}'.format(snap_source, snap_sink))
        self.pipelines_snap[self.image_snap_name] = snap_image

    def construct_pipelines(self):
        """
        Construct all pipelines based on the configuration variables for this session.
        :return: None
        """
        try:
            # Create camera pipelines
            # ----------------------------------------------------------------------------------------------------------
            print("CREATING CAMERA PIPELINES")
            self._construct_camera_pipelines()

            # H.264 recording via MPEG4 container mux to parallel streams
            # ----------------------------------------------------------------------------------------------------------
            if len(self.recording_config) > 0 and self.recording_config.get('enable', 'false').lower() == 'true':
                print("CREATING PERSISTENT RECORDING PIPELINES")
                self._construct_persistent_recording_pipeline()

            # Camera FIFO historical video buffers for video snapshot capability
            # ----------------------------------------------------------------------------------------------------------
            if len(self.video_snap_config) > 0 and self.video_snap_config.get('enable', 'false').lower() == 'true':
                print("CREATING BUFFER PIPELINES")
                self._construct_buffered_video_snapshot_pipeline()

            # H.264 to still image transcoder for image snapshot capability
            # ----------------------------------------------------------------------------------------------------------
            if len(self.image_snap_config) > 0 and self.image_snap_config.get('enable', 'false').lower() == 'true':
                print("CREATING STILL IMAGE ENCODER PIPELINE")
                self._construct_image_snapshot_pipeline()

        except (GstcError, GstdError) as e:
            print("Failure during pipeline construction.")
            print_exc()
            self.deconstruct_all_pipelines()
            self.kill_gstd()
            raise e

    def start_cameras(self):
        """
        Start each camera stream pipeline. Raises error if unsuccessful.
        :return: None
        """
        try:
            print("Starting camera streams.")
            for pipeline_name, pipeline in self.pipelines_cameras.items():
                print("Starting {}.".format(pipeline_name))
                pipeline.play()
            time.sleep(5)
            print("Camera streams initialized.")
        except (GstcError, GstdError) as e:
            print("Could not initialize camera streams.")
            print_exc()
            self.deconstruct_all_pipelines()
            self.kill_gstd()
            raise e

    def start_buffers(self):
        """
        Start the video buffer pipelines so they start holding backlog.
        :return: None
        """
        try:
            print("Starting camera stream buffers for video snapshot.")
            for pipeline_name, pipeline in self.pipelines_video_buffer.items():
                print("Starting {}.".format(pipeline_name))
                pipeline.play()
            time.sleep(1)
            print("Camera stream buffers initialized.")
            print("Buffers will reach capacity in {} seconds.".format(
                self.video_snap_config.get('buffer_time', DEFAULT_BUFFER_TIME)))
        except (GstcError, GstdError) as e:
            print("Could not initialize camera stream buffers.")
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
        file_location = self.recording_config.get('recording_filename', DEFAULT_RECORDING_FILENAME)
        unformat_dir, unformat_file = os.path.split(file_location)
        fns = []
        try:
            # for each camera: format the filename, then set location param inside the pipeline splitmuxsink
            for cam_name in self.pipelines_cameras.keys():
                format_dir = (unformat_dir.format(cam_name) if '{}' in unformat_dir else unformat_dir)
                format_file = (unformat_file.format(cam_name) if '{}' in unformat_file else unformat_file)
                # by this point the file portion of the path has already been checked to contain '%0Nd'
                format_full_path = os.path.join(format_dir, format_file)
                # >> RIGHT NOW THESE ARE BEING SET IN THE PIPELINE CONSTRUCTION
                # print("Setting file location for {} to {}.".format(cam_name, format_full_path))
                # self.pipelines_video_rec[self.persistent_record_name].set_property(
                #     'multisink_{}'.format(cam_name), 'location', format_full_path)
                fns.append(format_full_path)
        except (GstcError, GstdError) as e:
            print("Could not set camera recording locations. Failed on {}".format(cam_name))
            print_exc()
            return None
        print("Set file locations for all {} cameras.".format(len(self.pipelines_cameras)))
        # start the whole recording pipeline
        print("Starting recording.")
        try:
            self.pipelines_video_rec[self.persistent_record_name].play()
            time.sleep(5)
            print("Persistent recording pipeline playing.")
        except (GstcError, GstdError):
            print("Couldn't play persistent recording pipeline.")
            print_exc()
            return None
        return fns

    def stop_persistent_recording_all_cameras(self):
        """
        Send EOS to recording pipeline, then stop it.
        :return: None
        """
        try:
            print("Sending EOS to persistent recording pipeline.")
            self.pipelines_video_rec[self.persistent_record_name].eos()
            print("Stopping persistent recording pipeline.")
            self.pipelines_video_rec[self.persistent_record_name].stop()
        except (GstcError, GstdError) as e:
            print("Problem with stopping persistent recording.")
            print_exc()

    def take_image_snapshot(self, file_relative_location, file_absolute_location=None, cameras='all'):
        """
        Takes a still image snapshot of each camera specified. They are taken one at a time in order to avoid spinning
            up numerous H.264->still transcoding pipelines. Failure of one snapshot will not prevent the others.
        :param file_relative_location: location inside session directory to store snapshots; if more than one camera
            is specified, then '{}' placeholder must be included in directory or filename portion for camera name
        :param file_absolute_location: same as relative location, but setting this != None automatically overrides it
        :param cameras: cameras to snapshot; 'all'=all cameras; list/tuple of camera names; ','-sep. str of camera names
        :return: list of absolute filenames for each snapshot that was successful
        """
        # check if video snapshot pipeline was constructed
        if self.image_snap_name not in self.pipelines_snap or self.image_encoder_name not in self.pipelines_video_enc:
            warnings.warn("Image snapshot pipeline or encoder pipeline wasn't constructed. Ignoring command.")
            return None
        else:
            # get the image snap and image encode pipelines
            snapimg_pipeline = self.pipelines_snap[self.image_snap_name]
            encode_img_pipeline = self.pipelines_video_enc[self.image_encoder_name]
        # extract the camera list from the given parameter
        if cameras == 'all':
            camlist = list(self.pipelines_cameras.keys())
        elif type(cameras) in (list, tuple):
            camlist = cameras
        elif type(cameras) is str:
            camlist = cameras.split(',')
        else:
            warnings.warn("""Got an invalid type for argument `cameras`. 
            Need list/tuple of str or comma-delineated str or 'all'.""")
            return []
        # check that all of the camera names are correct
        if not all([cam in self.pipelines_cameras for cam in camlist]):
            warnings.warn("""One or more of the cameras specified for snapshot is not valid. 
            Available: {}. Specified: {}. Ignoring command.""".format(self.pipelines_cameras.keys(), cameras))
            return []
        print("Snapping cameras: {}".format(camlist))
        # set the directory and filename from the absolute or relative parameters given
        if file_absolute_location is None:
            snap_dir, snap_fn = os.path.split(file_relative_location)
        else:
            snap_dir, snap_fn = os.path.split(file_absolute_location)
        # check if there are multiple cameras and make sure the placeholder is included
        if len(camlist) > 1 and ('{}' not in snap_fn or '{}' not in snap_dir):
            warnings.warn("More than one camera given for snapshot, but no '{}' in file location. Ignoring command.")

        fns = []
        # run each camera independently
        # first check the directory existence and create if necessary
        for camera_name in self.pipelines_cameras.keys():
            try:
                if file_absolute_location is None:
                    snap_abs_dir = os.path.join(self.session_absolute_directory,
                                                (snap_dir if '{}' not in snap_dir else snap_dir.format(camera_name)))
                else:
                    snap_abs_dir = (snap_dir if '{}' not in snap_dir else snap_dir.format(camera_name))
                if not os.path.exists(snap_abs_dir):
                    print("Making directory: {}".format(snap_abs_dir))
                snap_abs_fn = os.path.join(snap_abs_dir,
                                           (snap_fn if '{}' not in snap_fn else snap_fn.format(camera_name)))
                # set the location of the filesink in the image snap pipeline
                print("Setting location of {} pipeline filesink to {}.".format(self.image_snap_name, snap_abs_fn))
                snapimg_pipeline.set_property("{}_filesink".format(self.image_snap_name), 'location', snap_abs_fn)
            except (OSError, GstcError, GstdError):
                print("Problem setting up directory and setting filesink location.")
                print_exc()
                continue
            try:
                # set the encoding pipeline to listen to the appropriate camera
                print("Setting encoding interpipe to listen to {}.".format(camera_name))
                encode_img_pipeline.listen_to('{}_sink'.format(camera_name))
                # play the image encoder pipeline and let it spin up (needs a key frame for proper H.264 decoding)
                print("Playing image encoder.")
                encode_img_pipeline.play()
                time.sleep(IMAGE_ENCODE_SPIN_UP)
                # run the snap image pipeline for a bit, not sure if this time matters much
                snapimg_pipeline.play()
                time.sleep(IMAGE_SNAP_EXECUTE_TIME)
                # TODO: EOS encoder?
                snapimg_pipeline.stop()
                fns.append(snap_abs_fn)
            except (GstcError, GstdError):
                print("Problem with encoding/snapshot pipeline.")
                print_exc()
                continue
        return fns

    def take_video_snapshot(self, duration, file_relative_location, file_absolute_location=None):
        """
        Takes a snapshot of video from each camera, beginning with the buffered backlog of video. This allows the
            video snapshot trigger to grab video from a little while ago. Buffer length specified in config file.
        :param duration: duration of video snapshot in seconds (min=5; max=3600)
        :param file_relative_location: relative location (directory + filename) inside session storage directory
        :param file_absolute_location: (overrides relative location) absolute directory + filename
        :return: absolute file path of snapshot video file, if successful; otherwise None
        """
        # check if video snapshot pipeline was constructed
        if self.video_snap_name not in self.pipelines_snap:
            warnings.warn("Video snapshot pipeline wasn't constructed. Ignoring command.")
            return None
        else:
            snapvid_pipeline = self.pipelines_snap[self.video_snap_name]
        # check duration limits
        if duration > 3600:
            warnings.warn("Video snapshot duration is too high. Limit = 1 hour (3600 seconds). Ignoring command.")
            return None
        elif duration < 5:
            warnings.warn("Video snapshot duration is too short. Minimum = 5 seconds. Ignoring command.")
            return None
        # decide directory from relative vs. absolute
        if file_absolute_location is None:
            snap_dir, snap_fn = os.path.split(file_relative_location)
            if snap_dir.startswith('./'):
                snap_dir = snap_dir[2:]
                print("Reformatting relative location to {}".format(snap_dir))
            elif snap_dir.startswith('/'):
                snap_dir = snap_dir[1:]
                print("Reformatting relative location to {}".format(snap_dir))
            snap_abs_dir = os.path.join(self.session_absolute_directory, snap_dir)
            print("Relative file location inside session directory: {}".format(snap_abs_dir))
        else:
            snap_abs_dir, snap_fn = os.path.split(file_absolute_location)
            print("Absolute file location directory override for video snap: {}".format(snap_abs_dir))
        # make the directory, if needed, then set the file location
        try:
            if not os.path.exists(snap_abs_dir):
                print("Directory doesn't exist. Making it now.")
                os.mkdir(snap_abs_dir)
            snap_abs_fn = os.path.join(snap_abs_dir, snap_fn)
            print("Setting location of {} pipeline filesink to {}.".format(self.video_snap_name, snap_abs_fn))
            snapvid_pipeline.set_property("{}_filesink".format(self.video_snap_name), 'location', snap_abs_fn)
        except (OSError, GstdError, GstcError):
            print("Problem with setting video snapshot file location.")
            print_exc()
            return None
        # play the video snapshot pipeline, send an EOS signal, then stop the pipeline
        try:
            print("Playing {} pipeline.".format(self.video_snap_name))
            snapvid_pipeline.play()
            print("Waiting for {} seconds of recording time...".format(duration))
            time.sleep(duration)
            print("Sending EOS and stop to {} pipeline.".format(self.video_snap_name))
            snapvid_pipeline.eos()
            snapvid_pipeline.stop()
            print("Video snapshot complete to {}.".format(snap_abs_fn))
        except (GstcError, GstdError):
            print("Problem playing and completing video shapshot pipeline.")
            print_exc()
            return None
        return snap_abs_fn

    def stop_all_pipelines(self):
        """
        Sends EOS signal to recording and encoder pipelines, then calls stop command on all instantiated pipelines.
        :return: None
        """
        # send the end of stream (EOS) signals first
        for group in (self.pipelines_video_rec, self.pipelines_video_enc):
            for pipeline_name, pipeline in group.items():
                try:
                    print("Sending EOS to {}.".format(pipeline_name))
                    pipeline.eos()
                except:
                    print("Couldn't EOS {}.".format(pipeline_name))
                    print_exc()
        print("Waiting a bit for EOS to take effect.")
        time.sleep(10)
        # now stop each pipeline
        for group in (self.pipelines_snap, self.pipelines_video_rec, self.pipelines_video_enc,
                      self.pipelines_video_buffer, self.pipelines_cameras):
            for pipeline_name, pipeline in group.items():
                try:
                    print("Stopping {}.".format(pipeline_name))
                    pipeline.stop()
                except:
                    print("Couldn't stop {}.".format(pipeline_name))

    def deconstruct_all_pipelines(self):
        """
        Calls the pipeline delete command on all instantiated pipelines.
        :return: None
        """
        for group in (self.pipelines_snap, self.pipelines_video_rec, self.pipelines_video_enc,
                      self.pipelines_video_buffer, self.pipelines_cameras):
            for pipeline_name, pipeline in group.items():
                try:
                    print("Deleting {} pipeline.".format(pipeline_name))
                    pipeline.delete()
                    print("Deleted {}.".format(pipeline_name))
                except (GstcError, GstdError):
                    print("Exception while deleting {}.".format(pipeline_name))
                    print_exc()

    def kill_gstd(self):
        """
        Stops the GstdManager that was instantiated for this IngestSession.
        return: None
        """
        self.manager.stop()

if __name__ == '__main__':
    session = IngestSession(session_root_directory='/home/dev/Videos/ingest_pipeline',
                            session_config_file='./sample.config')
    try:
        session.construct_pipelines()
        session.start_cameras()
        # session.start_buffers()
        session.start_persistent_recording_all_cameras()
        time.sleep(40)
        session.take_video_snapshot(duration=35, file_relative_location='/vidsnap/snap0.mp4')
        time.sleep(65)
        session.stop_persistent_recording_all_cameras()
    except KeyboardInterrupt:
        print_exc()
    finally:
        session.stop_all_pipelines()
        session.deconstruct_all_pipelines()
        session.kill_gstd()
