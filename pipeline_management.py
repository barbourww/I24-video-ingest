from parameters import *

import time
import datetime
from pygstc.gstc import *
from pygstc.logger import *
from traceback import print_exc
import subprocess
import os


class PipelineEntity(object):
    """

    """
    def __init__(self, client, name, description):
        self._name = name
        self._description = description
        self._client = client
        print("Creating pipeline: " + self._name)
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

    """
    def __init__(self, session_root_directory, session_config_file):
        """

        :param session_root_directory:
        :param session_config_file:
        :return: None
        """
        self.this_session_number = self._next_session_number(session_root_directory)
        session_relative_directory = DEFAULT_SESSION_DIRECTORY_FORMAT.format(self.this_session_number)
        self.session_absolute_directory = os.path.join(session_root_directory, session_relative_directory)
        if DEFAULT_SESSION_DIRECTORY_FORMAT.format(self.this_session_number) in os.listdir(session_root_directory):
            raise FileExistsError("""Directory overwrite conflict! 
            Check parameters.SESSION_DIRECTORY_FORMAT and self._next_session_number().""")
        os.mkdir(self.session_absolute_directory)
        # fill configuration variables
        # TODO: list all the config variables here with None initialization
        # self.camera_config is a list of dictionaries; others are just a dictionary
        self.camera_config, self.image_snap_config, self.video_snap_config, self.recording_config = \
            self._parse_config_file(session_config_file)
        self._copy_config_file(session_config_file)

        self.manager = None
        self.initialize_gstd()
        self.client = None
        self.initialize_gstd_client()

        self.pipelines_cameras = {}
        self.pipelines_video_enc = {}
        self.pipelines_video_buffer = {}
        self.pipelines_video_rec = {}
        self.pipelines_snap = {}


    def _next_session_number(self, session_root_directory):
        """
        Checks what directories present in `session_root_directory` match the SESSION_DIRECTORY_FORMAT and calculates
            the next session number. Does not wrap around to zero.
        :param session_root_directory: Root directory where session-specific directories will be placed.
        :return: largest session number found in `session_root_directory` plus 1
        """
        root_list_dir = os.listdir(session_root_directory)
        present_matches = []
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
                if line is None or len(line) == 0 or line[0] == '#':
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
        with open(os.path.join(self.session_absolute_directory, '_SESSION_INFO.txt')) as f:
            f.write("SESSION #{}".format(self.this_session_number))
            f.write("INFORMATIONAL/HEADER FILE")
            f.write("-" * 50)
            # directory information
            f.write("\nDirectory (absolute): {}".format(self.session_absolute_directory))
            # time information
            timenow = datetime.datetime.now()
            unix_timenow = (timenow - datetime.datetime(year=1970, month=1, day=1)).total_seconds()
            f.write("\nSession initialization time (local): {} (UNIX: {})".format(timenow, unix_timenow))
            utctimenow = datetime.datetime.utcnow()
            unix_utctimenow = (utctimenow - datetime.datetime(year=1970, month=1, day=1)).total_seconds()
            f.write("Session initialization time (UTC): {} (UNIX: {})".format(utctimenow, unix_utctimenow))
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

    def construct_pipelines(self):
        try:
            # Create camera pipelines
            # ----------------------------------------------------------------------------------------------------------
            for single_camera_config in self.camera_config:
                if 'rtsp_authentication' in single_camera_config and 'rtsp_address' in single_camera_config:
                    cam_connect = 'rtsp://{}@{}'.format(single_camera_config['rtsp_authentication'],
                                                        single_camera_config['rtsp_address'])
                    cam_source = 'rtspsrc location={}'.format(cam_connect)
                else:
                    raise AttributeError("Need rtsp_authentication and rtsp_address in each camera config block.")
                cam_name = single_camera_config['name']
                cam_sink = 'interpipesink name={}_sink forward-events=true forward-eos=true sync=false'.format(cam_name)
                cam = PipelineEntity(self.client, cam_name,
                                     '{} ! rtph264depay ! h264parse ! queue ! {}'.format(cam_connect, cam_sink))
                self.pipelines_cameras[cam_name] = cam

            # H.264 recording via MPEG4 container mux to parallel streams
            # ----------------------------------------------------------------------------------------------------------
            if len(self.recording_config) > 0 and self.recording_config.get('enable', 'false').lower() == 'true':
                record_name = 'record_h264'
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
                pd = ''
                for cam_name in self.pipelines_cameras.keys():
                    pd += ' interpipesrc format=time allow-renegotiation=false listen-to={} ! '.format(cam_name)
                    pd += 'splitmuxsink name=multisink_{} async-finalize=true muxer-pad-map=x-pad-map,video=video_0'.format(cam_name)
                record_h264 = PipelineEntity(self.client, record_name, pd)
                file_location = self.recording_config.get('recording_filename', DEFAULT_RECORDING_FILENAME)
                # split path location into directory and filename
                file_dir, file_name = os.path.split(file_location)
                if '%d' not in file_name and not any(['%0{}d'.format(i) in file_name for i in range(10)]):
                    raise AttributeError("Need to include '%d' or '%0Nd' (N:0-9) in  recording filename template.")
                # check if we need to create camera-specific directories
                if '{}' in file_dir:
                    create_dirs = [file_dir.format(cam_name) for cam_name in self.pipelines_cameras.keys()]
                elif '{}' in file_name:
                    create_dirs = [file_dir]
                else:
                    raise AttributeError("Need to camera name placeholder ('{}') in recording filename template.")
                # create the necessary directories if they don't exist
                for create_dir in create_dirs:
                    if not os.path.exists(create_dir):
                        os.mkdir(create_dir)
                for cam_name in self.pipelines_cameras.keys():
                    cam_dir = (file_dir if '{}' not in file_dir else file_dir.format(cam_name))
                    cam_file = (file_name if '{}' not in file_name else file_name.format(cam_name))
                    record_h264.set_property('multisink_{}'.format(cam_name), 'location',
                                             os.path.join(cam_dir, cam_file))
                    record_h264.set_property('multisink_{}'.format(cam_name), 'max-size-time', str(max_file_time_ns))
                    record_h264.set_property('multisink_{}'.format(cam_name), 'max-files', str(max_num_files))
                self.pipelines_video_rec[record_name] = record_h264

            # Camera FIFO historical video buffers for video snapshot capability
            # ----------------------------------------------------------------------------------------------------------
            if len(self.video_snap_config) > 0 and self.video_snap_config.get('enable', 'false').lower() == 'true':
                # Length of historical video buffer; number of seconds * 1e9 ns/s
                min_buffer_time = self.video_snap_config.get('buffer_time', DEFAULT_BUFFER_TIME) * 1e9
                # Set max time at 105% min time
                overflow_time = min_buffer_time * 1.05
                # Set buffer memory overflow for safety; assume 2x camera bitrate (in parameters.py) * 1024^2 B/MB
                overflow_size = overflow_time * 2 * ESTIMATED_CAMERA_BITRATE * 1024 * 1024

                for cam_name, _ in self.pipelines_cameras.items():
                    buffer_name = 'buffer_h264_{}'.format(cam_name)
                    buffer_source = 'interpipesrc format=time listen-to={}', cam_name
                    # Partial queue definition; other parameters set after pipeline creation.
                    qname = 'fifo_queue_{}'.format(cam_name)
                    # Set leaky=2 for FIFO; silent=true for no events; disable number of buffers limit.
                    queue_def = 'queue name={} max-size-buffers=0 leaky=2 silent=true flush-on-eos=false'
                    buffer_sink = 'interpipesink name={}_sink forward-events=true forward-eos=true sync=false'.format(
                        buffer_name)
                    buffer_def = '{} ! {} ! {}'.format(buffer_source, queue_def, buffer_sink)
                    new_buffer = PipelineEntity(self.client, buffer_name, buffer_def)
                    new_buffer.set_property(qname, 'min-threshold-time', str(int(min_buffer_time)))
                    new_buffer.set_property(qname, 'max-size-time', str(int(overflow_time)))
                    new_buffer.set_property(qname, 'max-size-bytes', str(int(overflow_size)))
                    self.pipelines_video_buffer[buffer_name] = new_buffer

            # Buffered video snapshot - conencts to queue-buffers from each camera, muxes, and file-sinks
            # ----------------------------------------------------------------------------------------------------------
                video_snap_name = 'snap_video'
                pd = ''
                # TODO: these probably need to be ordered into mp4mux
                for ci, cam_name in enumerate(self.pipelines_cameras.keys()):
                    pd += ' interpipesrc format=time allow-renegotiation=false listen-to=buffer_h264_{} ! '.format(
                        cam_name)
                    pd += 'snapmux.video_{}'.format(ci)
                # file location will be set later when the snapshot is triggered
                pd += ' mp4mux name=snapmux ! filesink name={}_filesink'.format(video_snap_name)
                snap_video = PipelineEntity(self.client, video_snap_name, pd)
                self.pipelines_snap[video_snap_name] = snap_video

            # H.264 to still image transcoder for image snapshot capability
            # ----------------------------------------------------------------------------------------------------------
            if len(self.image_snap_config) > 0 and self.image_snap_config.get('enable', 'false').lower() == 'true':
                encoder_name = 'image_encode'
                # source 'listen-to' parameter set at an arbitrary camera for now; changed during snapshot
                encoder_source = 'interpipesrc name={}_src format=time listen-to={}'.format(
                    encoder_name, self.pipelines_cameras.keys()[0])
                # TODO: JPEG encoding configuration? Quality?
                # TODO: selectable image encoder (e.g., PNG)?
                encoder_type = 'jpegenc'
                encoder_sink = 'interpipesink name={}_sink forward-events=true forward-eos=true sync=false async=false enable-last-sample=false drop=true'.format(
                    encoder_name)
                encoder_def = '{} ! avdec_h264 ! {} ! {}'.format(encoder_source, encoder_type, encoder_sink)
                image_encoder = PipelineEntity(self.client, encoder_name, encoder_def)
                self.pipelines_video_enc[encoder_name] = image_encoder

            # image snapshot - connects to only one camera at a time via image_encode pipeline and dumps a frame to file
            # ----------------------------------------------------------------------------------------------------------
                image_snap_name = 'snap_image'
                snap_source = 'interpipesrc name={}_src format=time listen-to={} num-buffers=1'.format(
                    image_snap_name, encoder_name)
                # file location will be set later when the snapshot is triggered
                snap_sink = 'filesink name={}_filesink'.format(image_snap_name)
                snap_image = PipelineEntity(self.client, image_snap_name, '{} ! {}'.format(snap_source, snap_sink))
                self.pipelines_snap[image_snap_name] = snap_image

        except (GstcError, GstdError) as e:
            print_exc()
            raise e

        finally:
            self.deconstruct_all_pipelines()
            self.kill_gstd()

    def start_cameras(self):
        pass

    def begin_persistent_recording_all_cameras(self):
        pass

    def stop_persistent_recording_all_cameras(self):
        pass

    def take_image_snapshot(self):
        pass

    def take_video_snapshot(self, duration):
        pass

    def stop_all_pipelines(self):
        for group in (self.pipelines_snap, self.pipelines_video_rec, self.pipelines_video_enc,
                      self.pipelines_video_buffer, self.pipelines_cameras):
            for pipeline_name, pipeline in group.items():
                pipeline.stop()

    def deconstruct_all_pipelines(self):
        for group in (self.pipelines_snap, self.pipelines_video_rec, self.pipelines_video_enc,
                      self.pipelines_video_buffer, self.pipelines_cameras):
            for pipeline_name, pipeline in group.items():
                pipeline.delete()

    def kill_gstd(self):
        self.manager.stop()

if __name__ == '__main__':



        # Play base pipelines
        for pipeline in pipelines_cameras:
            pipeline.play()
        for pipeline in pipelines_video_buffer:
            pipeline.play()
        time.sleep(10)

        # Set locations for video recordings
        for camera in pipelines_cameras:
            cam_name = camera.get_name()
            record_h264.set_property('multisink_{}'.format(cam_name), 'location',
                                     '/home/dev/Videos/ingest_pipeline/record_{}_%05d.mp4'.format(cam_name))
        # Play video recording pipelines
        for pipeline in pipelines_video_rec:
            pipeline.play()
        time.sleep(60)

        # Execute video snapshot
        snap_video.set_file_location('/home/dev/Videos/ingest_pipeline/video_snap.mp4')
        snap_video.play()
        time.sleep(20)
        snap_video.eos()

        # Execute still image snapshot
        # 5 second runtime per camera
        for camera in ('camera0', 'camera1', 'camera2', 'camera3'):
            print("Snapping {}.".format(camera))
            encode_jpeg.listen_to(camera)
            encode_jpeg.play()
            time.sleep(3)
            snap_jpeg.set_file_location('/home/dev/Videos/ingest_pipeline/snap_{}.jpeg'.format(camera))
            snap_jpeg.play()
            time.sleep(2)
            snap_jpeg.stop()

        # Send EOS event to encode pipelines for proper closing
        # EOS to recording pipelines
        for pipeline in pipelines_video_rec:
            pipeline.eos()
        for pipeline in pipelines_video_enc:
            pipeline.eos()

        time.sleep(10)

        # Stop pipelines
        for group in (pipelines_snap, pipelines_video_rec, pipelines_video_enc, pipelines_cameras):
            for pipeline in group:
                try:
                    pipeline.stop()
                except:
                    pass
    except KeyboardInterrupt:
        print_exc()
    finally:
        # Delete pipelines
        for group in (pipelines_snap, pipelines_video_rec, pipelines_video_enc, pipelines_cameras):
            for pipeline in group:
                pipeline.delete()
        manager.stop()