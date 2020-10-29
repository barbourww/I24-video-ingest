import datetime as dt
from ast import literal_eval
import matplotlib.pyplot as plt
import os
from utilities import get_manager_log_files


def plot_resource_usage(session_directory, plot_directory=None, cpu=True, memory=True, network=True, disk=True, recording=True):
    """
    Plot line graphs of resource usage values over time that were collected in video ingest session log files.
    :param session_directory: top level directory for the video ingest session
    :param plot_directory: directory in which to write plots (optional, defaults to log directory in session directory)
    :param cpu: (T/F) plot CPU usage
    :param memory: (T/F) plot memory usage
    :param network: (T/F) plot network usage
    :param disk: (T/F) plot disk usage
    :param recording: (T/F) plot recording directory file usage
    :return: file paths of plots that were written
    """
    # filtered lists for CPU, memory, network, disk, and recording categories
    cpu_vals = []
    memory_vals = []
    network_vals = []
    disk_vals = []
    recording_vals = []
    # determine the available manager filenames
    log_files = get_manager_log_files(session_directory=session_directory)
    # default to plot directory if needed
    if plot_directory is None:
        plot_directory = os.path.join(session_directory, 'logs')
    # filter the applicable log files
    for lf in log_files:
        with open(lf, 'r') as f:
            for line in f:
                if 'CPU:' in line:
                    cpu_vals.append((dt.datetime.fromisoformat(line.split(']')[0].strip('[')),
                                     literal_eval(line.split('CPU:')[1].strip())))
                elif 'MEMORY:' in line:
                    memory_vals.append((dt.datetime.fromisoformat(line.split(']')[0].strip('[')),
                                        literal_eval(line.split('MEMORY:')[1].strip())))
                elif 'NETWORK:' in line:
                    network_vals.append((dt.datetime.fromisoformat(line.split(']')[0].strip('[')),
                                         literal_eval(line.split('NETWORK:')[1].strip())))
                elif 'DISK:' in line:
                    disk_vals.append((dt.datetime.fromisoformat(line.split(']')[0].strip('[')),
                                      literal_eval(line.split('DISK:')[1].strip())))
                elif 'RECORDING:' in line:
                    recording_vals.append((dt.datetime.fromisoformat(line.split(']')[0].strip('[')),
                                           literal_eval(line.split('RECORDING:')[1].strip())))
                else:
                    continue

    # sort according to datetime for log entry
    cpu_vals.sort(key=lambda x: x[0])
    memory_vals.sort(key=lambda x: x[0])
    network_vals.sort(key=lambda x: x[0])
    disk_vals.sort(key=lambda x: x[0])
    recording_vals.sort(key=lambda x: x[0])

    # do the applicable plots
    plots_written = []
    if cpu is True:
        fig, ax = plt.subplots(1, 1, figsize=(12, 6))
        ct, cv = zip(*cpu_vals)
        ax.plot(ct, [v[0] for v in cv], label='CPU 1-min history')
        ax.plot(ct, [v[1] for v in cv], label='CPU 5-min history')
        ax.plot(ct, [v[2] for v in cv], label='CPU 15-min history')
        ax.set_ylabel("CPU utilization (%)", fontsize=12)
        ax.set_xlabel("Date, hour", fontsize=12)
        ax.set_title("System CPU utilization statistics", fontsize=16)
        fig.legend(fontsize=12)
        pfn = os.path.join(plot_directory, "cpu.pdf")
        plt.savefig(pfn)
        plots_written.append(pfn)
    if memory is True:
        fig, ax = plt.subplots(1, 1, figsize=(12, 6))
        mt, mv = zip(*memory_vals)
        ax.plot(mt, [v[0] / 1e9 for v in mv], label='Available memory')
        ax.plot(mt, [v[1] / 1e9 for v in mv], label='Total memory')
        ax.set_ylabel("Gigabytes (1e9 bytes)", fontsize=12)
        ax.set_xlabel("Date, hour", fontsize=12)
        ax.set_title("System memory (RAM) statistics", fontsize=16)
        fig.legend(fontsize=12)
        pfn = os.path.join(plot_directory, "memory.pdf")
        plt.savefig(pfn)
        plots_written.append(pfn)
    if network is True:
        fig, ax = plt.subplots(1, 1, figsize=(12, 6))
        nt, nv = zip(*network_vals)
        ax.plot(nt, [v[0] / 1e9 for v in nv], label='Network sent')
        ax.plot(nt, [v[1] / 1e9 for v in nv], label='Network received')
        ax.set_ylabel("Gigabytes (1e9 bytes)", fontsize=12)
        ax.set_xlabel("Date, hour", fontsize=12)
        ax.set_title("Network traffic statistics", fontsize=16)
        fig.legend(fontsize=12)
        pfn = os.path.join(plot_directory, "network.pdf")
        plt.savefig(pfn)
        plots_written.append(pfn)
    if disk is True:
        fig, ax = plt.subplots(1, 1, figsize=(12, 6))
        kt, kv = zip(*disk_vals)
        ax.plot(kt, [v[0] / 1e9 for v in kv], label='Disk space used')
        ax.plot(kt, [v[1] / 1e9 for v in kv], label='Disk space free')
        ax.plot(kt, [v[2] / 1e9 for v in kv], label='Disk space total')
        ax.set_ylabel("Gigabytes (1e9 bytes)", fontsize=12)
        ax.set_xlabel("Date, hour", fontsize=12)
        ax.set_title("Disk space statistics", fontsize=16)
        fig.legend(fontsize=12)
        pfn = os.path.join(plot_directory, "disk.pdf")
        plt.savefig(pfn)
        plots_written.append(pfn)
    if recording is True:
        fig, ax = plt.subplots(1, 1, figsize=(12, 6))
        ft, fv = zip(*recording_vals)
        ax.plot(ft, [v[0] for v in fv], label='Files recorded')
        ax2 = ax.twinx()
        ax2.plot(ft, [v[1] / 1e9 for v in fv], label='Files size', c='orange')
        ax2.set_ylim((ax2.get_ylim()[0], ax2.get_ylim()[1] * 1.1))
        ax.set_ylabel("Number of files", fontsize=12)
        ax2.set_ylabel("Gigabytes (1e9 bytes)", fontsize=12)
        ax.set_xlabel("Date, hour", fontsize=12)
        ax.set_title("File recording statistics", fontsize=16)
        fig.legend(fontsize=12)
        pfn = os.path.join(plot_directory, "files.pdf")
        plt.savefig(pfn)
        plots_written.append(pfn)
    return plots_written
