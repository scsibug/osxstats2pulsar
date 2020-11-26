import plistlib
import subprocess
import pulsar
import configparser
import signal
import os
import sys
from enum import Enum
import time

from pulsar.schema import *

class NetworkStat(Record):
    output_packet_rate = Float()
    input_packet_rate = Float()
    output_byte_rate = Float()
    input_byte_rate = Float()

class BacklightStat(Record):
    brightness = Float()

class DiskStat(Record):
    read_ops_rate = Float()
    write_ops_rate = Float()
    read_bytes_rate = Float()
    write_bytes_rate = Float()

class CpuStat(Record):
    package_watts = Float()
    freq_ratio = Float() #ratio of actual to nominal CPU frequency
    cores_active_ratio = Float()
    gpu_active_ratio = Float() # Integrated graphics
    cores_average_num = Float()

class GpuStat(Record):
    hw_avg_freq = Float()
    gpu_busy_ratio = Float()

class SmcStat(Record):
    fan_speed_rpm = Float()
    cpu_die_temp_celsius = Float()
    gpu_die_temp_celsius = Float()

class SystemStat(Record):
    hw_model = String()
    efi_version = String()
    kern_osversion = String()
    kern_boottime = Integer()
    load_1min_avg = Float()
    load_5min_avg = Float()
    load_15min_avg = Float()

class ScriptState(Enum):
    """Status of this script execution."""
    STARTED = 1
    RUNNING = 2
    EXITED = 3
    
class ScriptInfo(Record):
    """Information about the state of the script"""
    state = ScriptState # Script state
    startup = Integer() # Epoch time that script began
    version = String() # version of this script
    
"""Run built-in powermetrics utility, and send relevant data to pulsar."""

interval_ms = 30*1000
cmd = ["/usr/bin/powermetrics", "--samplers",  "battery,network,disk,cpu_power,gpu_power,smc", "-f", "plist", "-i", str(interval_ms)]

def select_keys(dictionary, keys):
    """Return a dictionary with only the named keys."""
    return dict((k, dictionary[k]) for k in keys
                if k in dictionary)

def backlight_stats(sample):
    # 'backlight': {'value': 569, 'min': 0, 'max': 1024},
    b = sample.get('backlight')
    if b:
        bl = b['value'] / b['max']
        return BacklightStat(brightness=bl)
    else:
        return None

def network_stats(sample):
    n = sample.get('network')
    if n:
        return NetworkStat(output_packet_rate=n['opacket_rate'],
                           input_packet_rate=n['ipacket_rate'],
                           output_byte_rate=n['obyte_rate'],
                           input_byte_rate=n['ibyte_rate'])
    else:
        return None

def disk_stats(sample):
    d = sample.get('disk')
    if d:
        return DiskStat(read_ops_rate=d['rops_per_s'],
                        write_ops_rate=d['wops_per_s'],
                        read_bytes_rate=d['rbytes_per_s'],
                        write_bytes_rate=d['wbytes_per_s'])
    else:
        return None

def cpu_stats(sample):
    c = sample.get('processor')
    if c:
        toplevel = select_keys(c, ['package_watts', 'freq_ratio'])
        pkg = c['packages'][0]
        # No support for multi-socket
        toplevel.update(select_keys(pkg, ['cores_active_ratio', 'gpu_active_ratio', 'average_num_cores']))
        return CpuStat(package_watts=c['package_watts'],
                       freq_ratio=c['freq_ratio'],
                       cores_active_ratio=pkg['cores_active_ratio'],
                       cores_average_num=pkg['average_num_cores'],
                       gpu_active_ratio=pkg['gpu_active_ratio'])
    else:
        return None

def gpu_stats(sample):
    g = sample.get('GPU')
    # Only support single GPU
    if g:
        gpu = g[0]['misc_counters']
        # PLIST keys have extraneous whitespace and colons at the end,
        # so search for these by prefix:
        avg_active_prefix = 'HW average active frequency'
        busy_prefix = 'GPU Busy'
        avg_active = list({val for key, val in gpu.items()  
                   if key.startswith(avg_active_prefix)})[0]
        busy = list({val for key, val in gpu.items()  
                   if key.startswith(busy_prefix)})[0]
        return GpuStat(hw_avg_freq=avg_active,
                       gpu_busy_ratio=busy)
    else:
        return None

def smc_stats(sample):
    s = sample.get('smc')
    if s:
        return SmcStat(fan_speed_rpm=s['fan'],
                       cpu_die_temp_celsius=s['cpu_die'],
                       gpu_die_temp_celsius=s['gpu_die'])
    else:
        return None

def sys_stats(sample):
    sys_details = select_keys(sample, ['hw_model', 'efi_version', 'kern_osversion', 'kern_boottime'])
    # get system load
    load1, load5, load15 = os.getloadavg()
    sys_details["load_1min_avg"] = load1
    sys_details["load_5min_avg"] = load5
    sys_details["load_15min_avg"] = load15
    return SystemStat(hw_model=sample['hw_model'],
                      efi_version=sample['efi_version'],
                      kern_osversion=sample['kern_osversion'],
                      kern_boottime=sample['kern_boottime'],
                      load_1min_avg=load1,
                      load_5min_avg=load5,
                      load_15min_avg=load15)
    
def process_sample(sample):
    backlight = backlight_value(sample)
    print("backlight: {}".format(backlight_value(sample)))
    print("network: {}".format(network_stats(sample)))
    print("disk: {}".format(disk_stats(sample)))
    print("CPU: {}".format(cpu_stats(sample)))
    print("GPU: {}".format(gpu_stats(sample)))
    print("SMC: {}".format(smc_stats(sample)))
    print("SysInfo: {}".format(sys_stats(sample)))
    

def exit_handler(signum, frame):
    """Cleanly exit"""
    exit_record = ScriptInfo(state=ScriptState.EXITED, startup=STARTUP, version=VERSION)
    agent_producer.send(exit_record)
    sys.exit()

if __name__ == '__main__':
    VERSION = "0.0.1"
    STARTUP = int(time.time())
    signal.signal(signal.SIGINT, exit_handler)
    signal.signal(signal.SIGTERM, exit_handler)
    config = configparser.ConfigParser()
    config.read('osxstats2pulsar.conf')
    pcfg = config["pulsar"]
    pulsar_host = pcfg.get("PulsarHost", "pulsar://localhost:6550")
    auth_token = pcfg.get("AuthToken", None)
    # Topic names
    network_topic = pcfg.get("NetworkTopic")
    backlight_topic = pcfg.get("BacklightTopic")
    disk_topic = pcfg.get("DiskTopic")
    cpu_topic = pcfg.get("CpuTopic")
    gpu_topic = pcfg.get("GpuTopic")
    smc_topic = pcfg.get("SmcTopic")
    sys_topic = pcfg.get("SystemTopic")
    agent_topic = pcfg.get("AgentTopic")
    if auth_token:
        client = pulsar.Client(pulsar_host,
                               authentication=AuthenticationToken(auth_token))
    else:
        client = pulsar.Client(pulsar_host)
    network_producer = client.create_producer(network_topic, schema=AvroSchema(NetworkStat))
    backlight_producer = client.create_producer(backlight_topic, schema=AvroSchema(BacklightStat))
    disk_producer = client.create_producer(disk_topic, schema=AvroSchema(DiskStat))
    cpu_producer = client.create_producer(cpu_topic, schema=AvroSchema(CpuStat))
    gpu_producer = client.create_producer(gpu_topic, schema=AvroSchema(GpuStat))
    smc_producer = client.create_producer(smc_topic, schema=AvroSchema(SmcStat))
    sys_producer = client.create_producer(sys_topic, schema=AvroSchema(SystemStat))
    agent_producer = client.create_producer(agent_topic, schema=AvroSchema(ScriptInfo))
    startup_record = ScriptInfo(state=ScriptState.STARTED, startup=STARTUP, version=VERSION)
    agent_producer.send(startup_record)
    p = subprocess.Popen(cmd,
                         shell=False,
                         bufsize=0,
                         stdout=subprocess.PIPE)
    # Read each line until we see a NULL byte (\x00) in the first line.
    curr_record = []
    records_processed = 0
    for line in iter(p.stdout.readline, b''):
        #print('>>> {}'.format(line.rstrip()))
        line_split = line.split(b'\x00')
        null_count = len(line_split)-1
        curr_record.append(line_split[0])
        if null_count == 1:
            records_processed += 1
            # Create a full byte array of all lines
            full_record = b"".join(curr_record)
            # Carry over everything past the null byte into a new record
            curr_record = [line_split[1]]
            sample = plistlib.loads(full_record)
            #process_sample(sample)
            #network_topic.
            network_producer.send(network_stats(sample))
            backlight_producer.send(backlight_stats(sample))
            disk_producer.send(disk_stats(sample))
            cpu_producer.send(cpu_stats(sample))
            gpu_producer.send(gpu_stats(sample))
            smc_producer.send(smc_stats(sample))
            sys_producer.send(sys_stats(sample))
            if records_processed % 100 == 0:
                run_record = ScriptInfo(state=ScriptState.RUNNING, startup=STARTUP, version=VERSION)
                agent_producer.send(run_record)

