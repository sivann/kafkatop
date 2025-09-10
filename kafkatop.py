#!/usr/bin/env python

# Author: spiros ioannou 2023
# Calculate kafka consumer lag statistics to estimate system health
# 

import sys
import os
import signal
import pprint
import json
#import logging
import statistics
import re
import time
import datetime
import argparse
import humanize
import readchar
from threading import Thread
import signal
import fcntl
import struct

from confluent_kafka import (KafkaException, ConsumerGroupTopicPartitions,
                             TopicPartition, ConsumerGroupState, TopicCollection,
                             IsolationLevel)
from confluent_kafka.admin import (AdminClient, NewTopic, NewPartitions, ConfigResource,
                                   ConfigEntry, ConfigSource, AclBinding,
                                   AclBindingFilter, ResourceType, ResourcePatternType,
                                   AclOperation, AclPermissionType, AlterConfigOpType,
                                   ScramMechanism, ScramCredentialInfo,
                                   UserScramCredentialUpsertion, UserScramCredentialDeletion,
                                   OffsetSpec)

from rich.console import Console
from rich.table import Table
from rich.live import Live
from rich import box
from rich.panel import Panel
from rich.text import Text
from rich.align import Align
from rich.console import Group

VERSION='1.14'

# Global variables for keyboard handling
stop_program = False
sort_key = None
force_refresh = False
warnings = []  # Store warnings to display in the top panel
terminal_resized = False  # Flag to indicate terminal resize

def add_warning(message):
    """Add a warning message to be displayed in the top panel"""
    global warnings
    warnings.append(f"[yellow]{message}[/yellow]")
    # Keep only the last 10 warnings to avoid clutter
    if len(warnings) > 10:
        warnings = warnings[-10:]

def get_warnings_panel():
    """Create a panel with current warnings - only visible when there are warnings"""
    global warnings
    if not warnings:
        return None
    
    # Get terminal size for dynamic height calculation
    try:
        import shutil
        terminal_height = shutil.get_terminal_size().lines
        # Use 20% of terminal height for warnings panel, max 8 lines, min 3 lines
        max_warnings_height = max(3, min(8, int(terminal_height * 0.2)))
    except:
        max_warnings_height = 5  # fallback
    
    # Limit warnings to fit in the calculated height
    display_warnings = warnings[-max_warnings_height:] if len(warnings) > max_warnings_height else warnings
    
    warning_text = "\n".join(display_warnings)
    
    # Add indicator if there are more warnings than displayed
    if len(warnings) > max_warnings_height:
        warning_text += f"\n[yellow]... and {len(warnings) - max_warnings_height} more warnings[/yellow]"
    
    return Panel(
        warning_text,
        title="[bold red]Warnings[/bold red]",
        border_style="red",
        padding=(0, 1)
    )

def handle_terminal_resize(signum, frame):
    """Handle terminal resize signal"""
    global terminal_resized
    terminal_resized = True

def clear_old_warnings():
    """Clear warnings older than 30 seconds to prevent clutter"""
    global warnings
    # Keep warnings based on terminal size
    try:
        import shutil
        terminal_height = shutil.get_terminal_size().lines
        max_warnings = max(3, min(10, int(terminal_height * 0.15)))  # 15% of terminal height
    except:
        max_warnings = 5  # fallback
    
    if len(warnings) > max_warnings:
        warnings = warnings[-max_warnings:]

def show_final_state(iteration, kd, rates, console, generate_table_func, wait_for_input=True):
    """Show final state before exiting"""
    # Exit the live display context first
    console.clear()
    
    # Create final table without live updates
    final_table = generate_table_func(iteration, kd, rates)
    
    # Print final state
    console.print("\n[bold cyan]Displaying final state - exiting[/bold cyan]\n")
    console.print(final_table)
    
    if wait_for_input:
        console.print(f"\n[dim]Exited after {iteration} iterations. Press any key to continue...[/dim]")
        # Wait for user input before exiting
        try:
            import readchar
            readchar.readkey()
        except:
            pass
    else:
        console.print(f"\n[dim]Exited after {iteration} iterations.[/dim]")

def check_for_quit():
    """
    Runs in a separate thread, waiting for keyboard input.
    """
    global stop_program, sort_key, force_refresh
    while not stop_program:
        try:
            # This will block until a key is pressed
            key = readchar.readkey().lower()
            
            if key == 'q':
                stop_program = True
            elif key in ['g', 't', 'p', 'e', 'l', 'c']:
                if key == 'g':
                    sort_key = 'group'
                elif key == 't':
                    sort_key = 'topic'
                elif key == 'p':
                    sort_key = 'partitions'
                elif key == 'e':
                    sort_key = 'eta'
                elif key == 'l':
                    sort_key = 'lag'
                elif key == 'c':
                    sort_key = 'rate'
                force_refresh = True  # Trigger immediate refresh for sorting
        except (EOFError, KeyboardInterrupt):
            stop_program = True
            break


# a: kafka AdminClient instance
def describe_consumer_groups(a, group_ids):
    """
    Return consumer group assigned topics and partitions
    consumer_group_topics{'consumer_group_id':{'topic':[partitions,..]}}
    """
    futureMap = a.describe_consumer_groups(group_ids, include_authorized_operations=False, request_timeout=10)
    consumer_group_topics={}

    for group_id, future in futureMap.items():
        try:
            g = future.result()
            #print("Group Id: {}".format(g.group_id))
            if g.group_id not in consumer_group_topics:
                consumer_group_topics[g.group_id]={}
            for member in g.members:
                if member.assignment:
                    #print("    Assignments       :")
                    for toppar in member.assignment.topic_partitions:
                        #print("      {} [{}]".format(toppar.topic, toppar.partition))
                        if toppar.topic  not in consumer_group_topics[g.group_id]:
                            consumer_group_topics[g.group_id][toppar.topic]=[]
                        consumer_group_topics[g.group_id][toppar.topic].append(toppar.partition)
        except KafkaException as e:
            print("Error while describing group id '{}': {}".format(group_id, e))
        except Exception:
            raise
    return consumer_group_topics

def list_topics(params, topic_name=None):
    a=params['a']

    if topic_name:
        r = a.list_topics(topic=topic_name, timeout=30)
    else:
        r = a.list_topics(timeout=30)

    return r

""" consumer_groups: {group_ids: [group1,group2,..],
        properties:[ {group_id}={state:..., type:...}
        a: AdminClient instance
"""
def list_consumer_groups(a, params):
    consumer_groups={'ids':[], 'properties':{}}
    # states: https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.ConsumerGroupState
    s=[]
    states = {ConsumerGroupState[state] for state in s}
    future = a.list_consumer_groups(request_timeout=20, states=states)
    try:
        list_consumer_groups_result = future.result()
        #print("{} consumer groups".format(len(list_consumer_groups_result.valid)))
        for valid in list_consumer_groups_result.valid:
            #print("GROUP id: {} is_simple: {} state: {}".format(valid.group_id, valid.is_simple_consumer_group, valid.state))
            if params['kafka_group_filter_pattern']:
                if not re.search(params['kafka_group_filter_pattern'], valid.group_id):
                    continue
            if params['kafka_group_exclude_pattern'] and re.search(params['kafka_group_exclude_pattern'], valid.group_id):
                continue
            if params['kafka_show_empty_groups']==False and valid.state == ConsumerGroupState.EMPTY:
                continue
            consumer_groups['ids'].append(valid.group_id)
            consumer_groups['properties'][valid.group_id] = {}
            consumer_groups['properties'][valid.group_id]['state'] = valid.state
            consumer_groups['properties'][valid.group_id]['is_simple'] = valid.is_simple_consumer_group

        #print("{} errors".format(len(list_consumer_groups_result.errors)))
        if len(consumer_groups['ids']) == 0:
            print("No consumer groups found, try altering --group-exclude-pattern or --group-include-pattern, check defaults",file=sys.stderr)
            sys.exit(1)

        for error in list_consumer_groups_result.errors:
            print("    error: {}".format(error))
    except Exception:
        raise
    consumer_groups['ids'].sort()
    return  consumer_groups

def list_consumer_group_offsets(a, consumer_group_id):
    """
    List consumer group partition offsets per topic for a consumer group
    Returns
      "<group>": {
          "<topic>": {
                "partno1": offset1,
                "partno2": offset2,
    """

    topic_partitions = None
    groups = [ConsumerGroupTopicPartitions(consumer_group_id, topic_partitions)]
    futureMap = a.list_consumer_group_offsets(groups)
    group_offsets={}

    for group_id, future in futureMap.items():
        try:
            response_offset_info = future.result()
            #print("Group: " + response_offset_info.group_id)
            for topic_partition in response_offset_info.topic_partitions:
                if topic_partition.error:
                    print("    Error: " + topic_partition.error.str() + " occurred with " + topic_partition.topic + " [" + str(topic_partition.partition) + "]")
                else:
                    if topic_partition.topic not in group_offsets:
                        group_offsets[topic_partition.topic]={}
                    group_offsets[topic_partition.topic][topic_partition.partition]=topic_partition.offset
                    #print("    " + topic_partition.topic + " [" + str(topic_partition.partition) + "]: " + str(topic_partition.offset))

        except KafkaException as e:
            print("Failed to list {}: {}".format(group_id, e))
        except Exception:
            raise
    return group_offsets

def list_topic_offsets(a, topic, partitions):
    topic_partition_offsets = {}
    isolation_level=IsolationLevel.READ_COMMITTED
    offset_spec = OffsetSpec.latest()
    topic_offsets={}


    for pnum in partitions:
        topic_partition = TopicPartition(topic, pnum)
        topic_partition_offsets[topic_partition] = offset_spec

    futmap = a.list_offsets(topic_partition_offsets, isolation_level=isolation_level, request_timeout=30)
    for partition, fut in futmap.items():
        try:
            result = fut.result()
            #print("Topicname : {} Partition_Index : {} Offset : {} Timestamp : {}" .format(partition.topic, partition.partition, result.offset, result.timestamp))
            if partition.topic not in topic_offsets:
                topic_offsets[partition.topic]={}
            topic_offsets[partition.topic][partition.partition] = result.offset

        except KafkaException as e:
            # e.g. "Failed to query partition leaders: No leaders found"
            print("Topicname : {} Partition_Index : {} Error : {}" .format(partition.topic, partition.partition, e))
    return topic_offsets



def calc_lag(a, params):
    kd={} # Kafka data

    # Consumer groups and offsets
    kd['consumer_groups'] = list_consumer_groups(a, params)

    # Topics assigned to each Consumer group
    consumer_group_topics = describe_consumer_groups(a, kd['consumer_groups']['ids'])


    kd['group_offsets']={}
    for group in kd['consumer_groups']['ids']:
        kd['group_offsets'][group] = list_consumer_group_offsets(a, group);
    # 'consumer_roup': {'topic1': {partno: offset,..}, {topic2:
    kd['group_offsets_ts']=time.time()

    #
    # Compile a list of topics our consumer groups are associated with
    # topics_with_groups:
    #"<topic_name>": {
    #   "groups": [
    #     "<group_id>"
    #   ],
    #   "partitions": [0,1,..]
    kd['topics_with_groups']={}
    for group in kd['group_offsets']:
        if len(kd['group_offsets'][group].keys()):
            #topic = list(kd['group_offsets'][group].keys())[0] #1st topic
            for topic in list(kd['group_offsets'][group].keys()):
                if topic not in kd['topics_with_groups']:
                    kd['topics_with_groups'][topic]={}
                    kd['topics_with_groups'][topic]['groups']=[]
                    partitions = list(kd['group_offsets'][group][topic].keys())
                    kd['topics_with_groups'][topic]['partitions'] = partitions
                kd['topics_with_groups'][topic]['groups'].append(group)
            
        else:
            add_warning(f'No offsets for group (never committed data): {group}') # can happen if never had data
            continue


    # latest offset of each topic of those consumed by consumer groups
    # topic_offsets:  '<topic_name>': {part_id: offset}
    kd['topic_offsets']={}
    for topic in kd['topics_with_groups']:
        partitions =  kd['topics_with_groups'][topic]['partitions']
        topic_offsets_perpart = list_topic_offsets(a, topic, partitions)
        kd['topic_offsets'].update(topic_offsets_perpart)
    kd['topic_offsets_ts']=time.time()
    #pprint.pprint(kd['topic_offsets'])


    #pprint.pprint(kd['group_offsets'])
    # Now we have topics + partition offsets in kd['topic_offsets'] and 
    # consumergroup+topic partition consumer offsets in kd['group_offsets'].
    # We can now calculate lag statistics 
    kd['group_lags']={}
    for group in kd['group_offsets']:
        if not len(kd['group_offsets'][group].keys()):
            continue
        kd['group_lags'][group]={}
        #topic = list(kd['group_offsets'][group].keys())[0] # first key, first topic, we only care for one
        for topic in  list(kd['group_offsets'][group].keys()):
            tos = kd['topic_offsets'][topic] # topic latest offsets per partition: '{0: 1234, 1:1234 ..]' 
            gos = kd['group_offsets'][group][topic] # consumer group offsets per topic partition

            lags=[] # offset lag per partition
            part_lag = {}
            for part in tos:
                to = tos[part]
                if part not in gos:
                    #print(f"Warning: partition {part} not part of ConsumerGroup {group} for topic {topic}, no data there?") 
                    # Can happen if part never had data from last app start, ignore offset
                    continue
                go = gos[part]
                lag = to - go
                part_lag[part]=lag
                lags.append(lag)
                #print(to,go,to-go)

            kd['group_lags'][group][topic]={}
            kd['group_lags'][group][topic]['partlags']=part_lag
            kd['group_lags'][group][topic]['topic']=topic
            kd['group_lags'][group][topic]['max'] = max(lags) if len(lags) else 0 # TODO handle empty data here
            kd['group_lags'][group][topic]['sum'] = sum(lags) if len(lags) else 0
            kd['group_lags'][group][topic]['mean'] = statistics.mean(lags) if len(lags) else 0
            kd['group_lags'][group][topic]['median'] = statistics.median(lags) if len(lags) else 0
            kd['group_lags'][group][topic]['min'] = min(lags) if len(lags) else 0
            #if len(lags):
            #    print(f'Group: {group:<40}, topic:{topic:<20}, parts:{len(tos):5}, LAG mean: {statistics.mean(lags):10.1f}, median: {statistics.median(lags)}')
            #else:
            #    print(f'Group: {group:<40}, topic:{topic:<20}, parts:{len(tos):5}, LAG mean: -, median: -')
    return kd
        

# Calclate consumption rate
# Consumed 1021 evts in 5 seconds, 204 evts/second remaining: 0 h, 0 sec
def calc_rate(kd1, kd2):
    rates={}
    for g in kd1['group_offsets']:
        rates[g]={}
        events_consumption_rate=None
        for t in kd1['group_offsets'][g]: # t: topic. Probably only one topic in consumergroup
            #print(f'Topic:{t}, group:{g}')

            if g not in kd2['group_offsets'] or g not in  kd1['group_offsets']:
                add_warning(f"Group {g} disappeared, skipping")
                continue # group disappeared
            po1 = kd1['group_offsets'][g][t] # part offsets
            po2 = kd2['group_offsets'][g][t] # part offsets
            po1_sum = sum(po1.values())
            po2_sum = sum(po2.values())
            events_consumed = po2_sum - po1_sum
            time_delta =  kd2['group_offsets_ts'] - kd1['group_offsets_ts']
            if time_delta == 0:
                add_warning(f"Time delta is 0 for group {g} and topic {t}, skipping")
                continue
            events_consumption_rate = round(events_consumed/time_delta,3) # messages per second 

            if t not in kd2['topic_offsets']:
                add_warning(f"Topic {t} not found in group data {g}: not supported multiple topics per consumer group, skipping")
                continue

            events_arrived = sum(kd2['topic_offsets'][t].values()) - sum(kd1['topic_offsets'][t].values())  # Diff of sum of topic offsets on all partitions 
            events_arrival_rate = round(events_arrived/time_delta,3) # messages per second 

            if events_consumption_rate > 0:
                remaining_sec =  int(kd2['group_lags'][g][t]['sum']) / events_consumption_rate
                rem_hms = str(datetime.timedelta(seconds=round(remaining_sec)))
            else:
                remaining_sec =  -1
                rem_hms="-"
        
            #print(f"{g:45} consumed {events_consumed:8} evts in {time_delta:3.1f}s, {events_consumption_rate:7.1f} evts/sec, remaining: {rem_hms:8}")

            if events_consumption_rate != None:
                rates[g][t]={'events_consumed': events_consumed, 'time_delta':time_delta, 'events_consumption_rate': events_consumption_rate, 
                 'events_arrival_rate': events_arrival_rate, 'remaining_sec':remaining_sec, 'rem_hms':rem_hms}
    return rates
        

def lag_show_text(params):
    a = params['a']
    kd = calc_lag(a, params)
    for g in kd['group_lags']:
        state = kd['consumer_groups']['properties'][g]['state']
        for t in kd['group_lags'][g]:
            parts_total = topic_nparts(params, t)
            print(f"Group: {g:<45}, topic: {t:20}, partitions:{len(kd['group_lags'][g][t]['partlags'].keys()):5}/{parts_total:5}, State: {state:30}, LAG min: {kd['group_lags'][g][t]['min']:10.1f}, max: {kd['group_lags'][g][t]['max']:10.1f}, median: {kd['group_lags'][g][t]['median']}")
 
    if params['kafka_poll_iterations'] == 0:
        sys.exit(0)
    time.sleep(params['kafka_poll_period'])
    kd1 = kd
    iteration=0
    while True:
        print("")
        iteration += 1

        kd2 = calc_lag(a, params)
        rates = calc_rate(kd1, kd2) 
        kd1 = kd2
        for g in rates:
            for t in rates[g]:
                if 'events_consumed' not in rates[g][t]:
                    #print(g,':no stats')
                    continue
                print(f"{g:45} consumed {rates[g][t]['events_consumed']:8} evts in {rates[g][t]['time_delta']:3.1f}s,"
                f"{rates[g][t]['events_consumption_rate']:10.1f} cons evts/sec,"
                f"{rates[g][t]['events_arrival_rate']:10.1f} new evts/sec,"
                f" remaining: {rates[g][t]['rem_hms']:8}")
        if iteration==params['kafka_poll_iterations'] and params['kafka_poll_iterations']>0:
            sys.exit(0)
        time.sleep(params['kafka_poll_period'])
 
 
# Evaluate health of consumption stats per topic
# Input:
#  lag: lag sum of partition
#  group: group_id (name)
#  rate dict of group
#
# Returns: sc (status), st (status text) dicts with keys: eta, lag, rate
#
def lag_health(group, lag, rate):
    sc={} # color status
    st={} # text status


    rs = rate['remaining_sec']
    if rs < 60:
        sc['eta']='[bold green]'
        st['eta']={'status': 'OK', 'reason':'ETA < 1m'}
    elif rs < 120:
        sc['eta']='[bold yellow]'
        st['eta']={'status': 'OK', 'reason':'ETA < 2m'}
    elif rs < 600:
        sc['eta']='[bold yellow]'
        st['eta']={'status': 'WARNING', 'reason':'ETA > 2m'}
    elif rs < 7200:
        sc['eta']='[bold magenta]'
        st['eta']={'status': 'ERROR', 'reason':'ETA > 10m'}
    else:
        sc['eta']='[bold red]'
        st['eta']={'status': 'CRITICAL', 'reason':'ETA > 2h'}

    # Lag and rate
    sc['rate']='[green]'
    st['rate']='OK'
    st['rate']={'status': 'OK', 'reason':''}
    sc['lag']='[white]'
    st['lag']={'status': 'OK', 'reason':''}

    if lag>0 and rate['events_consumption_rate'] == 0: #events_consumption_rate: consumption rate per second
        sc['rate']='[bold red]'
        st['rate']={'status': 'ERROR', 'reason':'Lag detected but not event consumption'}
    elif rate['events_arrival_rate'] > 5 * rate['events_consumption_rate']:
        sc['rate']='[bold red]'
        st['rate']={'status': 'ERROR', 'reason':'Arrival rate > 5 * consumption rate'}
    elif rate['events_arrival_rate'] > 2 * rate['events_consumption_rate']:
        sc['rate']='[bold yellow]'
        st['rate']={'status': 'WARNING', 'reason':'Arrival rate > 2 * consumption rate'}

    return st, sc

def lag_show_status(params):
    a = params['a']
    kd1 = calc_lag(a, params)
    time.sleep(params['kafka_poll_period'])
    kd2 = calc_lag(a, params)
    rates = calc_rate(kd1, kd2) 
    #print(json.dumps(rates,indent=2))

    gst={}
    for g in rates:
        for t in rates[g]:
            if 'events_consumed' not in rates[g][t]:
                continue
            lag = kd2['group_lags'][g][t]['sum']
            st, s = lag_health(g, lag, rates[g][t])
            gst[f"{g}-{t}"]=st
    print(json.dumps(gst,indent=2))



def show_summary_json(params):
    a = params['a']
    kd = calc_lag(a, params)
    summary={}
    for g in kd['group_lags']:
        state = f"{kd['consumer_groups']['properties'][g]['state']}"
        summary[g]={}
        summary[g]['state']=state
        summary[g]['topics']={}
        for t in kd['group_lags'][g]:
            parts_total = topic_nparts(params, t)
            summary[g]['topics'][t]={
                #"group": g,
                "partitions": len(kd['group_lags'][g][t]['partlags'].keys()),
                "lag_max": kd['group_lags'][g][t]['max'],
                "lag_min": kd['group_lags'][g][t]['min']
            }
    print(json.dumps(summary,indent=2))
 

def lag_show_rich(params):
    global stop_program, force_refresh, terminal_resized
    
    # Save original terminal state for restoration
    original_termios = None
    try:
        import termios
        import sys
        if hasattr(sys.stdin, 'fileno') and sys.stdin.isatty():
            fd = sys.stdin.fileno()
            original_termios = termios.tcgetattr(fd)
    except:
        pass
    a = params['a']
    kd = calc_lag(a, params)

    if params['kafka_summary']:
        table1 = Table(title="Initial Lag summary", show_lines=False)
        table1.add_column("Consumer Group", justify="left", style="cyan", no_wrap=True)
        table1.add_column("Topic", style="cyan")
        table1.add_column("Partitions\n(with groups/total)", justify="right", style="green")
        table1.add_column("Lag (part median)", justify="right", style="green")
        table1.add_column("Consumer Group State", justify="left", style="green")
        for g in kd['group_lags']:
            for t in kd['group_lags'][g]:
                state = kd['consumer_groups']['properties'][g]['state']
                if state == ConsumerGroupState.EMPTY:
                    stc="[red]"
                else:
                    stc="[green]"
                topic_name = kd['group_lags'][g][t]['topic']
                parts_with_consumers = str(len(kd['group_lags'][g][t]['partlags'].keys()))
                parts_total = topic_nparts(params, topic_name)

                table1.add_row(g, topic_name,
                    f"{parts_with_consumers}/{parts_total}",
                    str(kd['group_lags'][g][t]['median']),
                    f"{stc}{state}[/]"
                    )
        console = Console()
        console.print(table1)
        print("")

    if params['kafka_poll_iterations'] == 0:
        sys.exit(0)

    def generate_table(iteration, kd, rates):
        global sort_key
        dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Add keyboard shortcuts to caption
        caption = "Legend: [cyan]INFO[/] [green]OK[/] [yellow]WARN[/] [magenta]ERR[/] [red]CRIT[/] | Keys: [green]Q[/green]uit, Sort By: [green]G[/green]roup, [green]T[/green]opic, [green]P[/green]artitions, [green]E[/green]TA, [green]L[/green]ag, [green]C[/green]onsumed"
        if sort_key:
            caption += f" | Sorted by: [bold]{sort_key}[/] (highlighted column)"
            
        table = Table(title=f"Lags and Rates\n[bold cyan]Last poll: {dt}, poll period: {params['kafka_poll_period']}s, poll: \[{iteration}]", 
        show_lines=False, 
        box=box.SIMPLE_HEAD,
        caption=caption,
        caption_style="bold bright_white")

        # Create headers with highlighted first letters and reverse video for sorted column
        if sort_key == 'group':
            group_header = "[reverse bold bright_green]G[/reverse bold bright_green]roup"
        else:
            group_header = "[bold bright_green]G[/bold bright_green]roup"
            
        if sort_key == 'topic':
            topic_header = "[reverse bold bright_green]T[/reverse bold bright_green]opic"
        else:
            topic_header = "[bold bright_green]T[/bold bright_green]opic"
            
        if sort_key == 'partitions':
            partitions_header = "[reverse bold bright_green]P[/reverse bold bright_green]artitions"
        else:
            partitions_header = "[bold bright_green]P[/bold bright_green]artitions"
            
        since_header = "Since\n(sec)"
        events_header = "Events\nConsumed"
        new_rate_header = "New topic\nevts/sec"
        
        if sort_key == 'rate':
            consumed_rate_header = "[reverse bold bright_green]C[/reverse bold bright_green]onsumed\nevts/sec"
        else:
            consumed_rate_header = "[bold bright_green]C[/bold bright_green]onsumed\nevts/sec"
            
        if sort_key == 'eta':
            eta_header = "[reverse bold bright_green]E[/reverse bold bright_green]st. time\nto consume"
        else:
            eta_header = "[bold bright_green]E[/bold bright_green]st. time\nto consume"
            
        if sort_key == 'lag':
            lag_header = "[reverse bold bright_green]L[/reverse bold bright_green]ag"
        else:
            lag_header = "Total Lag"

        table.add_column(group_header, justify="left", style="cyan", no_wrap=True)
        table.add_column(topic_header, style="cyan")
        table.add_column(partitions_header, style="cyan")
        table.add_column(since_header, justify="right", style="green")
        table.add_column(events_header, justify="right",  style="green")
        table.add_column(new_rate_header, justify="right", style="green")
        table.add_column(consumed_rate_header, justify="right", style="green")
        table.add_column(eta_header, justify="right", style="green")
        table.add_column(lag_header, justify="right", style="magenta")

        # Collect all rows first for sorting
        rows_data = []
        for g in rates:
            for t in rates[g]:
                state = kd['consumer_groups']['properties'][g]['state']
                if 'events_consumed' not in rates[g][t]:
                    continue
                lag = kd['group_lags'][g][t]['sum']  # Fixed: was kd2, should be kd
                st, s = lag_health(g, lag, rates[g][t])

                # Highlight entire row if a cell has issues
                # More colors: https://rich.readthedocs.io/en/stable/appendix/colors.html#appendix-colors
                if 'status' in st['eta'] and st['eta']['status'] != 'OK':
                    row_style='on dark_red' #also: gray23 ,  reverse
                else:
                    row_style=None
                    if params['kafka_only_issues']: # don't display ok rows
                        continue

                if args.anonymize:
                    g1 = f"group {abs(hash(g)) % (10 ** 6):6}"
                    t1 = f"topic {abs(hash(t)) % (10 ** 6):6}"
                else:
                    g1=g
                    t1=t

                # Store row data for sorting
                row_data = {
                    'group': g1,
                    'topic': t1,
                    'partitions': len(kd['group_lags'][g][t]['partlags'].keys()),
                    'time_delta': rates[g][t]['time_delta'],
                    'events_consumed': rates[g][t]['events_consumed'],
                    'events_arrival_rate': rates[g][t]['events_arrival_rate'],
                    'events_consumption_rate': rates[g][t]['events_consumption_rate'],
                    'rem_hms': rates[g][t]['rem_hms'],
                    'lag_sum': kd['group_lags'][g][t]['sum'],
                    'rate_style': s['rate'],
                    'eta_style': s['eta'],
                    'lag_style': s['lag'],
                    'row_style': row_style,
                    'sort_group': g,  # For sorting by original group name
                    'sort_topic': t   # For sorting by original topic name
                }
                rows_data.append(row_data)

        # Sort rows based on sort_key
        if sort_key == 'group':
            rows_data.sort(key=lambda x: x['sort_group'])
        elif sort_key == 'topic':
            rows_data.sort(key=lambda x: x['sort_topic'])
        elif sort_key == 'partitions':
            rows_data.sort(key=lambda x: x['partitions'], reverse=True)
        elif sort_key == 'eta':
            # Sort by remaining time (convert to seconds for comparison)
            def eta_sort_key(x):
                eta_str = x['rem_hms']
                if eta_str == '-':
                    return float('inf')  # Put at end
                try:
                    # Parse time format like "0:00:05" or "1:23:45"
                    parts = eta_str.split(':')
                    if len(parts) == 3:
                        return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
                    return 0
                except:
                    return float('inf')
            rows_data.sort(key=eta_sort_key, reverse=True)
        elif sort_key == 'lag':
            rows_data.sort(key=lambda x: x['lag_sum'], reverse=True)
        elif sort_key == 'rate':
            rows_data.sort(key=lambda x: x['events_consumption_rate'], reverse=True)

        # Add sorted rows to table
        for row_data in rows_data:
            table.add_row(
                row_data['group'],
                row_data['topic'],
                str(row_data['partitions']),
                f"{row_data['time_delta']:.2f}",
                f"{humanize.metric(row_data['events_consumed'])}", 
                f"{humanize.metric(row_data['events_arrival_rate'])}", 
                f"{row_data['rate_style']}{humanize.metric(row_data['events_consumption_rate'])}", 
                f"{row_data['eta_style']}{row_data['rem_hms']}", 
                f"{row_data['lag_style']}{humanize.metric(row_data['lag_sum'])}",
                style=row_data['row_style']
            )
        
        # Create a group with warnings panel (if any) and the table
        components = []
        
        # Add warnings panel if there are any warnings
        warnings_panel = get_warnings_panel()
        if warnings_panel:
            components.append(warnings_panel)
        
        # Add the main table
        components.append(table)
        
        # Return a Group containing all components
        return Group(*components)


    iteration=0
    print("Please wait, calculating initial rates...")
    kd1 = calc_lag(a, params)
    #time.sleep(params['kafka_poll_period'])
    time.sleep(3) 
    kd2 = calc_lag(a, params)
    rates = calc_rate(kd1, kd2) 

    # Set up terminal resize signal handler
    signal.signal(signal.SIGWINCH, handle_terminal_resize)
    
    # Start the background thread to listen for keyboard input
    key_thread = Thread(target=check_for_quit, daemon=True)
    key_thread.start()

    table = generate_table(iteration, kd2, rates)
    #console.print(table)
    
    # Configure console for better terminal compatibility and reduced flickering
    import os
    # Set environment variables to help with terminal detection
    os.environ.setdefault('TERM', 'xterm-256color')
    os.environ.setdefault('COLORTERM', 'truecolor')
    
    console = Console(force_terminal=True, legacy_windows=False, width=None, height=None)
    live = Live(table, refresh_per_second=0.5, screen=True, console=console, 
                auto_refresh=False)  # Disable auto-refresh to control updates manually
    
    with live: # Live only works in with..
        while not stop_program:
            kd2 = calc_lag(a, params)
            rates = calc_rate(kd1, kd2) 
            kd1 = kd2
            iteration += 1
            clear_old_warnings()  # Clean up old warnings
            
            # Check for terminal resize and force refresh if needed
            if terminal_resized:
                terminal_resized = False
                # Force a complete refresh of the console
                console.clear()
            
            live.update(generate_table(iteration, kd2, rates))
            live.refresh()  # Manual refresh for better control

            if iteration==params['kafka_poll_iterations'] and params['kafka_poll_iterations']>0:
                # Exit the live context first, then show final state
                break
            
            # Sleep in small chunks to allow immediate quit response and sorting updates
            sleep_time = params['kafka_poll_period']
            sleep_chunk = 0.1  # Check for quit every 100ms
            while sleep_time > 0 and not stop_program:
                time.sleep(min(sleep_chunk, sleep_time))
                sleep_time -= sleep_chunk
                
                # Check if we need to refresh immediately (for sorting or terminal resize)
                if force_refresh or terminal_resized:
                    if terminal_resized:
                        terminal_resized = False
                        console.clear()  # Clear console on resize
                    if force_refresh:
                        force_refresh = False
                    live.update(generate_table(iteration, kd2, rates))
                    live.refresh()  # Manual refresh for sorting updates or resize
                    break  # Exit the sleep loop to refresh immediately
    
    # Show final state when quitting with 'q' or reaching max iterations
    if stop_program:
        show_final_state(iteration, kd2, rates, console, generate_table, wait_for_input=False)
    elif iteration == params['kafka_poll_iterations'] and params['kafka_poll_iterations'] > 0:
        show_final_state(iteration, kd2, rates, console, generate_table, wait_for_input=False)
    
    # Restore terminal state to prevent hidden characters
    try:
        import termios
        import sys
        if original_termios and hasattr(sys.stdin, 'fileno') and sys.stdin.isatty():
            fd = sys.stdin.fileno()
            # Restore original terminal settings
            termios.tcsetattr(fd, termios.TCSADRAIN, original_termios)
    except:
        pass
    
    print("Program finished.")


def init_conf(args):

    params={}
    broker=args.kafka_broker

    a = AdminClient({'bootstrap.servers': broker})
    params['a'] = a

    if args.kafka_group_exclude_pattern is not None and len(args.kafka_group_exclude_pattern):
        params['kafka_group_exclude_pattern']=re.compile(args.kafka_group_exclude_pattern)
    else:
        params['kafka_group_exclude_pattern']=None

    if args.kafka_group_filter_pattern is not None and len(args.kafka_group_filter_pattern):
        params['kafka_group_filter_pattern']=re.compile(args.kafka_group_filter_pattern) 
    else:
        params['kafka_group_filter_pattern']=None

    params['kafka_poll_period'] = int(args.kafka_poll_period)
    params['kafka_poll_iterations'] = int(args.kafka_poll_iterations)
    params['kafka_summary'] = int(args.kafka_summary)
    params['kafka_summary_json'] = int(args.kafka_summary_json)
    params['kafka_show_empty_groups'] = int(args.kafka_show_empty_groups)
    params['kafka_only_issues'] = int(args.kafka_only_issues)

    return params


def signal_handler(signal, frame):
    sys.exit(0)

# --info
def show_kafka_topicinfo(params):
    info={
        'topics':{},
        'brokers':{},
        'broker_name':'',
        'cluster_id':'',
    }

    cinfo = list_topics(params)

    for t in cinfo.topics:
        tname=t
        tparts=cinfo.topics[tname].partitions
        pkeys = tparts.keys()

        if args.kafka_topicinfo_parts:
            partinfo=[]
            for pk in pkeys:
                partinfo.append({'id': tparts[pk].id, 'leader': tparts[pk].leader, 'replicas': tparts[pk].replicas, 'nisrs': len(tparts[pk].isrs)})
        
            info['topics'][tname] = {'name':tname, 'partitions':len(pkeys), 'partinfo':partinfo}
        else:
            info['topics'][tname] = {'name':tname, 'partitions':len(pkeys)}

    for b in cinfo.brokers:
        bid=b
        bhost=cinfo.brokers[bid].host
        bport=cinfo.brokers[bid].port
        info['brokers'][bid]={'id':bid, 'host':bhost, 'port':bport}
    info['broker_name']=cinfo.orig_broker_name
    info['cluster_id']=cinfo.cluster_id
    print(json.dumps(info,indent=2))



    
topic2nparts={}
#populate and cahce topic2nparts
def topic_nparts(params, tname):
    global topic2nparts

    # populate cache for all topics
    if len(topic2nparts) == 0:
        cinfo = list_topics(params)
        for t in cinfo.topics:
            tparts=cinfo.topics[t].partitions
            nparts=len(tparts)
            topic2nparts[t]=nparts
    nparts = topic2nparts[tname]
    return nparts



if __name__ == '__main__':

    argparser = argparse.ArgumentParser(description='Kafka consumer statistics', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    argparser.add_argument('--kafka-broker', dest='kafka_broker', help='Broker address (host:port)', required = False, default='localhost' )
    argparser.add_argument('--text', dest='text', help='Disable rich text and color', required = False, default=False, action='store_true' )
    argparser.add_argument('--poll-period', dest='kafka_poll_period', help='Poll interval (sec) for rate calculations', required = False, default=5)
    argparser.add_argument('--poll-iterations', dest='kafka_poll_iterations', help='Refresh count before exiting (-1 for infinite)', required = False, default=15)
    argparser.add_argument('--group-exclude-pattern', dest='kafka_group_exclude_pattern', help='Exclude groups matching regex', required = False, default=None )# default='_[0-9]+$')
    argparser.add_argument('--group-filter-pattern', dest='kafka_group_filter_pattern', help='Filter groups by regex', required = False, default=None)
    argparser.add_argument('--status', dest='kafka_status', help='Report health as JSON and exit.', required = False, action='store_true')
    argparser.add_argument('--summary', dest='kafka_summary', help='Display consumer groups, states, topics, partitions, and lags summary.', default=False, required = False, action='store_true')
    argparser.add_argument('--summary-json', dest='kafka_summary_json', help='Display consumer groups, states, topics, partitions, and lags summary, in JSON and exit.', default=False, required = False, action='store_true')
    argparser.add_argument('--topicinfo', dest='kafka_topicinfo', help='Show topic metadata only (fast).', default=False, required = False, action='store_true')
    argparser.add_argument('--topicinfo-parts', dest='kafka_topicinfo_parts', help='Show topic and partition metadata', default=False, required = False, action='store_true')
    argparser.add_argument('--only-issues', dest='kafka_only_issues', help='Show only groups with high lag/issues.', default=False, required = False, action='store_true')
    argparser.add_argument('--anonymize', dest='anonymize', help='Anonymize topic and group names.', default=False, required = False, action='store_true')
    argparser.add_argument('--all', dest='kafka_show_empty_groups', help='Show all groups (including those with no members).', default=False, required = False, action='store_true')
    argparser.add_argument('--version', action='version', version=f'%(prog)s {VERSION}')
    args = argparser.parse_args()

    params = init_conf(args)
    signal.signal(signal.SIGINT, signal_handler)


    if args.kafka_topicinfo or args.kafka_topicinfo_parts:
        show_kafka_topicinfo(params)
        sys.exit(0)

    if args.kafka_status:
        lag_show_status(params)
        sys.exit(0)

    if args.kafka_summary_json:
        show_summary_json(params)
    elif args.text:
        lag_show_text(params)
    else:
        lag_show_rich(params)

