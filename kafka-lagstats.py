#!/usr/bin/env python

# Author: spiros.ioannou 2023
# Calculate kafka consumer lag statistics to estimate system health
# 

import sys
import os
import signal
import pprint
import json
import logging
import statistics
import random
import re
import time
import datetime
import argparse
import humanize

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

VERSION='%version%'



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

""" consumer_groups: {group_ids: [group1,group2,..],
        properties:[ {group_id}={state:..., type:...}
"""
def list_consumer_groups(a, params):
    consumer_groups={'ids':[], 'properties':{}}
    # states: https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.ConsumerGroupState
    s=[]
    states = {ConsumerGroupState[state] for state in s}
    future = a.list_consumer_groups(request_timeout=10, states=states)
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
            #print('ERROR: no topics for group',group) # can happen if never had data
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

            po1 = kd1['group_offsets'][g][t] # part offsets
            po2 = kd2['group_offsets'][g][t] # part offsets
            po1_sum = sum(po1.values())
            po2_sum = sum(po2.values())
            events_consumed = po2_sum - po1_sum
            time_delta =  kd2['group_offsets_ts'] - kd1['group_offsets_ts']
            events_consumption_rate = round(events_consumed/time_delta,3) # messages per second 

            if t not in kd2['topic_offsets']:
                print(f"WARNING: Topic {t} not found in group data {g}: not supported  multiple topics per consumer group, skipping")
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
        for t in kd['group_lags'][g]:
            print(f"Group: {g:<45}, topic:{t:20}, partitions:{len(kd['group_lags'][g][t]['partlags'].keys()):5}, LAG min: {kd['group_lags'][g][t]['min']:10.1f}, max: {kd['group_lags'][g][t]['max']:10.1f}, median: {kd['group_lags'][g][t]['median']}")
 
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
        time.sleep(params['kafka_poll_period'])
        if iteration==params['kafka_poll_iterations']:
            sys.exit(0)
 
 
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
        st['eta']='OK'
    elif rs < 120:
        sc['eta']='[bold yellow]'
        st['eta']={'status': 'OK', 'reason':'ETA > 1m'}
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

    gst={}
    for g in rates:
        for t in rates[g]:
            if 'events_consumed' not in rates[g]:
                continue
            lag = kd2['group_lags'][g][t]['sum']
            st, s = lag_health(g, lag, rates[g][t])
            gst[f"{g}-{t}"]=st
    print(json.dumps(gst,indent=2))



def lag_show_rich(params):
    a = params['a']
    kd = calc_lag(a, params)

    if not params['kafka_noinitial']:
        table1 = Table(title="Initial Lag summary", show_lines=False)
        table1.add_column("Group", justify="left", style="cyan", no_wrap=True)
        table1.add_column("Topic", style="cyan")
        table1.add_column("Partitions", justify="right", style="green")
        table1.add_column("Lag (part median)", justify="right", style="green")
        table1.add_column("Group State", justify="left", style="green")
        for g in kd['group_lags']:
            for t in kd['group_lags'][g]:
                state = kd['consumer_groups']['properties'][g]['state']
                if state == ConsumerGroupState.EMPTY:
                    stc="[red]"
                else:
                    stc="[green]"
                table1.add_row(g,kd['group_lags'][g][t]['topic'] , 
                    str(len(kd['group_lags'][g][t]['partlags'].keys())), 
                    str(kd['group_lags'][g][t]['median']),
                    f"{stc}{state}[/]"
                    )
        console = Console()
        console.print(table1)
        print("")


    def generate_table(iiteration, kd, rates) -> Table:
        #print('GTable, iteration:',iteration)
        dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        table = Table(title=f"Lags and Rates\n[bold cyan]Last poll: {dt}, poll period: {params['kafka_poll_period']}s, poll: \[{iteration}]", 
        show_lines=False, 
        box=box.SIMPLE_HEAD,
        caption="Legend: [cyan]INFO[/] [bold  green]OK[/] [bold yellow]WARN[/] [bold magenta]ERR[/] [bold red]CRIT[/]")

        table.add_column("Group", justify="left", style="cyan", no_wrap=True)
        table.add_column("Topic", style="cyan")
        table.add_column("Since\n(sec)", justify="right", style="green")
        table.add_column("Events\nConsumed", justify="right",  style="green")
        table.add_column("New topic\nevts/sec", justify="right", style="green")
        table.add_column("Consumed\nevts/sec", justify="right", style="green")
        table.add_column("Est. time\nto consume", justify="right", style="green")
        table.add_column("Total Lag", justify="right", style="magenta")

        for g in rates:
            for t in rates[g]:
                state = kd['consumer_groups']['properties'][g]['state']
                if 'events_consumed' not in rates[g][t]:
                    continue
                lag = kd2['group_lags'][g][t]['sum']
                st, s = lag_health(g, lag, rates[g][t])

                # Highlight entire row if a cell has issues
                # More colors: https://rich.readthedocs.io/en/stable/appendix/colors.html#appendix-colors
                if st['eta'] != 'OK':
                    row_style='on dark_red' #also: gray23 ,  reverse
                else:
                    row_style=None
                    if params['kafka_only_issues']: # don't display ok rows
                        continue


                g1=g.replace("unity","c1")
                g1=g1.replace("src","datasrc")
                g1=g1.replace("historian","importer")
                g1=g1.replace("ds","rd")
                g1=g1.replace("stng","fastreader")
                g1=g1.replace("downstream","export")
                g1=g1.replace("svar","slowreader")
                g1=g1.replace("Evts","_event")
                g1=g1.replace("us","wr")
                g1=g1.replace("collaborative","cgroup")
                t =  kd2['group_lags'][g][t]['topic']
                t1=t.replace("unity","t1")
                t1=t1.replace("historian","import")
                t1=t1.replace("Historian","Importer")
                t1=t1.replace("src","id_")
                t1=t1.replace("Evts","event")
                t1=t1.replace("Svar","floats")
                t1=t1.replace("Ds","incoming")
                t1=t1.replace("Stng","outgoing")
                t1=t1.replace("ngst","import")
                table.add_row(g1,  t1,
                    f"{rates[g][t]['time_delta']:.2f}",
                    f"{humanize.metric(rates[g][t]['events_consumed'])}", 
                    f"{humanize.metric(rates[g][t]['events_arrival_rate'])}", 
                    f"{s['rate']}{humanize.metric(rates[g][t]['events_consumption_rate'])}", 
                    f"{s['eta']}{rates[g][t]['rem_hms']}", 
                    f"{s['lag']}{humanize.metric(kd['group_lags'][g][t]['sum'])}",
                    style=row_style
                )
                #print(f"{g:45} consumed {rates[g]['events_consumed']:8} evts in {rates[g]['time_delta']:3.1f}s,"
        return table


    iteration=0
    print("Please wait, calculating initial rates...")
    kd1 = calc_lag(a, params)
    #time.sleep(params['kafka_poll_period'])
    time.sleep(3) 
    kd2 = calc_lag(a, params)
    rates = calc_rate(kd1, kd2) 

    table = generate_table(iteration, kd2, rates)
    #console.print(table)
    live = Live(table, refresh_per_second=1)
    
    with live: # Live only works in with..
        while True:
            kd2 = calc_lag(a, params)
            rates = calc_rate(kd1, kd2) 
            kd1 = kd2
            iteration += 1
            time.sleep(params['kafka_poll_period'])
            live.update(generate_table(iteration, kd2, rates))

            if iteration == params['kafka_poll_iterations']:
                sys.exit(0)


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
    params['kafka_noinitial'] = int(args.kafka_noinitial)
    params['kafka_show_empty_groups'] = int(args.kafka_show_empty_groups)
    params['kafka_only_issues'] = int(args.kafka_only_issues)

    return params


def signal_handler(signal, frame):
    sys.exit(0)


if __name__ == '__main__':

    argparser = argparse.ArgumentParser(description='Kafka consumer statistics', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    argparser.add_argument('--kafka-broker', dest='kafka_broker', help='Broker IP', required = False, default='localhost' )
    argparser.add_argument('--text', dest='text', help='Only plain text, no rich output.', required = False, default=False, action='store_true' )
    argparser.add_argument('--poll-period', dest='kafka_poll_period', help='Kafka offset poll period (seconds) for evts/sec calculation', required = False, default=5)
    argparser.add_argument('--poll-iterations', dest='kafka_poll_iterations', help='How many times to query and display stats. 0 = Inf', required = False, default=15)
    argparser.add_argument('--group-exclude-pattern', dest='kafka_group_exclude_pattern', help='If group matches regex, exclude ', required = False, default=None )# default='_[0-9]+$')
    argparser.add_argument('--group-filter-pattern', dest='kafka_group_filter_pattern', help='Include *only* the groups which match regex', required = False, default=None)
    argparser.add_argument('--status', dest='kafka_status', help='Report health status in json and exit.', required = False, action='store_true')
    argparser.add_argument('--noinitial', dest='kafka_noinitial', help='Do not display initial lag summary.', default=False, required = False, action='store_true')
    argparser.add_argument('--only-issues', dest='kafka_only_issues', help='Only show rows with issues.', default=False, required = False, action='store_true')
    argparser.add_argument('--all', dest='kafka_show_empty_groups', help='Show groups with no members.', default=False, required = False, action='store_true')
    argparser.add_argument('--version', action='version', version=f'%(prog)s {VERSION}')
    args = argparser.parse_args()

    params = init_conf(args)
    signal.signal(signal.SIGINT, signal_handler)


    if args.kafka_status:
        lag_show_status(params)
        sys.exit(0)

    if args.text:
        lag_show_text(params)
    else:
        lag_show_rich(params)

