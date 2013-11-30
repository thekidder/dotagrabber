#!/usr/bin/env python

import cPickle
import datetime
import os
import requests
import time
import settings

BASE_URL = 'https://api.steampowered.com/IDOTA2Match_570'
VERSION = 'V001'

MATCH_HISTORY = 'GetMatchHistory'
MATCH_HISTORY_SEQ = 'GetMatchHistoryBySequenceNum'
MATCH_DETAILS = 'GetMatchDetails'

API_KEY = settings.API_KEY


request_queue = list()
highest_match_received = 0
highest_seq_num = 0

def main():
    while True:
        try:
            run()
        except requests.exceptions.ConnectionError:
            print "error! sleeping."
            time.sleep(30)

def run():
    global request_queue
    global highest_match_received

    ensure_dir('data')

    all_games = os.listdir('data')

    for game in all_games:
        game = int(game)
        if game > highest_match_received:
            highest_match_received = game

    print('starting from {}'.format(highest_match_received))

    request_queue.append(get_new_matches_start())

    while True:
        if len(request_queue) > 0:
            top = request_queue[0]
            request_queue = request_queue[1:]
            sleep_time = process_request(top)
            time.sleep(sleep_time)
        else:
            request_queue.append(get_new_matches())

def process_request(request):
    global request_queue
    global highest_match_received

    url, handler = request
    r = requests.get(url)
    if(r.status_code == 503):
        print('rate limited!')
        return 30

    if(r.status_code != 200):
        print('some other error ({})! sleeping to be safe'.format(r.status_code))
        return 30

    handler(r)
    return 1


def get_new_matches_start():
    url  = '{}/{}/{}/?key={}'.format(BASE_URL, MATCH_HISTORY, VERSION, API_KEY)

    print(url)

    return [url, process_match_history]

def get_match_details(match_id):
    url  = '{}/{}/{}/?key={}&match_id={}'.format(BASE_URL, MATCH_DETAILS, VERSION, API_KEY, match_id)

    return [url, process_match_details]

def get_new_matches():
    global highest_seq_num

    if(highest_seq_num == 0):
        return get_new_matches_start()

    url  = '{}/{}/{}/?key={}&start_at_match_seq_num={}'.format(BASE_URL, MATCH_HISTORY_SEQ, VERSION, API_KEY, highest_seq_num + 1)

    print(url)

    return [url, process_match_history]

def process_match_history(response):
    global request_queue
    global highest_match_received
    global highest_seq_num

    matches = response.json()['result']['matches']
    matches.reverse()

    for match in matches:
        id = match['match_id']
        seq = match['match_seq_num']
        if highest_seq_num > 0 or id > highest_match_received:
            highest_match_received = int(id)
            print('got new match {}, {}'.format(id, seq))
            request_queue.append(get_match_details(id))
        if seq > highest_seq_num:
            highest_seq_num = int(seq)

def process_match_details(response):
    global request_queue
    global highest_match_received

    match = response.json()['result']

    print('got match {} starting at {}'.format(match['match_id'], datetime.datetime.utcfromtimestamp(match['start_time'])))

    f = open('data/{}'.format(match['match_id']), 'w')
    cPickle.dump(match, f)
    f.close()

def ensure_dir(f):
    if not os.path.exists(f):
        os.makedirs(f)


if __name__ == '__main__':
    main()
