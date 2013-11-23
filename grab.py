#!/usr/bin/env python

import cPickle
import datetime
import os
import requests
import time

BASE_URL = 'https://api.steampowered.com/IDOTA2Match_570'
VERSION = 'V001'

MATCH_HISTORY = 'GetMatchHistory'
MATCH_DETAILS = 'GetMatchDetails'

API_KEY = '63550223A5E9025487635A696C3CA77B'


request_queue = list()
highest_match_received = 0

def main():
    global request_queue
    global highest_match_received

    ensure_dir('data')

    all_games = os.listdir('data')

    for game in all_games:
        game = int(game)
        if game > highest_match_received:
            highest_match_received = game

    print('starting from {}'.format(highest_match_received))

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
        request_queue = [request].extend(request_queue)
        return 30

    if(r.status_code != 200):
        print('some other error! sleeping to be safe')
        request_queue = [request].extend(request_queue)
        return 30

    handler(r)
    return 1


def get_new_matches():
    url  = '{}/{}/{}/?key={}'.format(BASE_URL, MATCH_HISTORY, VERSION, API_KEY)

    return [url, process_match_history]

def get_match_details(match_id):
    url  = '{}/{}/{}/?key={}&match_id={}'.format(BASE_URL, MATCH_DETAILS, VERSION, API_KEY, match_id)

    return [url, process_match_details]

def process_match_history(response):
    global request_queue
    global highest_match_received

    matches = response.json()['result']['matches']
    matches.reverse()

    for match in matches:
        id = match['match_id']
        if id > highest_match_received:
            highest_match_received = id
            print('got new match {}'.format(id))
            request_queue.append(get_match_details(id))

def process_match_details(response):
    global request_queue
    global highest_match_received

    match = response.json()['result']

    print('got match starting at {}'.format(datetime.datetime.utcfromtimestamp(match['start_time'])))

    f = open('data/{}'.format(match['match_id']), 'w')
    cPickle.dump(match, f)
    f.close()

def ensure_dir(f):
    if not os.path.exists(f):
        os.makedirs(f)


if __name__ == '__main__':
    main()
