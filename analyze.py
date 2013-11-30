#!/usr/bin/env python

import cPickle
import datetime
import os

num = 0
total = 0

def main():
    global num
    global total
    all_games = os.listdir('data')

    for filename in all_games:
        with open('data/' + filename) as f:
            try:
                game = cPickle.load(f)
                analyze(game)
            except EOFError:
                print('EOF on {}, removing', filename)
                os.unlink('data/' + filename)
        if total % 1000 == 0:
            print('processed {}'.format(total))
    print('found {} valid games out of {}'.format(num, total))

def analyze(game):
    global num
    global total

    total += 1

    if game['duration'] > 300 \
            and game['human_players'] == 10 \
            and game['game_mode'] == 1 \
            and game['lobby_type'] in [0,5,6]:
        num += 1
    

if __name__ == '__main__':
    main()
