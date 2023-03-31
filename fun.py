from tqdm import tqdm
from typing import Tuple, List, Optional, Union, Callable, ClassVar, Dict
import pandas as pd
import json
from collections import defaultdict
from dataclasses import dataclass, field
from pprint import pprint
from snscrape.modules import twitter
from datetime import date
from copy import deepcopy
from bs4 import BeautifulSoup
from loguru import logger
import time
import csv

@dataclass
class Tweet_Node(twitter.Tweet):
    _new_child: 'Tweet_Node' = None
    child: List['Tweet_Node'] = field(default_factory=list)
    parent: Optional['Tweet_Node'] = None
    root: 'Tweet_Node' = None
    message: str = None
    is_automated: bool = None
    is_verified: bool = None
    
    @property
    def conversation(self) -> List:
        ret = [self.message]
        while self.parent:
            self = self.parent
            ret.append(self.message)
        return ret[::-1]

    def __eq__(self, other: Union['Tweet_Node', str, int]): 
        return self.id == other.id if isinstance(other, Tweet_Node) else self.id == other
    def __repr__(self): return self.url
    def __post_init__(self): 
        self.rawContent = self.rawContent.replace('\n', ' ')
        self.message = f'{self.user.displayname}: {self.rawContent}'
        if self._new_child and self._new_child not in self.child:
            self.child.append(self._new_child)
        self.conversationId = str(self.conversationId)
        self.inReplyToTweetId = str(self.inReplyToTweetId)
        self.id = str(self.id)
        if self.user.label:
            self.is_automated = 1 if self.user.label.description == 'Automated' else 0
        self.is_verified = 1 if self.user.verified else 0
        # self.root = self.recurse_to_root_tweet() # runs the to_root method I run Conversation_Tree.populate_leaves() instead

class Conversation_Tree:
    all_tree_roots: Dict[str, bool] = {}
    def __init__(self, root:'Tweet_Node'):
        self.root: Tweet_Node = root
        self.leaves: List['Tweet_Node'] = []
        self.all_tree_roots[str(root.conversationId)] = True
    
    def populate_leaves(self, leaf_container: List[Tweet_Node]):
        queue = [self.root]
        while queue:
            node = queue.pop(0)
            children = [leaf for leaf in leaf_container if leaf.inReplyToTweetId == node.id]
            for child in children:
                child.parent = node
                node.child.append(child)
                self.leaves.append(child)
            queue += [child for child in children if child.replyCount != 0]
            leaf_container = [leaf for leaf in leaf_container if leaf not in children]


def scrape_tweets(coin: str, date_lis: Tuple[str]) -> pd.DataFrame:
    allowed_sources = ["Twitter Web App", "Twitter for iPhone", "Twitter for Android"]
    since, until = date_lis
    f_path = f'data/{coin}_since_{since}_until_{until}_scrape'
    headers = ["User", "Date", "ID", "Likes", "Replies", "Retweets", "Source of Tweet", "User Tweet Count", "User Follower Count", "Automated User", "Verified User", "Language", "Tweet", "Conversation", "rootId"]
    logger.add(sink=f_path+'.log', enqueue=True, format="<green>{time}</green> | <level>{message}</level>", level='INFO')
    with open(f_path+'.csv', 'w', newline='') as csv_f:
        csv_writer = csv.writer(csv_f)
        csv_writer.writerow(headers)
        for tweet in twitter.TwitterSearchScraper(f'"{coin}" since:{since} until:{until} lang:en').get_items(): # conversation_id: is the root tweet's ID
            st = time.monotonic()
            tweet = Tweet_Node(**vars(tweet))
            if tweet.likeCount < 200:
                continue
            if tweet.sourceLabel not in allowed_sources or Conversation_Tree.all_tree_roots.get(tweet.conversationId): # if tweet is root and no replies
                continue
            if tweet.conversationId == tweet.id and tweet.replyCount == 0: # if root tweet
                leaf = tweet
                csv_writer.writerow([leaf.user.username, leaf.date, leaf.id, leaf.likeCount, leaf.replyCount, leaf.retweetCount, leaf.sourceLabel, leaf.user.statusesCount, leaf.user.followersCount, leaf.is_automated, leaf.is_verified, leaf.lang, leaf.rawContent, leaf.conversation, leaf.conversationId])
                et = time.monotonic() 
                logger.info(f'coin: {coin} {since, until} single tweet {tweet} took: {float(et - st):.2f}')
                continue
            if tweet.conversationId != tweet.id:
                try:
                    root_tweet = next(twitter.TwitterTweetScraper(str(tweet.conversationId)).get_items())
                except StopIteration: # StopIteration is called because either tweet has been deleted or is privated
                    continue # skip tweet
                except Exception as e:
                    print(e)
                else:
                    if isinstance(root_tweet, twitter.Tombstone):
                        continue
            else:
                root_tweet = tweet
            tree = Conversation_Tree(root=Tweet_Node(**vars(root_tweet)))
            leaf_container = [i for i in twitter.TwitterSearchScraper(f'"{coin}" lang:en conversation_id:{tweet.conversationId}').get_items() if i.sourceLabel in allowed_sources]
            #leaf_container = [i for i in twitter.TwitterSearchScraper(f'lang:en conversation_id:{tweet.conversationId}').get_items() if i.sourceLabel in allowed_sources]
            leaf_container = [Tweet_Node(**vars(leaf)) for leaf in leaf_container]
            tree.populate_leaves(leaf_container)
            for leaf in tree.leaves:
                csv_writer.writerow([leaf.user.username, leaf.date, leaf.id, leaf.likeCount, leaf.replyCount, leaf.retweetCount, leaf.sourceLabel, leaf.user.statusesCount, leaf.user.followersCount, leaf.is_automated, leaf.is_verified, leaf.lang, leaf.rawContent, leaf.conversation, leaf.conversationId])
            et = time.monotonic() 
            logger.info(f'coin: {coin} {since, until} {tree.root} took: {float(et - st):.2f} leaves: {len(tree.leaves)}')


def mp_tweet_scraper(coin: str, date_lis: Tuple[date, date], fxn=scrape_tweets) -> bool:
    try:
        date_lis = date_formatter(date_lis, "%Y-%m-%d")
        fxn(coin=coin, date_lis=date_lis)
        return 0
    except Exception as e:
        return e


def date_formatter(d: Tuple[date, date], f: str) -> Tuple[Tuple[str, str]]:
    return (d[0].strftime(f), d[1].strftime(f))


if __name__ == "__main__":
    from pycoingecko import CoinGeckoAPI
    from multiprocessing import Pool
    from dotenv import load_dotenv
    import os
    import requests

    date_lis = [["luna", [date(2022, 5, 1), date(2022, 5, 5)]],
                ["luna", [date(2022, 5, 6), date(2022, 5, 10)]],
                ["luna", [date(2022, 5, 11), date(2022, 5, 14)]],
                ["shib", [date(2021, 8, 22), date(2021, 8, 25)]],
                ["shib", [date(2021, 8, 26), date(2021, 8, 29)]],
                ["shib", [date(2021, 8, 2), date(2021, 8, 5)]],
                ["shib", [date(2021, 8, 6), date(2021, 8, 8)]],
                ["doge", [date(2021, 12, 19), date(2021, 12, 21)]],
                ["doge", [date(2021, 2, 3), date(2021, 2, 5)]],
                ["doge", [date(2021, 2, 7), date(2021, 2, 9)]],
                ["doge", [date(2020, 12, 31), date(2021, 1, 4)]],
                ["doge", [date(2021, 1, 27), date(2021, 2, 2)]],
                ["doge", [date(2021, 4, 12), date(2021, 4, 16)]],
                ]
    join_lis = [["luna", [date(2022, 5, 1), date(2022, 5, 14)]],
                ["shib", [date(2021, 8, 22), date(2021, 8, 29)]],
                ["shib", [date(2021, 8, 2), date(2021, 8, 8)]],
                ["doge", [date(2021, 12, 19), date(2021, 12, 21)]],
                ["doge", [date(2021, 2, 3), date(2021, 2, 5)]],
                ["doge", [date(2021, 2, 7), date(2021, 2, 9)]],
                ["doge", [date(2020, 12, 31), date(2021, 1, 4)]],
                ["doge", [date(2021, 1, 27), date(2021, 2, 2)]],
                ["doge", [date(2021, 4, 12), date(2021, 4, 16)]],
                ]
    join_lis = [["shib", [date(2021, 8, 22), date(2021, 8, 29)]]]

 
    load_dotenv()
    COIN_API = os.getenv("COIN_API")
    p_cnt = os.cpu_count()
    cg = CoinGeckoAPI()
    headers = {'X-CoinAPI-Key' : COIN_API}
    for entry in join_lis:
        coin, time_intv = entry
        since, until = time_intv
        for market in ["SPOT"]:
            f_path = f'data_olhcv/{coin}_since_{since}_until_{until}_{market}.csv'
            with open(f_path, 'w', newline='') as f:
                url = f'https://rest.coinapi.io/v1/ohlcv/BINANCE_{market}_{coin}_USDT/history?period_id=15MIN&time_start={since.isoformat()}&time_end={until.isoformat()}&include_empty_items=true&limit=10000'
                response = requests.get(url, headers=headers).json()

                writer = csv.writer(f)
                writer.writerow([
                    'time_period_start', 'time_period_end', 'time_open', 'time_close',
                    'price_open', 'price_high', 'price_low', 'price_close',
                    'volume_traded', 'trades_count'
                ])

                # Write data rows
                for item in response:
                    try:
                        writer.writerow([
                            item['time_period_start'], item['time_period_end'],
                            item['time_open'], item['time_close'], item['price_open'],
                            item['price_high'], item['price_low'], item['price_close'],
                            item['volume_traded'], item['trades_count']
                        ])
                    except Exception as e:
                        print(e)
                        pprint(item)
                        print(coin, since, until)
    # pprint(cg.get_coins_markets(vs_currency = "USD"))
    # print(cg.get_coins_list())
    # pool = Pool(processes=p_cnt)
    # results = pool.starmap(mp_tweet_scraper, list(date_lis))
