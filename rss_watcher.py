import logging
import logging.config
import feedparser as fp
import pymongo as pm
import datetime as dt
import urllib.parse
import argparse
import time

import url_helper
import db_helper
import configs
import log_config
import parsers
from article import Article



def get_rss_config():
    client = pm.MongoClient(configs.db_conn_str)
    db = client.crawls
    config_coll = db[configs.config_rss_coll]
    query = config_coll.find({'active': True})
    rss_config = {i['url']:
        {
            'src_name': i['src_name'],
            'section': i['section'],
        }
        for i in query}
    return rss_config

def get_parser_config():
    client = pm.MongoClient(configs.db_conn_str)
    db = client.crawls
    config_coll = db[configs.config_parser_coll]
    query = config_coll.find()
    parser_config = {i['net_loc']: i['parser'] for i in query}
    return parser_config

def filter_new_articles(article_urls):
    log.debug('Filtering out crawled articles')
    client = pm.MongoClient(configs.db_conn_str)
    db = client.crawls
    singles_coll = db[configs.news_singles_coll]
    query = singles_coll.find({'url': {'$in': article_urls}}, {'url': 1})
    if bool(query):
        old_articles_url = [i['url'] for i in query]
    else:
        new_articles_url = []
    new_articles_url = list(set(article_urls) - set(old_articles_url))
    return new_articles_url

def to_db(article):
    log.debug('Inserting crawled articles')
    client = pm.MongoClient(configs.db_conn_str)
    db = client.crawls
    singles_coll = db[configs.news_singles_coll]
    results = singles_coll.replace_one({'url': article.url}, article.export_db_fmt(), upsert=True)
    log.debug('Insert Results {}'.format(results.raw_result))

def get_feed(rss_config):
    log.debug('Start getting rss feed')
    rss_urls = list(rss_config.keys())
    rss_feeds = url_helper.get_urls_async(rss_urls, configs.rss_ix_stop_btw_batch, configs.rss_ix_sim_req)
    feed = []
    for rss_url, rss_contents in rss_feeds.items():
        log.debug('Parsing index {}'.format(rss_url))
        try:
            rss_feed =fp.parse(rss_contents)
            rss_entries = rss_feed['entries']
            tmp_section = rss_config[rss_url]['section']
            for rss_entry in rss_entries:

                tmp_url = rss_entry['links'][0]['href']
                tmp_title = rss_entry['title']
                if 'published_parsed' in rss_entry.keys():
                    as_of_dt = dt.datetime(*rss_entry['published_parsed'][:6])
                else:
                    as_of_dt = None
                feed.append(Article(url=tmp_url,
                                    title=tmp_title,
                                    section=tmp_section,
                                    as_of_dt=as_of_dt))


        except Exception as e:
            log.error('Error parsing index {} \n {}'.format(rss_url, e.message))
    return feed

def process_feed():
    log.info('Start scanning rss')
    rss_config = get_rss_config()
    parser_config = get_parser_config()
    feed = get_feed(rss_config)
    new_articles_url = filter_new_articles([i.url for i in feed])
    log.info('{} new article discovered'.format(len(new_articles_url)))
    new_feed = filter(lambda x: x.url in new_articles_url, feed)
    processed_news = []
    for i in new_feed:
        if i.url not in processed_news:
            process_news_worker(i, parser_config)
            processed_news.append(i.url)
        else:
            log.debug('{} already processed. Skipping'.format(i.url))
    log.info('Complete processing rss')


def process_news_worker(article, parser_config):
    """
    :param article: (article object)
    :return:
    """
    try:
        log.debug('Processing {}'.format(article.url))
        article_html_maps = url_helper.get_url_sync(article.url)
        net_loc = urllib.parse.urlparse(article.url).netloc
        if net_loc in parser_config.keys():
            parsed_article_dict = parsers.parser_master(parser_config[net_loc], article_html_maps[article.url], article.url)
            if bool(parsed_article_dict) is True:
                if bool(parsed_article_dict['title']):
                    article.title = parsed_article_dict['title']
                if bool(parsed_article_dict['section']):
                    article.section = parsed_article_dict['section']
                if bool(parsed_article_dict['as_of_dt']):
                    article.as_of_dt = parsed_article_dict['as_of_dt']
                if bool(parsed_article_dict['src']):
                    article.src = parsed_article_dict['src']
                if bool(parsed_article_dict['text']):
                    article.text = parsed_article_dict['text']
            else:
                article.parser_ready = False
        else:
            log.debug('Parser not ready for {}. Only RSS content parsed'.format(net_loc))
            article.parser_ready = False
        article.crawl_ts = dt.datetime.utcnow()
        to_db(article)
        log.debug('Sleeping {}'.format(5))
        time.sleep(5)
    except:
        log.error('Error processing news {}'.format(article.url))

def main():
    while True:
        process_feed()
        log.info('Next Round in {} Minutes'.format(configs.rss_stop_per_round / 60))
        time.sleep(configs.rss_stop_per_round)

def backfill():
    log.debug('Start Backfilling')
    parser_config = get_parser_config()

    client = pm.MongoClient(configs.db_conn_str)
    db = client.crawls
    singles_coll = db[configs.news_singles_coll]
    query = singles_coll.find({'parser_ready': False})

    articles = []
    for i in query:
        try:
            article = Article(url=i['url'],
                                    title=i['title'],
                                    text=i['text'],
                                    section=i['section'],
                                    as_of_dt=i['as_of_dt'],
                                    crawl_ts=i['crawl_ts'],
                                    src=i['src'])
            net_loc = urllib.parse.urlparse(article.url).netloc
            if net_loc in parser_config.keys():
                articles.append(article)
        except:
            log.debug('Issue in parsing article from db')

    log.debug('{} articles to reprocess'.format(len(articles)))
    for ix, article in enumerate(articles):
        log.debug('Process {}/{}'.format(ix + 1, len(articles)))
        process_news_worker(article, parser_config)
    log.debug('Complete Backfilling')

def source_test():
    rss_url = 'http://cn.reuters.com/rssFeed/CNTopGenNews'
    rss_config = {rss_url:{
            'src_name': 'test_src',
            'section': 'test_section',
        }
    }
    feed = get_feed(rss_config)
    return feed

if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description='Process some integers.')
    arg_parser.add_argument('--backfill', dest='backfill', action='store_const',
                        const=True, default=False,
                        help='Backfill news where article isnt ready')

    args = arg_parser.parse_args()
    if args.backfill is True:
        script_name='rss_backfill'
    else:
        script_name = 'rss_watcher'

    config = log_config.get_config(script_name, configs.webhook_url)
    logging.config.dictConfig(config)
    log = logging.getLogger(script_name)


    try:
        if args.backfill is True:
            backfill()
        else:
            main()
    except KeyboardInterrupt:
        log.info('Stopping')
