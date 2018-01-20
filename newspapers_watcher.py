import logging
import logging.config
import pymongo as pm
import datetime as dt
import urllib.parse
import argparse
import time
import pandas as pd

import url_helper
import db_helper
import configs
import log_config
import parsers
import newspaper_ix_parser as pix
from article import Article

# script_name = 'newspaper_watcher'
# log = logging.getLogger(script_name)


def process_newspaper(source = 'all', start_date = None, end_date = None):

    log.info('Start processing newspaper source: {} from: {} to: {}'.format(source, start_date, end_date))
    src_ix_map = {
        'appledaily': pix.appledaily,
        'orientaldaily': pix.orientaldaily,
        'peoplesdaily': pix.peoplesdaily,
        'pladaily': pix.pladaily,
        # 'securitiestimes': pix.securitiestimes
        }

    if source == 'all':
        src_to_process_map = src_ix_map
    else:
        src_to_process_map = {source: src_ix_map[source]}


    for src, ix_parser in src_to_process_map.items():
        for scan_dt in pd.date_range(start_date, end_date):
            try:
                log.info('Start Processing {} for date {}'.format(src, scan_dt))
                articles = []
                daily_articles = ix_parser(scan_dt)
                articles += daily_articles
                new_articles_url = filter_new_articles([i.url for i in articles])
                log.info('{} new article discovered'.format(len(new_articles_url)))
                new_articles = filter(lambda x: x.url in new_articles_url, articles)
                processed_news = []
                for i in new_articles:
                    if i.url not in processed_news:
                        process_news_worker(i)
                        processed_news.append(i.url)
                    else:
                        log.debug('{} already processed. Skipping'.format(i.url))
                log.info('Done Processing {} for date {}'.format(src, scan_dt))
            except Exception as e:
                log.error('Error Processing {} for date {}'.format(src, scan_dt))
                log.debug('Error msg'.format(e))

        log.info('Done processing newspaper source: {} from: {} to: {}'.format(source, start_date, end_date))

def process_news_worker(article):
    try:
        log.debug('Processing {}'.format(article.url))
        article_html_maps = url_helper.get_url_sync(article.url)
        parsed_article_dict = parsers.parser_master(parser_name=article.src,
                                                    article_html = article_html_maps[article.url],
                                                    url = article.url)
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
        to_db(article)
        log.debug('Sleeping {}'.format(configs.pause_btw_crawl))
        time.sleep(configs.pause_btw_crawl)
    except Exception as e:
        log.error('Error processing news {}'.format(article.url))
        log.debug('Error msg {}'.format(e))

def to_db(article):
    log.debug('Inserting crawled articles')
    client = pm.MongoClient(configs.db_conn_str)
    db = client.crawls
    newspaper_coll = db[configs.newspaper_coll]
    results = newspaper_coll.replace_one({'url': article.url}, article.export_db_fmt(), upsert=True)
    log.debug('Insert Results {}'.format(results.raw_result))

def filter_new_articles(article_urls):
    log.debug('Filtering out crawled articles')
    client = pm.MongoClient(configs.db_conn_str)
    db = client.crawls
    newspaper_coll = db[configs.newspaper_coll]
    query = newspaper_coll.find({'url': {'$in': article_urls}}, {'url': 1})
    if bool(query):
        old_articles_url = [i['url'] for i in query]
    else:
        new_articles_url = []
    new_articles_url = list(set(article_urls) - set(old_articles_url))
    return new_articles_url

def backfill(source, start_date, end_date):
    log.info('Start backfilling newspaper {} from {} to {}'.format(source, start_date, end_date))
    process_newspaper(source, start_date, end_date)

def daily_scan():
    while True:
        log.info('Start daily newspaper scan')
        end_date = dt.datetime.combine(dt.datetime.utcnow().date(), dt.time(0))
        start_date = end_date
        process_newspaper('all', start_date, end_date)
        log.info('Done daily newspaper scan. Sleep for {} seconds'.format(24 * 60 * 60))
        time.sleep(24 * 60 * 60)

def paper_test():
    as_of_dt = dt.datetime(2017,12,22)
    process_newspaper(as_of_dt)

if __name__ == '__main__':


    arg_parser = argparse.ArgumentParser(description='Newspaper watcher.')
    arg_parser.add_argument('--backfill', dest='backfill', action='store_const',
                        const=True, default=False,
                        help='Backfill newspaper')
    arg_parser.add_argument('--source', dest='source', action='store',
                        nargs=1,
                        help='source to backfill')
    arg_parser.add_argument('--start_date', dest='start_date', action='store',
                        nargs=1,
                        help='backfill start date')
    arg_parser.add_argument('--end_date', dest='end_date', action='store',
                        nargs=1,
                        help='backfill end date')

    args = arg_parser.parse_args()
    if args.backfill is True:
        dt_fmt = r'%Y%m%d'
        source = args.source[0]
        start_dt = dt.datetime.strptime(args.start_date[0], dt_fmt)
        end_dt = dt.datetime.strptime(args.end_date[0], dt_fmt)
        script_name = 'newspaper_backfill_{}_{}_{}'.format(source, args.start_date[0],args.end_date[0])
    else:
        script_name = 'newspaper_watcher'
        source = 'all'
        end_dt = dt.datetime.combine(dt.datetime.utcnow().date(), dt.time(0))
        start_dt = end_dt

    config = log_config.get_config(script_name, configs.webhook_url)
    logging.config.dictConfig(config)
    log = logging.getLogger(script_name)

    try:
        if args.backfill is True:
            backfill(source=source, start_date=start_dt, end_date=end_dt)
        else:
            daily_scan()
    except KeyboardInterrupt:
        log.info('Stopping')
