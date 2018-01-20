import logging
import logging.config
import pymongo as pm
import datetime as dt
import urllib.parse
import argparse
import time
import pandas as pd
import math
from bs4 import BeautifulSoup as bs
import demjson
from itertools import compress

import url_helper
import configs
import log_config
from article import Article

script_name = 'webnews_watcher'
log = logging.getLogger(script_name)

class NewsWatcher:
    def __init__(self):
        self.data_coll = self._get_db_coll(configs.data_coll)
        self.misc_config_coll = self._get_db_coll(configs.misc_config_coll)
        self.pause_btw_crawl = configs.wn_pause_btw_crawl
        self.pause_btw_monitor = configs.wn_pause_btw_monitor
        self.pause_btw_linebacking = configs.wn_pause_btw_linebacking
        self.src_pipeline = {
            'hexun': {'ix_parser': self.ix_parser_hexun,
                      'article_parser': self.article_parser_hexun}
        }

    def _get_db_coll(self, collection_name):
        client = pm.MongoClient(configs.db_conn_str)
        db = client.crawls
        coll = db[collection_name]
        return coll

    def _filter_new_articles(self, article_urls):
        log.debug('Filtering out crawled articles')
        query = self.data_coll.find({'url': {'$in': article_urls}}, {'url': 1})
        if bool(query):
            old_articles_url = [i['url'] for i in query]
        else:
            new_articles_url = []
        new_articles_url = list(set(article_urls) - set(old_articles_url))
        return new_articles_url

    def _filter_missing_date(self, src, query_dates):
        log.debug('Filtering out missing date')
        query = self.misc_config_coll.find_one({'name': 'crawl_completed_date',
                                                'src': src,
                                                })
        missing_flags = [i not in query['items'] for i in query_dates] #if completed, return true else false
        return missing_flags

    def _to_db(self, article):
        log.debug('Inserting crawled articles')
        try:
            results = self.data_coll.replace_one({'url': article.url}, article.export_db_fmt(), upsert=True)
            log.debug('Insert Results {}'.format(results.raw_result))
        except Exception as e:
            log.debug('Failed insert. Exception: \n {}'.format(e))

    def _log_completed_date(self, src, as_of_date):
        log.debug('Logging completion date')
        try:
            query = self.misc_config_coll.update_one(
                {'name': 'crawl_completed_date', 'src': src, },
                {'$addtoset': {'items': as_of_date}})
            query = self.misc_config_coll.update_one(
                {'name': 'crawl_completed_date', 'src': src, },
                {'push': {'items': {'$each': [], '$sort': 1}}})
        except Exception as e:
            logging.debug(e)

    def _article_processor(self, article):
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh-TW;q=0.7,zh;q=0.6',
            'Connection': 'keep-alive',
            'DNT': '1',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36'
        }
        try:
            log.debug('Processing {}'.format(article.url))
            article_html_maps = url_helper.get_url_sync(article.url, headers=headers)
            log.debug('Sleeping {}'.format(self.pause_btw_crawl))
            time.sleep(self.pause_btw_crawl)
            content_article = self.src_pipeline[article.src]['article_parser'](article= article, article_html=article_html_maps[article.url])
            article.left_merge(content_article)
        except Exception as e:
            log.error('Error processing news {}'.format(article.url))
            log.debug('Error msg {}'.format(e))
        return article

    def _process_news(self, src, as_of_dt):
        log.info('Start Processing {} for date {}'.format(src, as_of_dt))
        daily_articles = self.src_pipeline[src]['ix_parser'](as_of_dt)
        new_articles_url = self._filter_new_articles([i.url for i in daily_articles])
        log.info('{} new article discovered'.format(len(new_articles_url)))
        new_articles = filter(lambda x: x.url in new_articles_url, daily_articles)
        processed_news = []
        for i in new_articles:
            if i.url not in processed_news:
                processed_article = self._article_processor(i)
                self._to_db(processed_article)
            else:
                log.debug('{} already processed. Skipping'.format(i.url))
        log.info('Done processing source: {} for date: {}'.format(src, as_of_dt))

    def monitor(self):
        while True:
            log.info('Start checking updates')
            as_of_dt = dt.datetime.combine(dt.datetime.utcnow().date(), dt.time(0))
            for src in self.src_pipeline.keys():
                self._process_news(src, as_of_dt)
            log.info('Next checking in {} seconds'.format(self.pause_btw_monitor))
            time.sleep(self.pause_btw_monitor)

    def linebacker(self):
        while True:
            log.info('Start linebacking')
            for src in self.src_pipeline.keys():
                query = self.misc_config_coll.find_one({'name': 'linebacker_start_date', 'src': src})
                start_date = query['start_date']
                end_date = dt.datetime.combine(dt.datetime.utcnow().date() - dt.timedelta(days=1), dt.time(0))
                self.backfill(src, start_date, end_date)
            log.info('Next checking in {} seconds'.format(self.pause_btw_linebacking))
            time.sleep(self.pause_btw_linebacking)

    def backfill(self, src, start_date, end_date):
        if src == 'all':
            sources = list(self.src_pipeline.keys())
        else:
            sources = [src]
        date_rng = pd.date_range(start_date, end_date)
        process_flags = self._filter_missing_date(src, date_rng)
        for s in sources:
            for scan_dt in compress(date_rng, process_flags):
                try:
                    log.info('Start Processing {} for date {}'.format(src, scan_dt))
                    self._process_news(s, scan_dt)
                    self._log_completed_date(s, scan_dt)
                except Exception as e:
                    log.error('Error Processing {} for date {}'.format(s, scan_dt))
                    log.debug('Error msg {}'.format(e))


    def ix_parser_hexun(self, as_of_dt):
        src_name = 'hexun'
        dt_fmt = r'%Y-%m-%d'
        articles_per_pg = 30
        url_template = r'http://roll.hexun.com/roolNews_listRool.action?type=all&ids=100,101,103,125,105,124,162,194,108,122,121,119,107,116,114,115,182,120,169,170,177,180,118,190,200,155,130,117,153,106&date={date}&page={page}'
        headers = {
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh-TW;q=0.7,zh;q=0.6',
            'Connection': 'keep-alive',
            #     'Cookie': '__jsluid=55133f1037eb29153a2290b27dc5d112; UM_distinctid=1608c824e45225-0187e4f3081221-16386656-fa000-1608c824e4639e; HexunTrack=SID=20171225151038013bf64005c70424b62990fd27a22ba0eb7&CITY=81&TOWN=0; __utma=194262068.122689198.1514185838.1514185838.1514185838.1; __utmc=194262068; __utmz=194262068.1514185838.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); vjuids=2c8772259.1608c828e36.0.577adf14db4dd; vjlast=1514185855.1514185855.30; hxck_sq_common=LoginStateCookie=; ASL=17525,0000p,3d5d7863; ADVC=35cb70ac656a72; ADVS=35cb70ac656a72; cn_1263247791_dplus=%7B%22distinct_id%22%3A%20%221608c824e45225-0187e4f3081221-16386656-fa000-1608c824e4639e%22%2C%22sp%22%3A%20%7B%22userFirstDate%22%3A%20%2220171225%22%2C%22userID%22%3A%20%22%22%2C%22userName%22%3A%20%22%22%2C%22userType%22%3A%20%22nologinuser%22%2C%22userLoginDate%22%3A%20%2220171225%22%2C%22%24_sessionid%22%3A%200%2C%22%24_sessionTime%22%3A%201514186147%2C%22%24dp%22%3A%200%2C%22%24_sessionPVTime%22%3A%201514186147%7D%2C%22initial_view_time%22%3A%20%221514181081%22%2C%22initial_referrer%22%3A%20%22http%3A%2F%2Fnews.hexun.com%2F%22%2C%22initial_referrer_domain%22%3A%20%22news.hexun.com%22%7D; CNZZDATA1262910278=584070923-1514181788-http%253A%252F%252Fnews.hexun.com%252F%7C1514181788; __utmb=194262068.3.10.1514185838',
            'DNT': '1',
            'Host': 'roll.hexun.com',
            'Referer': 'http://roll.hexun.com/',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36'
        }
        current_page = 1
        num_pages = 1
        articles = []
        log.debug('Getting {} index page for date {}'.format(src_name, as_of_dt))
        while current_page <= num_pages:
            url = url_template.format(date = as_of_dt.strftime(dt_fmt), page=current_page)
            ix_page = url_helper.get_url_sync(url, headers=headers)[url]
            time.sleep(self.pause_btw_crawl)
            try:
                ix_parsed = demjson.decode(ix_page)
                num_rcd = int(ix_parsed['sum'])
                num_pages = math.ceil(num_rcd/articles_per_pg) #comment out for debug
                rcds = ix_parsed['list']
                for rcd in rcds:
                    if not rcd['titleLink'].endswith('PDF'):
                        articles.append(Article(url=rcd['titleLink'],
                                                title=rcd['title'],
                                                section=rcd['columnName'],
                                                as_of_dt=as_of_dt,
                                                crawl_ts=dt.datetime.utcnow(),
                                                src=src_name
                                                )
                                        )
            except Exception as e:
                log.error('Failed to parse {} index page {} for date {}'.format(src_name, current_page, as_of_dt))
                log.debug('Error msg {}'.format(e))
            finally:
                current_page += 1
        return articles

    def article_parser_hexun(self, article, article_html):
        src_name = 'hexun'
        content = None
        soup = bs(article_html, 'html.parser')
        if soup.find('div', class_='art_context') is not None:
            content_div = soup.find('div', class_='art_context')
        elif soup.find('div', class_='concent') is not None:
            content_div = soup.find('div', class_='concent')
        else:
            content_div = None
            log.error('Failed to parse content for {}'.format(article.url))

        # get article
        if content_div is not None:
            if bool(content_div.find_all('p')) is True:
                content_p = [i.text for i in content_div.find_all('p')]
                content = ''.join(content_p)
            else:
                content = content_div.text
        content_article = Article(text=content)
        return content_article


def test_crawler():
    source = 'hexun'
    start_dt = dt.datetime(2018,1,14)
    end_dt = dt.datetime(2018, 1, 14)
    watcher = NewsWatcher()
    watcher.backfill(source, start_dt, end_dt)


if __name__ == '__main__':
    ### Normal Run ###
    arg_parser = argparse.ArgumentParser(description='Webnews Watcher.')
    arg_parser.add_argument('--action', dest='action', action='store',
                            nargs=1,
                            help='action to take. Either backfill, monitor or lineback')
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
    action = args.action[0]
    if action == 'backfill':
        dt_fmt = r'%Y%m%d'
        source = args.source[0]
        start_dt = dt.datetime.strptime(args.start_date[0], dt_fmt)
        end_dt = dt.datetime.strptime(args.end_date[0], dt_fmt)
        script_name = 'webnews_backfill_{}_{}_{}'.format(source, args.start_date[0],args.end_date[0])
    elif action == 'monitor':
        script_name = 'webnews_watcher'
        source = 'all'
        end_dt = dt.datetime.combine(dt.datetime.utcnow().date(), dt.time(0))
        start_dt = end_dt
    elif action == 'lineback':
        script_name = 'webnews_linebacker'
        source = 'all'
        end_dt = dt.datetime.combine(dt.datetime.utcnow().date(), dt.time(0))
        start_dt = end_dt

    config = log_config.get_config(script_name, configs.webhook_url)
    logging.config.dictConfig(config)
    log = logging.getLogger(script_name)

    watcher = NewsWatcher()

    try:
        if action == 'backfill':
            watcher.backfill(source, start_dt, end_dt)
        elif action == 'monitor':
            watcher.monitor()
        elif action == 'lineback':
            watcher.linebacker()
    except KeyboardInterrupt:
        log.info('Stopping')

    ### Normal Run ###

    # test_crawler()