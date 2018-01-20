from bs4 import BeautifulSoup as bs
import datetime as dt
import logging
import logging.config
import pymongo as pm
from pandas import date_range
import pprint as pp
from collections import OrderedDict
import urllib.parse

import url_helper
import configs
import log_config

script_name = r'crawler.orientaldaily'

def get_ix_pg(as_of_dt):
    log.debug('Getting index page for date {}'.format(as_of_dt))
    dt_fmt = '%Y%m%d'
    path_template = r'http://orientaldaily.on.cc/cnt/sitemap/{}/index.html'
    path = path_template.format(as_of_dt.strftime(dt_fmt))
    ix_page = url_helper.get_url(path)
    soup = bs(ix_page, 'html.parser')

    article_tbl = soup.find('table', class_='sitemapTable')

    #url section mapping
    url_section_map = {}
    for section in article_tbl.find_all('h1'):
        subseq_links = []
        for i in section.find_all_next('ul', class_='clearList'):
            for j in i.find_all('a'):
                subseq_links.append(j['href'])
        next_section = section.find_next('h1')
        next_sect_subseq_links = []
        if next_section is not None:
            for i in next_section.find_all_next('ul', class_='clearList'):
                for j in i.find_all('a'):
                    next_sect_subseq_links.append(j['href'])
        this_section_links = list(set(subseq_links) - set(next_sect_subseq_links))
        for url in this_section_links:
            url_section_map[url] = section.text

    #article links map
    url_title_map = {}
    for i in article_tbl.find_all('ul', class_='clearList'):
        for j in i.find_all('a'):
            url = j['href']
            url_title_map[url] = j.text

    #combine both
    article_links = []
    base = r'http://orientaldaily.on.cc'
    for url, title in url_title_map.items():
        tmp_url = urllib.parse.urljoin(base, url)
        tmp_section = url_section_map[url]
        article_links.append({'url': tmp_url,
                              'title': title,
                              'section': tmp_section})
    return article_links

def parse_article(article_html):
    article=None
    try:
        #get lead txt
        leader_txt=''
        try:
            soup = bs(article_html, 'html.parser')
            lead_div = soup.find('div', class_='leadin')
            leader_txt = lead_div.text
        except:
            log.debug('No Leading Text')
        #main text
        main_content_txt = ''
        try:
            main_content_div = soup.find('div', id='contentCTN-right')
            main_content_div.script.decompose()
            main_content_txt = main_content_div.text
        except:
            log.debug('No Main Text')
        article = leader_txt + main_content_txt
    except:
        log.debug('Failed to parse article. HTML: {}'.format(article_html))
    return article

def process_news(as_of_dt):
    log.info('Start processing news for {}'.format(as_of_dt))
    daily_paper=None
    try:
        daily_paper = get_ix_pg(as_of_dt) #add retry for getting index
        article_links = [i['url'] for i in daily_paper]
        article_html_maps = url_helper.get_urls_async(article_links, True, configs.aapl_sim_req)
        for article in daily_paper:
            article['contents'] = parse_article(article_html_maps[article['url']]) #add retry in getting contents
        to_db(as_of_dt, daily_paper)
        log.info('Finish processing news for {}'.format(as_of_dt))
    except:
        log.error('Failed processing news for {}'.format(as_of_dt))
    return daily_paper

def to_db(as_of_dt, daily_paper):
    try:
        log.debug('Inserting paper as of date {}'.format(as_of_dt))
        client = pm.MongoClient(configs.db_conn_str)
        db=client.crawls
        apple_daily_coll = db['orientaldaily']
        docu = OrderedDict([
            ('as_of_date', as_of_dt),
            ('crawl_date', dt.datetime.utcnow()),
            ('articles', daily_paper)
        ])
        db_results = apple_daily_coll.insert_one(docu)
        log.debug(db_results)
    except:
        log.error('Failed to insert data for {}'.format(as_of_dt))

def backfill_hp():
    start_dt = dt.datetime(2005,1,1)
    end_dt = dt.datetime.combine(dt.datetime.utcnow().date(), dt.time())
    log.info('Start backfill from {} to {}'.format(start_dt, end_dt))
    crawled_dt = get_crawled_dt()
    crawl_list = list(set(date_range(start_dt, end_dt)) - set(crawled_dt))
    crawl_list.sort(reverse=True)
    for d in crawl_list:
        process_news(d)
    log.info('Complete backfill from {} to {}'.format(start_dt, end_dt))


def get_crawled_dt():
    client = pm.MongoClient(configs.db_conn_str)
    db = client.crawls
    apple_daily_coll = db['orientaldaily']
    query = apple_daily_coll.find({}, {'as_of_date':1})
    crawled_dt = [i['as_of_date'] for i in query]
    return crawled_dt

def test():
    as_of_dt = dt.datetime(2017, 1, 1)
    papers = process_news(as_of_dt)
    print(papers)


if __name__ == '__main__':

    config = log_config.get_config(script_name, configs.webhook_url)
    logging.config.dictConfig(config)
    log = logging.getLogger(script_name)

    # logging.config.dictConfig(config)
    # logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(threadName)s - %(message)s')

    # as_of_dt = dt.datetime(2017, 10, 3)
    # article_links = get_ix_pg(as_of_dt)
    # pp.pprint(article_links)
    try:
        log.info('Job Starting')
        backfill_hp()
        log.info('Job Complete')
    except :
        log.error('Job Terminated')
