from bs4 import BeautifulSoup as bs
import datetime as dt
import logging
import pymongo as pm
from pandas import date_range
import pprint as pp
from collections import OrderedDict

import url_helper
import configs

def get_ix_pg(as_of_dt):
    logging.debug('Getting index page for date {}'.format(as_of_dt))
    dt_fmt = '%Y%m%d'
    path_template = r'https://hk.appledaily.com/archive/index/{}'
    path = path_template.format(as_of_dt.strftime(dt_fmt))
    ix_page = url_helper.get_url(path)
    soup = bs(ix_page, 'html.parser')
    archive_div = soup.find('div', class_='Archive')
    #need to add checking see if this is ix page

    #find article type mapping; need to add try
    logging.debug('Getting index page for date {}'.format(as_of_dt))
    title_div = archive_div.find_all('div', class_='title')
    url_title_map = {}
    for section in title_div:
        section_name = section.text
        section_urls = section.next_sibling.next_sibling.find_all('a')
        for section_url in section_urls:
            url_title_map[section_url['href']] = section_name

    #find all articule link
    raw_article_links = archive_div.find_all('a')
    article_links = []
    for lk in raw_article_links:
        tmp_url = lk['href']
        tmp_title = lk.text
        tmp_section = url_title_map[tmp_url] #need to add try
        if bool(tmp_title):
            article_links.append({'url': tmp_url, 'title': tmp_title, 'section': tmp_section})
    return article_links

# def get_articles(article_links):
#     articles = []
#     for article_link in article_links:
#         article_pg = url_helper.retrieve_pg(article_link)
#         article = parse_article(article_pg)
#         articles.append(article) #need to add try
#     return articles

def parse_article(article_html):
    article=None
    try:
        soup = bs(article_html, 'html.parser')
        main_content_div = soup.find('div', id='masterContent')
        article = main_content_div.text.strip()
    except:
        logging.debug('Failed to parse article. HTML: {}'.format(article_html))
    return article

def process_news(as_of_dt):
    logging.debug('Start processing news for {}'.format(as_of_dt))
    daily_paper=None
    try:
        daily_paper = get_ix_pg(as_of_dt) #add retry for getting index
        article_links = [i['url'] for i in daily_paper]
        article_html_maps = url_helper.get_urls(article_links, True, configs.aapl_sim_req)
        for article in daily_paper:
            article['contents'] = parse_article(article_html_maps[article['url']]) #add retry in getting contents
        to_db(as_of_dt, daily_paper)
        logging.debug('Finish processing news for {}'.format(as_of_dt))
    except:
        logging.debug('Failed processing news for {}'.format(as_of_dt))
    return daily_paper

def to_db(as_of_dt, daily_paper):
    try:
        logging.debug('Inserting paper as of date {}'.format(as_of_dt))
        client = pm.MongoClient(configs.db_conn_str)
        db=client.crawls
        apple_daily_coll = db['appledaily']
        docu = OrderedDict([
            ('as_of_date', as_of_dt),
            ('crawl_date', dt.datetime.utcnow()),
            ('articles', daily_paper)
        ])
        db_results = apple_daily_coll.insert_one(docu)
        logging.debug(db_results)
    except:
        logging.error('Failed to insert data for {}'.format(as_of_dt))

def backfill_hp():
    start_dt = dt.datetime(2017,1,1)
    end_dt = dt.datetime.combine(dt.datetime.utcnow().date(), dt.time())
    logging.debug('Start backfill from {} to {}'.format(start_dt, end_dt))
    crawled_dt = get_crawled_dt()
    crawl_list = list(set(date_range(start_dt, end_dt)) - set(crawled_dt))
    crawl_list.sort(reverse=True)
    for d in crawl_list:
        process_news(d)
    logging.debug('Complete backfill from {} to {}'.format(start_dt, end_dt))


def get_crawled_dt():
    client = pm.MongoClient(configs.db_conn_str)
    db = client.crawls
    apple_daily_coll = db['appledaily']
    query = apple_daily_coll.find({}, {'as_of_date':1})
    crawled_dt = [i['as_of_date'] for i in query]
    return crawled_dt


def test():
    as_of_dt = dt.datetime(2017, 10, 4)
    papers = process_news(as_of_dt)
    print(papers)




if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(threadName)s - %(message)s')
    try:
        backfill_hp()
    except KeyboardInterrupt:
        logging.info('Terminating')

    # as_of_dt = dt.datetime(2007,10,4)
    # article_links = get_ix_pg(as_of_dt)
    # pp.pprint(article_links)
    # test_article_urls = [r'https://hk.news.appledaily.com/international/daily/article/20171004/20172589']
    # articles = get_articles(test_article_urls)
    # pp.pprint(articles)
