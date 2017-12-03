from bs4 import BeautifulSoup as bs
import datetime as dt
import logging
import logging.config

from pandas import date_range
import pprint as pp
from collections import OrderedDict
import urllib.parse

import url_helper
import db_helper
import configs
import log_config

script_name = r'crawler.securitiestimes'
# note that the paper doesnt publish on holidays

def get_ix_pg(as_of_dt):
    log.debug('Getting index page for date {}'.format(as_of_dt))
    # dt_fmt = '%Y%m'
    path_template = r'http://epaper.stcn.com/paper/zqsb/html/{}/{}/node_1.htm'
    # path_template = r'http://epaper.stcn.com/paper/zqsb/html/{}/{}/node_1.htm'
    path = path_template.format(as_of_dt.strftime(r'%Y-%m'), as_of_dt.strftime(r'%d'))
    ix_page = url_helper.get_url(path)
    soup = bs(ix_page, 'html.parser')
    article_links = None
    #get articles urls
    try:
        section_div = soup.find('div', class_='listWrap')
        sub_sections_div = section_div.find_all('div', class_='area')
        article_links = []
        for sub_section in sub_sections_div:
            sub_section_name = sub_section.h2.a.text
            for article_link_section in sub_section.find_all('li'):
                tmp_article_url = urllib.parse.urljoin(path, article_link_section.a['href'])
                tmp_article_title = article_link_section.a.text
                article_links.append({'url': tmp_article_url,
                                      'title': tmp_article_title,
                                      'section': sub_section_name})
    except:
        logging.debug('Failed to find article links for {}'.format(as_of_dt))
    return article_links

def parse_article(article_html):
    article = None
    try:
        soup = bs(article_html, 'html.parser')
        content_div = soup.find('div', class_='tc_con')
        article = '\n'.join([i.text for i in content_div.find_all('p')])
    except:
        log.debug('Failed to find article')
    return article

def process_news(as_of_dt):
    log.info('Start processing news for {}'.format(as_of_dt))
    daily_paper=None
    try:
        daily_paper = get_ix_pg(as_of_dt) #add retry for getting index
        article_links = [i['url'] for i in daily_paper]
        article_html_maps = url_helper.get_urls_async(article_links, True, configs.secu_times_sim_req, configs.http_stop_mean, configs.secu_times_stop_std, configs.secu_times_stop_max)
        for article in daily_paper:
            article['contents'] = parse_article(article_html_maps[article['url']]) #add retry in getting contents
        db_helper.to_db(as_of_dt, configs.securitiestimes_coll, daily_paper)
        log.info('Finish processing news for {}'.format(as_of_dt))
    except:
        log.error('Failed processing news for {}'.format(as_of_dt))
    return daily_paper

def backfill_hp():
    start_dt = dt.datetime(2017,6,1)
    end_dt = dt.datetime.combine(dt.datetime.utcnow().date(), dt.time())
    log.info('Start backfill from {} to {}'.format(start_dt, end_dt))
    crawled_dt = db_helper.get_crawled_dt(configs.securitiestimes_coll)
    crawl_list = list(set(date_range(start_dt, end_dt)) - set(crawled_dt))
    crawl_list.sort(reverse=True)
    for d in crawl_list:
        if d.weekday() != 6: #skip sunday
            process_news(d)
        else:
            log.info('Skip {}'.format(d))
    log.info('Complete backfill from {} to {}'.format(start_dt, end_dt))


def test_article_links():
    config = log_config.get_config(script_name, configs.webhook_url)
    logging.config.dictConfig(config)
    log = logging.getLogger(script_name)
    as_of_dt = dt.datetime(2017, 11, 18)
    article_links = get_ix_pg(as_of_dt)
    pp.pprint(article_links)

def test_article():
    config = log_config.get_config(script_name, configs.webhook_url)
    logging.config.dictConfig(config)
    log = logging.getLogger(script_name)
    path = r'http://epaper.stcn.com/paper/zqsb/html/2017-11/18/content_1066059.htm'
    content_pg = urllib.request.urlopen(path).read()
    article = parse_article(content_pg)
    print(article)

def test_process_one():
    as_of_dt = dt.datetime(2017, 11, 7)
    papers = process_news(as_of_dt)
    print(papers)

if __name__ == '__main__':
    config = log_config.get_config(script_name, configs.webhook_url)
    logging.config.dictConfig(config)
    log = logging.getLogger(script_name)

    backfill_hp()
