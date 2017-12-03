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

script_name = r'crawler.81daily'

def get_ix_pg(as_of_dt):
    log.debug('Getting index page for date {}'.format(as_of_dt))
    # dt_fmt = '%Y%m'

    path_template = r'http://www.81.cn/jfjbmap/content/{}/{}/node_2.htm'
    path = path_template.format(as_of_dt.strftime(r'%Y-%m'), as_of_dt.strftime(r'%d'))
    ix_page = url_helper.get_url(path)
    soup = bs(ix_page, 'html.parser')

    #get sub-section url
    subsection_div = soup.find('div', class_='col-md-4-10 channel-list').find_all('a')
    sub_sections_url_map = {}
    for i in subsection_div:
        if i['href'].endswith('htm'):
            sub_section_url = urllib.parse.urljoin(path, i['href'])
            sub_section_name = i.text
            sub_sections_url_map[sub_section_url]= sub_section_name

    #get articles url
    sub_sections_html_map = url_helper.get_urls_async(list(sub_sections_url_map.keys()),
                                                      True,
                                                      configs.aapl_sim_req)
    article_links=[]
    for sub_section_url, sub_section_html in sub_sections_html_map.items():
        sub_soup =  bs(sub_section_html, 'html.parser')
        article_urls_a = sub_soup.find('div', class_='newslist-item current').find_all('a')
        for i in article_urls_a:
            if i['href'].endswith('htm'):
                tmp_url = urllib.parse.urljoin(sub_section_url, i['href'])
                tmp_name = sub_sections_url_map[sub_section_url]
                tmp_title = i.text
                article_links.append({'url': tmp_url,
                                     'title': tmp_title,
                                     'section': tmp_name})
    return article_links

def parse_article(article_html):
    article = None
    soup = bs(article_html, 'html.parser')
    content_div = soup.find('div', class_='article-content')

    #get article
    article_p = [i.text for i in content_div.find_all('p')]
    article = ''.join(article_p)
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
        db_helper.to_db(as_of_dt, configs.b81daily_coll, daily_paper)
        log.info('Finish processing news for {}'.format(as_of_dt))
    except:
        log.error('Failed processing news for {}'.format(as_of_dt))
    return daily_paper

def backfill_hp():
    start_dt = dt.datetime(2016,1,1)
    end_dt = dt.datetime.combine(dt.datetime.utcnow().date(), dt.time())
    log.info('Start backfill from {} to {}'.format(start_dt, end_dt))
    crawled_dt = db_helper.get_crawled_dt(configs.b81daily_coll)
    crawl_list = list(set(date_range(start_dt, end_dt)) - set(crawled_dt))
    crawl_list.sort(reverse=True)
    for d in crawl_list:
        process_news(d)
    log.info('Complete backfill from {} to {}'.format(start_dt, end_dt))


def test_article_links():
    config = log_config.get_config(script_name, configs.webhook_url)
    logging.config.dictConfig(config)
    log = logging.getLogger(script_name)
    as_of_dt = dt.datetime(2016, 11, 18)
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
    config = log_config.get_config(script_name, configs.webhook_url)
    logging.config.dictConfig(config)
    log = logging.getLogger(script_name)
    as_of_dt = dt.datetime(2017, 11, 7)
    papers = process_news(as_of_dt)
    print(papers)

if __name__ == '__main__':

    config = log_config.get_config(script_name, configs.webhook_url)
    logging.config.dictConfig(config)
    log = logging.getLogger(script_name)

    backfill_hp()