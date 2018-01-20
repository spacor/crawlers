import logging
import logging.config
from bs4 import BeautifulSoup as bs
import datetime as dt
import urllib.parse

import url_helper
from article import Article

script_name = 'newspaper_ix_parser'
log = logging.getLogger(script_name)

def appledaily(as_of_dt):
    src_name = 'appledaily'
    log.debug('Getting {} index page for date {}'.format(src_name, as_of_dt))
    dt_fmt = '%Y%m%d'
    path_template = r'https://hk.appledaily.com/archive/index/{}'
    path = path_template.format(as_of_dt.strftime(dt_fmt))
    ix_page = url_helper.get_url_sync(path)
    soup = bs(ix_page[path], 'html.parser')
    archive_div = soup.find('div', class_='Archive')
    #need to add checking see if this is ix page

    #find article type mapping; need to add try
    title_div = archive_div.find_all('div', class_='title')
    url_title_map = {}
    for section in title_div:
        section_name = section.text
        section_urls = section.next_sibling.next_sibling.find_all('a')
        for section_url in section_urls:
            url_title_map[section_url['href']] = section_name

    #find all articule link
    raw_article_links = archive_div.find_all('a')
    articles = []
    for lk in raw_article_links:
        tmp_url = lk['href']
        tmp_title = lk.text
        tmp_section = url_title_map[tmp_url] #need to add try
        if bool(tmp_title):
            articles.append(Article(url=tmp_url,
                                    title = tmp_title,
                                    section = tmp_section,
                                    as_of_dt=as_of_dt,
                                    crawl_ts=dt.datetime.utcnow(),
                                    src = src_name
                                    )
                            )
    return articles

def orientaldaily(as_of_dt):
    src_name = 'orientaldaily'
    log.debug('Getting {} index page for date {}'.format(src_name, as_of_dt))
    dt_fmt = '%Y%m%d'
    path_template = r'http://orientaldaily.on.cc/cnt/sitemap/{}/index.html'
    path = path_template.format(as_of_dt.strftime(dt_fmt))
    ix_page = url_helper.get_url_sync(path)
    soup = bs(ix_page[path], 'html.parser')

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
    articles = []
    base = r'http://orientaldaily.on.cc'
    for url, title in url_title_map.items():
        tmp_url = urllib.parse.urljoin(base, url)
        tmp_section = url_section_map[url]
        articles.append(Article(url=tmp_url,
                                title=title,
                                section=tmp_section,
                                as_of_dt=as_of_dt,
                                crawl_ts=dt.datetime.utcnow(),
                                src=src_name
                                )
                        )
    return articles

def peoplesdaily(as_of_dt):
    src_name = 'peoplesdaily'
    log.debug('Getting {} index page for date {}'.format(src_name, as_of_dt))
    path_template = r'http://paper.people.com.cn/rmrb/html/{}/{}/nbs.D110000renmrb_01.htm'
    path = path_template.format(as_of_dt.strftime(r'%Y-%m'), as_of_dt.strftime(r'%d'))
    ix_page = url_helper.get_url_sync(path)
    soup = bs(ix_page[path], 'html.parser')

    #get sub-section url
    subsection_div = soup.find('div', id='pageList').find_all('a')
    sub_sections_url_map = {}
    for i in subsection_div:
        if i['href'].endswith('htm'):
            sub_section_url = urllib.parse.urljoin(path, i['href'])
            sub_section_name = i.text
            sub_sections_url_map[sub_section_url]= sub_section_name

    #get articles url
    articles=[]
    for sub_section_url, sub_section_name in sub_sections_url_map.items():
        sub_section_html = url_helper.get_url_sync(sub_section_url)
        sub_soup =  bs(sub_section_html[sub_section_url], 'html.parser')
        article_urls_a = sub_soup.find('div', id='titleList').find_all('a')
        for i in article_urls_a:
            if i['href'].endswith('htm'):
                tmp_url = urllib.parse.urljoin(sub_section_url, i['href'])
                articles.append(Article(url=tmp_url,
                                        title=None,
                                        section=sub_section_name,
                                        as_of_dt=as_of_dt,
                                        crawl_ts=dt.datetime.utcnow(),
                                        src=src_name
                                        )
                                )
    return articles

def pladaily(as_of_dt):
    src_name = 'pladaily'
    log.debug('Getting {} index page for date {}'.format(src_name, as_of_dt))

    path_template = r'http://www.81.cn/jfjbmap/content/{}/{}/node_2.htm'
    path = path_template.format(as_of_dt.strftime(r'%Y-%m'), as_of_dt.strftime(r'%d'))
    ix_page = url_helper.get_url_sync(path)
    soup = bs(ix_page[path], 'html.parser')

    #get sub-section url
    subsection_div = soup.find('div', class_='col-md-4-10 channel-list').find_all('a')
    sub_sections_url_map = {}
    for i in subsection_div:
        if i['href'].endswith('htm'):
            sub_section_url = urllib.parse.urljoin(path, i['href'])
            sub_section_name = i.text
            sub_sections_url_map[sub_section_url]= sub_section_name

    #get articles url

    articles=[]
    for sub_section_url, sub_section_name in sub_sections_url_map.items():
        sub_section_html = url_helper.get_url_sync(sub_section_url)
        sub_soup = bs(sub_section_html[sub_section_url], 'html.parser')
        article_urls_a = sub_soup.find('div', class_='newslist-item current').find_all('a')
        for i in article_urls_a:
            if i['href'].endswith('htm'):
                tmp_url = urllib.parse.urljoin(sub_section_url, i['href'])
                tmp_title = i.text
                articles.append(Article(url=tmp_url,
                                        title=tmp_title,
                                        section=sub_section_name,
                                        as_of_dt=as_of_dt,
                                        crawl_ts=dt.datetime.utcnow(),
                                        src=src_name
                                        )
                                )
    return articles


def securitiestimes(as_of_dt):
    src_name = 'securitiestimes'
    log.debug('Getting {} index page for date {}'.format(src_name, as_of_dt))
    path_template = r'http://epaper.stcn.com/paper/zqsb/html/{}/{}/node_2.htm'
    path = path_template.format(as_of_dt.strftime(r'%Y-%m'), as_of_dt.strftime(r'%d'))
    if as_of_dt.weekday != 6:
        ix_page = url_helper.get_url_sync(path)
        soup = bs(ix_page[path], 'html.parser')
        article_links = None
        #get articles urls
        section_div = soup.find('div', id='webtree')
        sub_sections_div = section_div.find_all('dl')
        articles = []
        for sub_section in sub_sections_div:
            sub_section_name = sub_section.dt.a.text
            for article_link_section in sub_section.find_all('li'):
                tmp_article_url = urllib.parse.urljoin(path, article_link_section.a['href'])
                tmp_article_title = article_link_section.a.text
                articles.append(Article(url=tmp_article_url,
                                        title=tmp_article_title,
                                        section=sub_section_name,
                                        as_of_dt=as_of_dt,
                                        crawl_ts=dt.datetime.utcnow(),
                                        src=src_name
                                        )
                                )
    else:
        articles = []
    return articles

def ix_test():
    as_of_dt = dt.datetime(2017,12,20)
    articles = securitiestimes(as_of_dt)
    print(articles)

if __name__ == '__main__':
    ix_test()