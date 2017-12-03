from bs4 import BeautifulSoup as bs
import logging
import datetime as dt

script_name = 'parsers'
log = logging.getLogger(script_name)

def parser_master(parser_name, article_html, url):
    article_contents = None
    #need to add try to catch parser issue
    try:
        if parser_name == 'xinhua_news':
            article_contents = xinhua(article_html)
        elif parser_name == 'qq_news':
            article_contents = qq(article_html)
        elif parser_name == 'reuters_cn_news':
            article_contents = reuters_cn(article_html)
        elif parser_name == '163_news':
            article_contents = news163(article_html)
        elif parser_name == 'eastday':
            article_contents = eastday(article_html)
        elif parser_name == 'sohu':
            article_contents = sohu(article_html)
        elif parser_name == 'ifeng':
            article_contents = ifeng(article_html)
        elif parser_name == 'sina':
            article_contents = sina(article_html)
        else:
            log.error('Article parser {} doesnt exist'.format(parser_name))
    except:
        log.error('Parser {} unable to parse article {}'.format(parser_name, url ))

    # if article_contents is None:
    #     article_contents = {
    #         'title': None,
    #         'section': None,
    #         'as_of_dt': None,
    #         'src': None,
    #         'text': None
    #     }
    return article_contents

def xinhua(article_html):
    src = 'xinhua'
    article_contents = None
    tmp_as_of_dt= None
    soup = bs(article_html, 'html.parser')


    #find main article
    content_div = None
    if soup.find('div', id='p-detail') is not None: #template 1
        content_div = soup.find('div', id='p-detail')
    elif soup.find('div', id='content') is not None:  # template 2
        content_div = soup.find('div', id='content')
    elif soup.find('div', class_='article') is not None:  # template 3
        content_div = soup.find('div', class_='article')
    else:
        log.debug('Failed to find main article')
    if content_div is not None:
        article_text_p = []
        for i in content_div.find_all('p'):
            article_text_p.append(i.text)


    if soup.find('span', class_='h-time') is not None:
        publish_time_span = soup.find('span', class_='h-time')
        dt_fmt = '%Y-%m-%d %H:%M:%S'
        tmp_as_of_dt = dt.datetime.strptime(publish_time_span.text.strip(), dt_fmt)
    elif soup.find('span', id='pubtime') is not None:
        publish_time_span = soup.find('span', id='pubtime')
        dt_fmt = '%Y年%m月%d日 %H:%M:%S'
        tmp_as_of_dt = dt.datetime.strptime(publish_time_span.text.strip(), dt_fmt)
    else:
        log.debug('Failed to find as of date')
    article_contents = {
        'title': None,
        'section': None,
        'as_of_dt': tmp_as_of_dt,
        'src': src,
        'text': '\n'.join(article_text_p)
    }
    return article_contents

def qq(article_html):
    src = 'qq'
    article_contents = None
    soup = bs(article_html, 'html.parser')
    if soup.find('div', class_='qq_article') is not None: #template 1
        content_div = soup.find('div', class_='qq_article')
    elif soup.find('div', class_='content-article') is not None: #template 2
        content_div =soup.find('div', class_='content-article')
    elif soup.find('div', id='C-Main-Article-QQ') is not None: #template 3
        content_div = soup.find('div', id='C-Main-Article-QQ')
    else:
        log.debug('Failed to find main article')
    article_text_p = []
    for i in content_div.find_all('p'):
        article_text_p.append(i.text)
    article_contents = {
        'title': None,
        'section': None,
        'as_of_dt': None,
        'src': src,
        'text': '\n'.join(article_text_p)
    }
    return article_contents

def reuters_cn(article_html):
    article_contents = None
    src = 'reuter_cn'
    soup = bs(article_html, 'html.parser')
    content_div = soup.find('div', class_='ArticleBody_body_2ECha')
    article_text_p = []
    for i in content_div.find_all('p'):
        article_text_p.append(i.text)
    article_contents = {
        'title': None,
        'section': None,
        'as_of_dt': None,
        'src': src,
        'text': '\n'.join(article_text_p)
    }
    return article_contents

def news163(article_html):
    article_contents = None
    src = '163'
    soup = bs(article_html, 'html.parser')
    if soup.find('div', class_='post_text') is not None: #template 1
        content_div = soup.find('div', class_='post_text')
    if soup.find('div', class_='post_content_main') is not None: #template 2
        content_div = soup.find('div', class_='post_content_main')
    if soup.find('div', class_='post_body') is not None: #template 3
        content_div = soup.find('div', class_='post_body')
    else:
        log.debug('Failed to find main article')
    article_text_p = []
    for i in content_div.find_all('p'):
        article_text_p.append(i.text)
    article_contents = {
        'title': None,
        'section': None,
        'as_of_dt': None,
        'src': src,
        'text': '\n'.join(article_text_p)
    }
    return article_contents

def eastday(article_html):
    article_contents = None
    src = 'eastday'
    soup = bs(article_html, 'html.parser')
    if soup.find('div', id='content') is not None: #template 1
        content_div = soup.find('div', id='content')
    else:
        log.debug('Failed to find main article')
    article_text_p = []
    for i in content_div.find_all('p'):
        article_text_p.append(i.text)
    article_contents = {
        'title': None,
        'section': None,
        'as_of_dt': None,
        'src': src,
        'text': '\n'.join(article_text_p)
    }
    return article_contents

def sohu(article_html):
    article_contents = None
    src = 'sohu'
    soup = bs(article_html, 'html.parser')
    if soup.find('article', class_='article') is not None: #template 1
        content_div = soup.find('article', class_='article')
    else:
        log.debug('Failed to find main article')
    article_text_p = []
    for i in content_div.find_all('p'):
        article_text_p.append(i.text)
    article_contents = {
        'title': None,
        'section': None,
        'as_of_dt': None,
        'src': src,
        'text': '\n'.join(article_text_p)
    }
    return article_contents

def ifeng(article_html):
    article_contents = None
    src = 'ifeng'
    soup = bs(article_html, 'html.parser')
    if soup.find('div', id='main_content') is not None: #template 1
        content_div = soup.find('div', id='main_content')
    else:
        log.debug('Failed to find main article')
    article_text_p = []
    for i in content_div.find_all('p'):
        article_text_p.append(i.text)
    article_contents = {
        'title': None,
        'section': None,
        'as_of_dt': None,
        'src': src,
        'text': '\n'.join(article_text_p)
    }
    return article_contents

def sina(article_html):
    article_contents = None
    src = 'sina'
    soup = bs(article_html, 'html.parser')
    # if soup.find('div', id='main_content') is not None: #template 1
    #     content_div = soup.find('div', id='main_content')
    # else:
    #     log.debug('Failed to find main article')
    content_div = soup
    article_text_p = []
    for i in content_div.find_all('p'):
        article_text_p.append(i.text)
    article_contents = {
        'title': None,
        'section': None,
        'as_of_dt': None,
        'src': src,
        'text': '\n'.join(article_text_p)
    }
    return article_contents

def test_xh_parser():
    import url_helper
    url = r'http://news.xinhuanet.com/politics/2017-11/29/c_1122032030.htm'
    contents = url_helper.get_url_sync(url)

    parsed = xinhua(contents[url])
    print(parsed)

def test_qq_parser():
    import url_helper
    url = r'http://news.qq.com/a/20171125/008910.htm'
    contents = url_helper.get_url_sync(url)

    parsed = qq(contents[url])
    print(parsed)

if __name__ == '__main__':
    test_qq_parser()