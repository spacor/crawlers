from bs4 import BeautifulSoup as bs
import logging
import datetime as dt

script_name = 'parsers'
log = logging.getLogger(script_name)

def parser_master(parser_name, article_html, url):
    article_contents = None
    #need to add try to catch parser issue
    parser_dict = {
        'xinhua_news': xinhua,
        'qq_news': qq,
        'reuters_cn_news': reuters_cn,
        '163_news': news163,
        'eastday': eastday,
        'sohu': sohu,
        'ifeng': ifeng,
        'sina': sina,
        'china': china,
        'appledaily': appledaily,
        'orientaldaily': orientaldaily,
        'peoplesdaily': peoplesdaily,
        'pladaily': pladaily,
        'securitiestimes': securitiestimes
    }
    try:
        if parser_name in parser_dict:
            article_contents = parser_dict[parser_name](article_html)
        else:
            log.error('Article parser {} doesnt exist'.format(parser_name))
    except:
        log.error('Parser {} unable to parse article {}'.format(parser_name, url ))

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
    if soup.find('div', class_='article-body') is not None: #template 4
        content_div = soup.find('div', class_='article-body')
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

def china(article_html):
    article_contents = None
    src = 'china'
    soup = bs(article_html, 'html.parser')
    if soup.find('div', class_='navp c') is not None: #template 1
        content_div = soup.find('div', class_='navp c')
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

def appledaily(article_html):
    article=None
    src = 'appledaily'
    soup = bs(article_html, 'html.parser')
    main_content_div = soup.find('div', id='masterContent')
    article = main_content_div.text.strip()
    article_contents = {
        'title': None,
        'section': None,
        'as_of_dt': None,
        'src': src,
        'text': article
    }
    return article_contents

def orientaldaily(article_html):
    article=None
    src = 'orientaldaily'

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
    article_contents = {
        'title': None,
        'section': None,
        'as_of_dt': None,
        'src': src,
        'text': article
    }
    return article_contents

def peoplesdaily(article_html):
    src = 'peoplesdaily'
    article = None
    title = None
    soup = bs(article_html, 'html.parser')
    content_div = soup.find('div', class_='text_c')

    #get title
    try:
        title_1 = content_div.h3.text
    except:
        title_1= ''
    try:
        title_2 = content_div.h1.text
    except:
        title_2=''
    try:
        title_3 = content_div.h2.text
    except:
        title_3=''
    title = '\n'.join([title_1, title_2, title_3])

    #get article
    article_p = [i.text for i in content_div.find_all('p')]
    article = ''.join(article_p)
    article_contents = {
        'title': title,
        'section': None,
        'as_of_dt': None,
        'src': src,
        'text': article
    }
    return article_contents

def pladaily(article_html):
    src = 'pladaily'
    article = None
    soup = bs(article_html, 'html.parser')
    content_div = soup.find('div', class_='article-content')

    #get article
    article_p = [i.text for i in content_div.find_all('p')]
    article = ''.join(article_p)
    article_contents = {
        'title': None,
        'section': None,
        'as_of_dt': None,
        'src': src,
        'text': article
    }
    return article_contents

def securitiestimes(article_html):
    src = 'securitiestimes'

    article = None
    soup = bs(article_html, 'html.parser')
    content_div = soup.find('div', class_='tc_con')
    article = '\n'.join([i.text for i in content_div.find_all('p')])
    article_contents = {
        'title': None,
        'section': None,
        'as_of_dt': None,
        'src': src,
        'text': article
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