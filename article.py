from collections import OrderedDict
import datetime as dt

class Article:
    def __init__(self,
                 url = None,
                 title = None,
                 text = None,
                 section = None,
                 as_of_dt = None,
                 crawl_ts = None,
                 src = None):

        self.url = url
        self.title = title
        self.text = text
        self.section = section
        self.as_of_dt = as_of_dt
        self.crawl_ts = crawl_ts
        self.src = src
        self.parser_ready = True

    def __str__(self):
        return '{url} {title} {text} {section} {as_of_dt} {crawl_ts} {src}'.format(url=self.url,
                                                                                   title=self.title,
                                                                                   text = self.text,
                                                                                   section = self.section,
                                                                                   as_of_dt=self.as_of_dt,
                                                                                   crawl_ts=self.crawl_ts,
                                                                                   src = self.src)


    def export_db_fmt(self):
        docu = OrderedDict([
            ('url', self.url),
            ('title', self.title),
            ('text', self.text),
            ('section', self.section),
            ('as_of_dt', self.as_of_dt),
            ('crawl_ts', dt.datetime.utcnow()),
            ('src', self.src),
            ('parser_ready', bool(self.text))
        ])
        return docu

    def left_merge(self, right_article):
        if self.url is None:
            self.url = right_article.url
        if self.title is None:
            self.title = right_article.title
        if self.text is None:
            self.text = right_article.text
        if self.section is None:
            self.section = right_article.section
        if self.as_of_dt is None:
            self.as_of_dt = right_article.as_of_dt
        if self.crawl_ts is None:
            self.crawl_ts = right_article.crawl_ts
        if self.src is None:
            self.src = right_article.src
