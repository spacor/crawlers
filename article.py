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

    # def update_field(self, field_name, value):
    #     field_map = {'url': self.url,
    #                  'title': self.title,
    #                  'text': self.text,
    #                  'section': self.section,
    #                  'as_of_dt': self.as_of_dt,
    #                  'crawl_ts': self.crawl_ts,
    #                  'src': self.src}
    #
    #     if field_name in field_map.keys():
    #         if value is not None:
    #             field_map[field_name] = value

    def export_db_fmt(self):
        docu = OrderedDict([
            ('url', self.url),
            ('title', self.title),
            ('text', self.text),
            ('section', self.section),
            ('as_of_dt', self.as_of_dt),
            ('crawl_ts', dt.datetime.utcnow()),
            ('src', self.src),
            ('parser_ready', self.parser_ready)
        ])
        return docu