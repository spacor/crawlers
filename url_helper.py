import aiohttp
import asyncio
import requests
import time
import random
from itertools import islice
import logging
import configs
import cchardet as chardet

script_name = 'url_helper'
log = logging.getLogger(script_name)

def chunk(it, size):
    it = iter(it)
    return iter(lambda: tuple(islice(it, size)), ())

async def fetch(session, url):
    t= time.monotonic()
    thushold=configs.http_timeout
    rs = None
    try:
        with aiohttp.Timeout(thushold):
            async with session.get(url) as response:
                rs = await response.text()
                time_taken = str(time.monotonic() - t)
                log.debug('OK: timeout thushold: {} time taken: {} status: {} url: {}'.format(
                    thushold, time_taken,str(response.status), url))
                # rs=rs[:15] #debug


    except Exception as e:
        time_taken = str(time.monotonic() - t)
        log.debug('Failed: timeout thushold: {} time taken: {} type: {} error: {} url: {}'.format(
            thushold, time_taken, str(type(e)), str(e),url))


    return url, rs

def get_urls_async(urls,
                   stops_btw_batch = True,
                   num_sim_req = 5,
                   stop_mean = configs.http_stop_mean,
                   stop_std = configs.http_stop_std,
                   stop_max = configs.http_stop_max):

    """
    :param urls: (list) list of url to get
    :param stops_btw_batch: (boolean) whether to stop between each batch
    :param num_sim_req: (int) number of simultaneous requests
    :return: (dict) key = url, value = html, None if error
    """
    url_batches = chunk(urls, num_sim_req)
    aggre_results = []
    t = time.monotonic()
    for ix, url_batch in enumerate(url_batches):
        log.debug('Chunk: {} Started '.format(ix))
        t_chunk = time.monotonic()
        batch_url_contents = {i: None for i in url_batch}
        for trail in range(configs.http_retry_max):
            log.debug('trail {} started'.format(trail))
            loop = asyncio.get_event_loop()
            conn = aiohttp.TCPConnector(verify_ssl=False)
            asyncio.set_event_loop(loop)
            futures = []
            with aiohttp.ClientSession(loop=loop, connector=conn) as session:
                urls_to_get = [i for i in batch_url_contents.keys() if batch_url_contents[i] is None]
                for url in urls_to_get:
                    futures.append(asyncio.ensure_future(fetch(session, url)))
                results = loop.run_until_complete(asyncio.gather(*futures))
                for i, j in results:
                    batch_url_contents[i] = j
                # aggre_results += results


            if stops_btw_batch is True:
                sleep_time = max(min(stop_max, random.gauss(stop_mean, stop_std)), 0)
                log.debug('Sleep for {} seconds'.format(sleep_time))
                time.sleep(sleep_time)

            if None in batch_url_contents.values():
                log.debug('Error in retrieving article.')
            else:
                break

        for i, j in batch_url_contents.items():
            if j is None:
                log.debug('Error in retrieving article for {} after {} retry'.format(i, configs.http_retry_max))
            else:
                aggre_results.append((i, j))

        log.debug('Chunk: {} Done in : {}'.format(ix, str(time.monotonic() - t_chunk)))
    log.debug('All Done in {}'.format(str(time.monotonic() - t)))
    url_contents = {url: contents for url, contents in aggre_results}
    return url_contents

def get_url(url):
    """
    :param url: (str) single url to get
    :return: (str) html, None if error
    """
    if isinstance(url, list):
        urls = url
    elif isinstance(url, str):
        urls = [url]
    assert isinstance(urls, list)
    results = get_urls_async(urls, False)
    return results[url]

def get_url_sync(url, headers = None):
    """
    :param url: (str) single url to get
    :return: html, None if error
    Notice that this function utilize requests, which is blocking
    """
    response = None
    for trial in range(configs.http_retry_max):

        try:
            r = requests.get(url, timeout=configs.http_timeout, headers = headers)
            if r.status_code == 200:
                response = r.content.decode(chardet.detect(r.content)['encoding'])
                break
            else:
                logging.debug('Error while requesting {}. Status Code: {}'.format(url, r.status_code))
        except requests.exceptions.Timeout:
            logging.debug('Timeout while requesting {}'.format(url))
        except requests.exceptions.RetryError:
            logging.debug('RetryError while requesting {}'.format(url))
        except requests.exceptions.InvalidURL:
            logging.debug('URL error while requesting {}'.format(url))
            break
        except Exception as e:
            logging.debug(r'Error while requesting {}. \n Error msg: {}'.format(url, e))
        if trial - 1 < configs.http_retry_max:
            stop_mean = configs.http_stop_mean
            stop_std = configs.http_stop_std
            stop_max = configs.http_stop_max
            sleep_time = max(min(stop_max, random.gauss(stop_mean, stop_std)), 0)
            logging.debug('Retry in {} seconds'.format(sleep_time))
            time.sleep(sleep_time)
        else:
            logging.debug('Retry Max reached')
    return {url: response}


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(threadName)s - %(message)s')
    urls  = [
        "https://360.cn",
        "https://chinadaily.com.cn",
        "https://cntv.cn",
        "https://gmw.cn",
        "https://googleusercontent.com",
        "https://hao123.com",
        "https://imdb.com",
        "https://live.com",
        "https://naver.com",
        "https://nicovideo.jp",
        "https://pixnet.net",
        "https://qq.com",
        "https://rakuten.co.jp",
        "https://sina.com.cn",
        "https://sohu.com",
        "https://soso.com",
        "https://t.co",
        "https://tianya.cn",
        "https://xinhuanet.com",
        "https://xvideos.com",
        "https://yahoo.co.jp",
        "https://yahoo.com",
        "https://yandex.ru",
        "http://appaaaa.com"
    ]
    # results = get_urls(urls, False, 20)
    for i in urls:
        results = get_url_sync(i)
        logging.debug(results)