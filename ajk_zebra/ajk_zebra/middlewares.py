# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/spider-middleware.html
import hashlib
from scrapy import signals
import time
import json
import requests
import random
from scrapy.utils.project import get_project_settings

settings = get_project_settings()



class AjkZebraSpiderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, dict or Item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Response, dict
        # or Item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class ProxyMiddleware(object):

    # 太阳HTTP代理
    def process_request(self, request, spider):
        # 获取settings中的几个ip列表
        proxies_queue = settings.get('IP_QUEUE')
        ip_used = settings.get('IP_USED')
        ip_blackset = settings.get('IP_BLACKSET')
        check_url = 'https://ali.anjuke.com/'
        headers = {'User-Agent': "Mozilla/5.0 (X11; Linux i686; U;) Gecko/20070322 Kazehakase/0.4.5"}
        while len(ip_used) > 50:
            ip_reuse = ip_used.pop()
            if ip_reuse['used_time'] < time.time() - 1:
                proxies_queue.append(ip_reuse)
            else:
                ip_used.insert(0, ip_reuse)
        # 取出IP
        while len(proxies_queue) < 75:
            # 请求代理IP
            url = "http://http-api.taiyangruanjian.com/getip?num=25&type=2&pro=&city=0&yys=0&port=11&pack=7802&ts=1&ys=0&cs=0&lb=1&sb=0&pb=4&mr=1&regions="
            response = requests.get(url).content
            data_list = json.loads(response)['data']
            for data in data_list:
                ip = "{}:{}".format(data['ip'], data['port'])
                proxies_ip_temp = {}
                proxies = {'https': ip}
                try:
                    # 检测ip有效性
                    response_ip = requests.get(check_url, headers=headers, proxies=proxies, timeout=15, verify=False,
                                               allow_redirects=False)
                    print(response_ip.status_code)
                    if response_ip.status_code == 200:
                        print(ip)
                        proxies_ip_temp['ip'] = ip
                        proxies_ip_temp['expire_time'] = time.mktime(
                            time.strptime(data['expire_time'], '%Y-%m-%d %H:%M:%S'))
                        proxies_ip_temp['used_time'] = time.time()
                        proxies_queue.append(proxies_ip_temp)
                except requests.exceptions.RequestException as e:
                    print(e)
                except TypeError as e:
                    print(e)
        while True:
            # 随机取一个ip
            proxies_ip = random.choice(proxies_queue)
            proxies_queue.remove(proxies_ip)
            # 判断是否在黑名单
            if proxies_ip['ip'] not in ip_blackset:
                # 判断ip过期时间
                if proxies_ip['expire_time'] > time.time() + 3:
                    request.meta['proxy'] = 'https://{}'.format(proxies_ip['ip'])
                    proxies_ip['used_time'] = time.time()
                    # 将可以使用的ip插入已使用的ip列表开始位置，并从最后取出一个
                    ip_used.insert(0, proxies_ip)
                    ip_reuse = ip_used.pop()
                    if ip_reuse['used_time'] < time.time() - 1:
                        proxies_queue.append(ip_reuse)
                    else:
                        ip_used.insert(0, ip_reuse)
                    break
            else:
                ip_blackset.remove(proxies_ip['ip'])
        return None


class ProcessHeaderMidware():
    """process request add request info"""

    def process_request(self, request, spider):
        """
        随机从列表中获得header， 并传给user_agent进行使用
        """
        ua = random.choice(settings.get('USER_AGENT_LIST'))
        # spider.logger.info(msg='now entring download midware')
        if ua:
            request.headers['User-Agent'] = ua
            # Add desired logging message here.
            # spider.logger.info(u'User-Agent is : {} {}'.format(request.headers.get('User-Agent'), request))
        pass
