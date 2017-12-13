# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/spider-middleware.html
import random
import json
import time
import datetime
import requests
from scrapy.utils.project import get_project_settings
from scrapy import signals

settings = get_project_settings()


# 随机User_Agent
class ProcessHeaderMidware():
    """process request add request info"""

    def process_request(self, request, spider):
        """
        随机从列表中获得header， 并传给user_agent进行使用
        """
        ua = random.choice(settings.get('USER_AGENT_LIST'))
        if ua:
            request.headers['User-Agent'] = ua
        pass

# 太阳HTTP代理
class TaiYangProxyMiddleware(object):
    def process_request(self, request, spider):
        # 获取settings中的几个ip列表
        proxies_queue = settings.get('IP_QUEUE')
        ip_used = settings.get('IP_USED')
        ip_blackset = settings.get('IP_BLACKSET')
        check_url = 'https://zhangbei.anjuke.com/'
        headers = {'User-Agent': "Mozilla/5.0 (X11; Linux i686; U;) Gecko/20070322 Kazehakase/0.4.5"}
        # 取出IP
        while len(proxies_queue) < 40:
            # 请求代理IP
            if len(ip_used) > 1:
                ip_reuse = ip_used.pop()
                if ip_reuse['used_time'] < time.time() - 6:
                    proxies_queue.append(ip_reuse)
                else:
                    ip_used.insert(0, ip_reuse)
            url = "http://http-api.taiyangruanjian.com/getip?num=25&type=2&pro=&city=0&yys=0&port=11&pack=7802&ts=1&ys=0&cs=0&lb=1&sb=0&pb=4&mr=1&regions="
            response = requests.get(url).content
            jo = json.loads(response)
            if jo['msg'] == '0':
                data_list = jo['data']
                for data in data_list:
                    ip = "{}:{}".format(data['ip'], data['port'])
                    proxies_ip_temp = {}
                    proxies = {'https': ip}
                    try:
                        # 检测ip有效性
                        response_ip = requests.get(check_url, headers=headers, proxies=proxies, timeout=15,
                                                   verify=False,
                                                   allow_redirects=False)
                        print(ip, response_ip.status_code)
                        if response_ip.status_code == 200:
                            proxies_ip_temp['ip'] = ip
                            proxies_ip_temp['expire_time'] = time.mktime(
                                time.strptime(data['expire_time'], '%Y-%m-%d %H:%M:%S'))
                            proxies_ip_temp['used_time'] = time.time()
                            proxies_queue.append(proxies_ip_temp)
                    except requests.exceptions.RequestException as e:
                        print(e)
                    except TypeError as e:
                        print(e)
            else:
                tomorrow = datetime.datetime.replace(datetime.datetime.now() + datetime.timedelta(days=1), hour=19,
                                                     minute=21, second=0)
                delta = tomorrow - datetime.datetime.now()
                print("Now time is %s ,Wait to 19:21 :%s seconds " % (datetime.datetime.now(), delta.seconds))
                time.sleep(delta.seconds)
        while True:
            # 随机取一个ip
            proxies_ip = random.choice(proxies_queue)
            proxies_queue.remove(proxies_ip)
            # 判断是否在黑名单
            if proxies_ip['ip'] not in ip_blackset:
                # 判断ip过期时间
                if proxies_ip['expire_time'] > time.time() + 6:
                    request.meta['proxy'] = 'https://{}'.format(proxies_ip['ip'])
                    proxies_ip['used_time'] = time.time()
                    # 将可以使用的ip插入已使用的ip列表开始位置，并从最后取出一个
                    ip_used.insert(0, proxies_ip)
                    ip_reuse = ip_used.pop()
                    if ip_reuse['used_time'] < time.time() - 6:
                        proxies_queue.append(ip_reuse)
                    else:
                        ip_used.insert(0, ip_reuse)
                    break
            else:
                ip_blackset.remove(proxies_ip['ip'])
        return None