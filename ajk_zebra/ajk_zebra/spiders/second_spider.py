# -*- coding: utf-8 -*-
import scrapy
import logging
import re
from scrapy import Request
from scrapy.selector import Selector
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import TimeoutError, TCPTimedOutError, DNSLookupError
from ajk_zebra.items import ResoldHouseItem
from scrapy.utils.project import get_project_settings

logger = logging.getLogger(__name__)
settings = get_project_settings()


class SecondSpiderSpider(scrapy.Spider):
    name = 'second_spider'
    custom_settings = {
        'ITEM_PIPELINES': {
            'ajk_zebra.pipelines.ResoldHousePipeline': 300,
            'ajk_zebra.pipelines.ResoldHouseUrlPipeline': 301
        },
        'LOG_FILE': './logs/resold_second.log',
        'LOG_FORMAT': '%(name)s-%(levelname)s: %(message)s',
        'LOG_LEVEL': 'ERROR'
    }

    allowed_domains = ['anjuke.com']
    # 拿出小区数量小于1500的城市列表页的首页url
    with open('./data/city_second_url.txt') as f:
        city_url = f.readlines()
    start_urls = [url.strip() for url in city_url]
    # 拿出请求过的小区详情页的url
    with open('./data/resold_url.txt') as f:
        house_url = f.readlines()
    house_urls = [url.strip() for url in house_url]
    ip_blackset = settings.get('IP_BLACKSET')
    num_black = 0  # 出现验证码的次数

    def start_requests(self):
        for start_url in self.start_urls:
            city_url = start_url.split()[0]
            if city_url is not None:
                yield Request(city_url, self.parse, meta={'request_url': city_url})

    def parse(self, response):
        #  解析该城市下的首页小区列表
        city_house_url = response.meta['request_url']
        selector = Selector(response)
        print(city_house_url, response.url)
        house_num = selector.xpath("//div[@class='sortby']/span/em[2]/text()").extract_first()
        # 判断是否出现验证码，是则重发请求
        if house_num is None:
            self.ip_blackset.add(response.meta['proxy'].replace(r'https://', ''))
            print(self.ip_blackset)
            self.num_black += 1
            logger.error('had show verification code %s times' % self.num_black)
            yield scrapy.Request(url=city_house_url, callback=self.parse, dont_filter=True,
                                 errback=self.log_error, meta={'request_url': city_house_url})
        elif int(house_num) == 0:
            return
        else:
            div_list = selector.xpath("//div[@id='list-content']/div[@class='li-itemmod']")
            for resold_house in div_list:
                # 解析出单个小区信息
                house_url = 'https://anjuke.com' + resold_house.xpath(
                    "./div[@class='li-info']/h3/a/@href").extract_first()
                # 判断小区url是否在请求过的url列表中
                if house_url not in self.house_urls:
                    avg_price = resold_house.xpath("./div[@class='li-side']/p/strong/text()").extract_first()
                    chain_month = resold_house.xpath("./div[@class='li-side']/p[2]/text()").extract_first()
                    resold_number = resold_house.xpath("./div//p[2]/span/a/text()").extract_first()
                    yield scrapy.Request(url=house_url, callback=self.parse_resold_house_info,
                                         errback=self.log_error, meta={'request_url': house_url, 'avg_price': avg_price,
                                                                       'chain_month': chain_month,
                                                                       'resold_number': resold_number})
                else:
                    print('already exist!')
            next_url = selector.xpath(
                "//div[@class = 'page-content']//a[contains(@class,'aNxt')]/@href").extract_first()
            if next_url is not None:
                yield scrapy.Request(url=next_url, callback=self.parse_next, errback=self.log_error,
                                     meta={'request_url': next_url})

    def parse_next(self, response):
        #  解析该城市下的下一页小区列表
        city_house_url = response.meta['request_url']
        selector = Selector(response)
        print(city_house_url, response.url)
        house_num = selector.xpath("//div[@class='sortby']/span/em[2]/text()").extract_first()
        # 判断是否出现验证码，是则重发请求
        if house_num is None:
            self.ip_blackset.add(response.meta['proxy'].replace(r'https://', ''))
            print(self.ip_blackset)
            self.num_black += 1
            logger.error('had show verification code %s times' % self.num_black)
            yield scrapy.Request(url=city_house_url, callback=self.parse, dont_filter=True,
                                 errback=self.log_error, meta={'request_url': city_house_url})
        elif int(house_num) == 0:
            return
        else:
            div_list = selector.xpath("//div[@id='list-content']/div[@class='li-itemmod']")
            for resold_house in div_list:
                # 解析出单个小区信息
                house_url = 'https://anjuke.com' + resold_house.xpath(
                    "./div[@class='li-info']/h3/a/@href").extract_first()
                # 判断小区url是否在请求过的url列表中
                if house_url not in self.house_urls:
                    avg_price = resold_house.xpath("./div[@class='li-side']/p/strong/text()").extract_first()
                    chain_month = resold_house.xpath("./div[@class='li-side']/p[2]/text()").extract_first()
                    resold_number = resold_house.xpath("./div//p[2]/span/a/text()").extract_first()
                    yield scrapy.Request(url=house_url, callback=self.parse_resold_house_info,
                                         errback=self.log_error, meta={'request_url': house_url, 'avg_price': avg_price,
                                                                       'chain_month': chain_month,
                                                                       'resold_number': resold_number})
                else:
                    print('already exist!')
            next_url = selector.xpath(
                "//div[@class = 'page-content']//a[contains(@class,'aNxt')]/@href").extract_first()
            if next_url is not None:
                yield scrapy.Request(url=next_url, callback=self.parse_next, errback=self.log_error,
                                     meta={'request_url': next_url})

    def parse_resold_house_info(self, response):
        # 解析小区详情页的信息
        house_url = response.meta['request_url']
        avg_price = response.meta['avg_price']
        chain_month = response.meta['chain_month']
        resold_number = response.meta['resold_number']
        print(house_url, response.url)
        selector = Selector(response)
        house_title = selector.xpath("//div[@class='comm-title']/h1/text()").extract_first()
        # 判断是否出现验证码，是则重发请求
        if house_title is None:
            self.ip_blackset.add(response.meta['proxy'].replace(r'https://', ''))
            print(self.ip_blackset)
            self.num_black += 1
            logger.error('had show verification code %s times' % self.num_black)
            yield scrapy.Request(url=house_url, callback=self.parse_resold_house_info, dont_filter=True,
                                 errback=self.log_error, meta={'request_url': house_url, 'avg_price': avg_price,
                                                               'chain_month': chain_month,
                                                               'resold_number': resold_number})
        else:
            item = ResoldHouseItem()
            item['city_name'] = selector.xpath("//div[@id='switch_apf_id_5']/text()") \
                .extract_first().replace('\n', '').replace(' ', '')
            map_url = selector.xpath("//div[@class='comm-title']/a/@href").extract_first()
            if map_url is not None:
                lng_lat = re.findall(r".*?l1=(.*?)&l2=(.*?)&l3=.*?", map_url)
                if len(lng_lat) > 0:
                    item['map_lng'] = lng_lat[0][1]
                    item['map_lat'] = lng_lat[0][0]
            item['house_url'] = house_url
            item['house_title'] = house_title.strip()
            item['avg_price'] = avg_price
            item['chain_month'] = chain_month
            item['building_years'] = selector.xpath(
                u"//dt[text()='建造年代：']/following-sibling::dd[1]/text()").extract_first()
            item['resold_number'] = resold_number
            item['house_address'] = selector.xpath("//div[@class='comm-title']/h1/span/text()").extract_first()
            item['property_type'] = selector.xpath("//dl[@class='basic-parms-mod']/dd[1]/text()").extract_first()
            item['property_price'] = selector.xpath("//dl[@class='basic-parms-mod']/dd[2]/text()").extract_first()
            item['total_area'] = selector.xpath("//dl[@class='basic-parms-mod']/dd[3]/text()").extract_first()
            item['total_houses'] = selector.xpath("//dl[@class='basic-parms-mod']/dd[4]/text()").extract_first()
            item['parking_number'] = selector.xpath("//dl[@class='basic-parms-mod']/dd[6]/text()").extract_first()
            item['plot_ratio'] = selector.xpath("//dl[@class='basic-parms-mod']/dd[7]/text()").extract_first()
            item['greening_rate'] = selector.xpath("//dl[@class='basic-parms-mod']/dd[8]/text()").extract_first()
            item['developers'] = selector.xpath("//dl[@class='basic-parms-mod']/dd[9]/text()").extract_first()
            item['property_company'] = selector.xpath("//dl[@class='basic-parms-mod']/dd[10]/text()").extract_first()
            item['detail_community'] = selector.xpath(
                "//div[contains(@class,'comm-brief-mod')]/p/text()").extract_first()
            yield item

    def log_error(self, failure):
        if failure.check(HttpError):
            request = failure.request
            self.logger.error('HttpError on %s', request.url)
        elif failure.check(DNSLookupError):
            request = failure.request
            self.logger.error('DNSLookupError on %s', request.url)
        elif failure.check(TimeoutError, TCPTimedOutError):
            request = failure.request
            if 'anjuke.com/community/view/' in request.url:
                house_url = request.meta['request_url']
                avg_price = request.meta['avg_price']
                chain_month = request.meta['chain_month']
                resold_number = request.meta['resold_number']
                yield scrapy.Request(url=house_url, callback=self.parse_resold_house_info, dont_filter=True,
                                     errback=self.log_error, meta={'request_url': house_url, 'avg_price': avg_price,
                                                                   'chain_month': chain_month,
                                                                   'resold_number': resold_number})
            else:
                self.logger.error('TimeoutError on %s', request.url)
                self.ip_blackset.add(request.meta['proxy'].replace(r'https://', ''))
                print(self.ip_blackset)
        else:
            pass
