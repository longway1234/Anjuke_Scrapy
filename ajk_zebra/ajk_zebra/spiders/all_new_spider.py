# -*- coding: utf-8 -*-
import random
import re
from twisted.internet.error import DNSLookupError, TimeoutError, TCPTimedOutError
import scrapy
from copy import deepcopy
from scrapy.selector import Selector
from scrapy.spidermiddlewares.httperror import HttpError
from ajk_zebra.items import NewHouseItem


class AllNewSpiderSpider(scrapy.Spider):
    name = 'all_new_spider'
    allowed_domains = ['anjuke.com']
    with open('./spiders/city_url.txt') as f:
        city_url = set(f.readlines())
    start_urls = list(city_url)
    recall_city_urls = []
    recall_urls = []

    def parse(self, response):
        # 解析出城市列表
        selector = Selector(response)
        print(response.url)
        url_list = selector.xpath("//div[@class='letter_city']/ul/li//a/@href").extract()
        for city_url in url_list:
            print(city_url)
            yield scrapy.Request(url=city_url, callback=self.parse_new_url, errback=self.log_error,
                                 meta={"city": city_url})

    def parse_new_url(self, response):
        # 该城市解析出新楼盘的url
        selector = Selector(response)
        city_new_url = selector.xpath("//div[@class='sec_divnew div_xinfang']/a/@href").extract_first()
        while len(self.recall_urls) > 0:
            house_url = random.choice(self.recall_urls)
            self.recall_urls.remove(house_url)
            print('request recall_url')
            yield scrapy.Request(url=house_url['item']['house_url'], callback=self.parse_new_info,
                                 errback=self.log_error,
                                 meta={'item': house_url['item'], 'city_id': house_url['city_id']})
        print('recall_url is Null')
        while len(self.recall_city_urls) > 0:
            city_url = random.choice(self.recall_city_urls)
            self.recall_city_urls.remove(city_url)
            print('request recall_city_urls')
            yield scrapy.Request(url=city_url, callback=self.parse_new_houses, errback=self.log_error, )
        print('recall_city_urls is Null')
        if city_new_url is not None:
            yield scrapy.Request(url=city_new_url, callback=self.parse_new_houses, errback=self.log_error,
                                 meta={"city": city_new_url})

    def parse_new_houses(self, response):
        # 获得该城市下的新楼盘列表
        city_new_url = response.meta['city']
        selector = Selector(response)
        city = selector.xpath("//div[@class='sel-city']/a/span[@class='city']/text()").extract_first()
        div_list = selector.xpath("//div[@class='list-results']/div[@class='key-list'][1]/div[@class='item-mod']")
        if div_list is None:
            self.recall_city_urls.append(city_new_url)
        else:
            for resold_house in div_list:
                # 解析出单个小区信息
                item = NewHouseItem()
                item['city_name'] = city.split()[0]
                house_url = resold_house.xpath("./@data-link").extract_first()
                city_id = re.findall(r'https://(.*?).fang.anjuke.com/loupan/(.*?).html', house_url)[0]
                item['house_url'] = "https://{}.fang.anjuke.com/loupan/canshu-{}.html".format(city_id[0], city_id[1])
                item['house_title'] = resold_house.xpath("./div[@class='infos']//h3/span/text()").extract_first()
                item['property_type'] = resold_house.xpath(
                    "./div[@class='infos']//i[@class='status-icon wuyetp']/text()").extract_first()
                print(item['house_url'])
                yield scrapy.Request(url=item['house_url'], callback=self.parse_new_info,
                                     meta={"item": deepcopy(item), "city_id": deepcopy(city_id)})
        next_url = selector.xpath("//div[@class='list-page']//a[contains(@class,'next-page')]/@href").extract_first()
        if next_url is not None:
            yield scrapy.Request(url=next_url, callback=self.parse_new_houses, meta={"city": next_url})

    def parse_new_info(self, response):
        # 解析楼盘详情页的信息
        selector = Selector(response)
        item = deepcopy(response.meta["item"])
        city_id = deepcopy(response.meta["city_id"])
        house_address = selector.xpath(u"//div[text()='楼盘地址']/following-sibling::div[1]/text()").extract_first()
        if house_address is None:
            self.recall_urls.append({'item': deepcopy(item), 'city_id': city_id})
        else:
            item['house_address'] = house_address
            item['feature'] = selector.xpath(u"//div[text()='楼盘特点']/following-sibling::div[1]/a/text()").extract()
            reference_price = selector.xpath(
                u"//div[text()='参考单价']/following-sibling::div[1]/span/text()").extract_first()
            if reference_price is not None:
                item['reference_price'] = reference_price.split()[0] + u"元/㎡"
            total_price = selector.xpath(u"//div[text()='楼盘总价']/following-sibling::div[1]/span/text()").extract_first()
            if total_price is not None:
                item['total_price'] = total_price.split()[0] + u"万元/套起"
            item['developers'] = selector.xpath(
                u"//div[text()='开发商']/following-sibling::div[1]/a/text()").extract_first()

            item['sales_telephone'] = selector.xpath(
                u"//div[text()='售楼处电话']/following-sibling::div[1]/span/text()").extract_first()
            min_payment = selector.xpath(u"//div[text()='最低首付']/following-sibling::div[1]/text()").extract_first()
            if min_payment is not None:
                item['min_payment'] = min_payment.split()[0]
            sales_date = selector.xpath(u"//div[text()='最新开盘']/following-sibling::div[1]/text()").extract_first()
            if sales_date is not None:
                item['sales_date'] = sales_date.split()[0]

            completion_date = selector.xpath(
                u"//div[text()='交房时间']/following-sibling::div[1]/text()").extract_first()
            if completion_date is not None:
                item['completion_date'] = completion_date.strip()
            item['sales_address'] = selector.xpath(
                u"//div[text()='售楼处地址']/following-sibling::div[1]/text()").extract_first()
            item['building_type'] = selector.xpath(
                u"//div[text()='建筑类型']/following-sibling::div[1]/text()").extract_first()
            planning_number = selector.xpath(
                u"//div[text()='规划户数']/following-sibling::div[1]/text()").extract_first()
            if planning_number is not None:
                item['planning_number'] = planning_number.split()[0]
            item['property_years'] = selector.xpath(
                u"//div[text()='产权年限']/following-sibling::div[1]/text()[1]").extract_first()
            plot_ratio = selector.xpath(
                u"//div[text()='容积率']/following-sibling::div[1]/text()[1]").extract_first()
            if plot_ratio is not None:
                item['plot_ratio'] = plot_ratio.split()[0]
            item['greening_rate'] = selector.xpath(
                u"//div[text()='绿化率']/following-sibling::div[1]/text()[1]").extract_first()
            progress_works = selector.xpath(
                u"//div[text()='工程进度']/following-sibling::div[1]/text()[1]").extract_first()
            if progress_works is not None:
                item['progress_works'] = progress_works.split()[0]
            item['property_price'] = selector.xpath(
                u"//div[text()='物业管理费']/following-sibling::div[1]/text()").extract_first()
            item['property_company'] = selector.xpath(
                u"//div[text()='物业公司']/following-sibling::div[1]/a/text()").extract_first()
            item['parking_number'] = selector.xpath(
                u"//div[text()='车位数']/following-sibling::div[1]/text()").extract_first()
            parking_rate = selector.xpath(
                u"//div[text()='车位比']/following-sibling::div[1]/text()").extract_first()
            if parking_rate is not None and len(parking_rate.split()) > 0:
                item['parking_rate'] = parking_rate.split()[0]
            map_url = u"https://m.anjuke.com/{}/loupan/{}/".format(city_id[0], city_id[1])
            yield scrapy.Request(url=map_url, callback=self.parse_map_info,
                                 meta={"item": deepcopy(item)})

    def parse_map_info(self, response):
        # 解析地图信息
        item = deepcopy(response.meta['item'])
        map_url = response.selector.xpath("//div[@class='lpinfo']/a[@class='wui-line']/@href").extract_first()
        if map_url is not None:
            lng_lat = re.findall(r".*?lng=(.*?)&lat=(.*?)&id=.*?", map_url)
            if len(lng_lat) > 0:
                item['map_lng'] = lng_lat[0][0]
                item['map_lat'] = lng_lat[0][1]
        return item

    def log_error(self, failure):
        if failure.check(HttpError):
            response = failure.value.response
            self.logger.error('HttpError on %s', response.url)
        elif failure.check(DNSLookupError):
            request = failure.request
            self.logger.error('DNSLookupError on %s', request.url)
        elif failure.check(TimeoutError, TCPTimedOutError):
            request = failure.request
            self.logger.error('TimeoutError on %s', request.url)
        else:
            pass
