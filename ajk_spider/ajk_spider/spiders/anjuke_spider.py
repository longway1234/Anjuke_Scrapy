# -*- coding: utf-8 -*-
import re
import scrapy
from copy import deepcopy
from scrapy import Selector
from scrapy.spidermiddlewares.httperror import HttpError
from scrapy.utils.project import get_project_settings
from twisted.internet.error import DNSLookupError, TimeoutError, TCPTimedOutError

from ajk_spider.items import NewHouseItem, ResoldHouseItem, CityAvgItem


class AnjukeSpiderSpider(scrapy.Spider):
    name = 'anjuke_spider'
    custom_settings = {
        'ITEM_PIPELINES': {
            'ajk_spider.pipelines.NewHousePipeline': 300,
            'ajk_spider.pipelines.ResoldHousePipeline': 300,
            'ajk_spider.pipelines.CityAvgPricePipeline': 300,
        },
        'DOWNLOADER_MIDDLEWARES': {
            'ajk_spider.middlewares.ProcessHeaderMidware': 111,
            # 'ajk_spider.middlewares.TaiYangProxyMiddleware': 110,
        },
        'CONCURRENT_REQUESTS': '1',
        'CONCURRENT_REQUESTS_PER_IP': '1',
        'DOWNLOAD_DELAY': '1.5',
        'LOG_FILE': './logs/anjuke.log',
        'LOG_FORMAT': '%(name)s-%(levelname)s: %(message)s',
        'LOG_LEVEL': 'ERROR'
    }
    allowed_domains = ['anjuke.com']
    start_urls = ['https://www.anjuke.com/sy-city.html']
    settings = get_project_settings()
    ip_blackset = settings.get('IP_BLACKSET')
    # 拿出请求过的小区详情页的url
    new_urls = []
    try:
        for line in open('./data/new.csv', 'r'):
            fields = line.split(',')
            new_urls.append(fields[23])
    except IOError as e:
        print(e)
    resold_urls = []
    try:
        for line in open('./data/resold.csv', 'r'):
            fields = line.split(',')
            resold_urls.append(fields[16])
    except IOError as e:
        print(e)
    num_black = 0  # 出现验证码的次数

    def parse(self, response):
        # 解析出城市列表
        selector = Selector(response)
        url_list = selector.xpath("//div[@class='letter_city']/ul/li//a/@href").extract()
        for city_url in url_list[0:383]:
            # 获取新楼盘的url
            yield scrapy.Request(url=city_url, callback=self.parse_new_url, errback=self.log_error,
                                 meta={'request_url': city_url})
            # 请求二手房小区列表页
            resold_url = city_url + "/community/?from=navigation"
            yield scrapy.Request(url=resold_url, callback=self.parse_resold_houses, errback=self.log_error,
                                 meta={'request_url': resold_url, 'retry_time': 0})
            # 请求城市二手房均价页面
            avg_price_url = city_url + "/market/"
            yield scrapy.Request(url=avg_price_url, callback=self.parse_avg_price, errback=self.log_error,
                                 meta={'request_url': avg_price_url, 'retry_time': 0})

    def parse_new_url(self, response):
        # 该城市解析出新楼盘的url
        request_url = response.meta["request_url"]
        print(request_url, response.url)
        selector = Selector(response)
        city_new_url = selector.xpath("//div[@class='sec_divnew div_xinfang']/a/@href").extract_first()
        if city_new_url is not None:
            yield scrapy.Request(url=city_new_url, callback=self.parse_new_houses, errback=self.log_error,
                                 meta={'request_url': city_new_url, 'retry_time': 0})

    def parse_new_houses(self, response):
        # 获得该城市下的新楼盘列表
        request_url = response.meta["request_url"]
        retry_time = response.meta["retry_time"]
        selector = Selector(response)
        house_num = selector.xpath("//div[@class='sort-condi']/span/em/text()").extract_first()
        if house_num is None:
            if retry_time < 4:
                # self.ip_blackset.add(response.meta['proxy'].replace(r'https://', ''))
                self.num_black += 1
                retry_time += 1
                if self.num_black % 10 == 0:
                    print('had show verification code %s times' % self.num_black)
                yield scrapy.Request(url=request_url, callback=self.parse_new_houses, errback=self.log_error,
                                     dont_filter=True, meta={'request_url': request_url, 'retry_time': retry_time})
            else:
                self.logger.error("%s had show verification code %s times" % (request_url, retry_time))
        elif int(house_num) == 0:
            return
        else:
            div_list = selector.xpath("//div[@class='list-results']/div[@class='key-list'][1]/div[@class='item-mod']")
            city = selector.xpath("//div[@class='sel-city']/a/span[@class='city']/text()").extract_first()
            for resold_house in div_list:
                # 解析出单个小区信息
                item = NewHouseItem()
                item['city_name'] = city.split()[0]
                house_url = resold_house.xpath("./@data-link").extract_first()
                city_id = re.findall(r'https://(.*?).fang.anjuke.com/loupan/(.*?).html', house_url)[0]
                item['house_url'] = "https://{}.fang.anjuke.com/loupan/canshu-{}.html".format(city_id[0], city_id[1])
                if item['house_url'] not in self.new_urls:
                    item['house_title'] = resold_house.xpath("./div[@class='infos']//h3/span/text()").extract_first()\
                        .replace('\n', '').replace(' ', '').replace('，', ' ').replace(',', ' ')
                    item['property_type'] = resold_house.xpath(
                        "./div[@class='infos']//i[@class='status-icon wuyetp']/text()").extract_first()
                    yield scrapy.Request(url=item['house_url'], callback=self.parse_new_info,
                                         meta={"item": deepcopy(item), "city_id": deepcopy(city_id),
                                               'request_url': item['house_url'], 'retry_time': 0})
                else:
                    print('new_house already exist!!!')
        next_url = selector.xpath("//div[@class='list-page']//a[contains(@class,'next-page')]/@href").extract_first()
        if next_url is not None:
            yield scrapy.Request(url=next_url, callback=self.parse_new_houses, errback=self.log_error,
                                 meta={'request_url': next_url, 'retry_time': 0})

    def parse_new_info(self, response):
        # 解析楼盘详情页的信息
        request_url = response.meta["request_url"]
        retry_time = response.meta["retry_time"]
        print(request_url, response.url)
        selector = Selector(response)
        item = deepcopy(response.meta["item"])
        city_id = deepcopy(response.meta["city_id"])
        house_address = selector.xpath(u"//div[text()='楼盘地址']/following-sibling::div[1]/text()").extract_first()
        if house_address is None:
            if retry_time < 4:
                # self.ip_blackset.add(response.meta['proxy'].replace(r'https://', ''))
                retry_time += 1
                self.num_black += 1
                if self.num_black % 10 == 0:
                    print('had show verification code %s times' % self.num_black)
                yield scrapy.Request(url=item['house_url'], callback=self.parse_new_info, dont_filter=True,
                                     meta={"item": deepcopy(item), "city_id": deepcopy(city_id),
                                           'request_url': request_url, 'retry_time': retry_time})
            else:
                self.logger.error("%s had show verification code %s times" % (request_url, retry_time))
        else:
            item['house_address'] = house_address.replace('\n', '')\
                .replace(' ', '').replace('，', ' ').replace(',', ' ')
            feature = selector.xpath(u"//div[text()='楼盘特点']/following-sibling::div[1]/a/text()").extract()
            if feature is not None:
                item['feature'] = ''.join(feature).replace('\n', '') \
                    .replace(' ', '').replace('，', ' ').replace(',', ' ')
            reference_price = selector.xpath(
                u"//div[text()='参考单价']/following-sibling::div[1]/span/text()").extract_first()
            if reference_price is not None:
                item['reference_price'] = reference_price.split()[0] + u"元/㎡"
            total_price = selector.xpath(
                u"//div[text()='楼盘总价']/following-sibling::div[1]/span/text()").extract_first()
            if total_price is not None:
                item['total_price'] = total_price.split()[0] + u"万元/套起"
            developers = selector.xpath(u"//div[text()='开发商']/following-sibling::div[1]/a/text()").extract_first()
            if developers is not None:
                item['developers'] = developers.replace('\n', '').replace(' ', '').replace('，', ' ').replace(',', ' ')
            sales_telephone = selector.xpath(
                u"//div[text()='售楼处电话']/following-sibling::div[1]/span/text()").extract_first()
            if sales_telephone is not None:
                item['sales_telephone'] = sales_telephone.replace(' ', '').replace('?', '').replace('\n', '') \
                    .replace('，', ' ').replace(',', ' ')
            min_payment = selector.xpath(u"//div[text()='最低首付']/following-sibling::div[1]/text()").extract_first()
            if min_payment is not None:
                item['min_payment'] = min_payment.split()[0].replace('\n', '') \
                    .replace(' ', '').replace('，', ' ').replace(',', ' ')
            sales_date = selector.xpath(u"//div[text()='最新开盘']/following-sibling::div[1]/text()").extract_first()
            if sales_date is not None:
                item['sales_date'] = sales_date.split()[0].replace('\n', '') \
                    .replace(' ', '').replace('，', ' ').replace(',', ' ')
            completion_date = selector.xpath(
                u"//div[text()='交房时间']/following-sibling::div[1]/text()").extract_first()
            if completion_date is not None:
                item['completion_date'] = completion_date.strip().replace('\n', '') \
                    .replace(' ', '').replace('，', ' ').replace(',', ' ')
            sales_address = selector.xpath(
                u"//div[text()='售楼处地址']/following-sibling::div[1]/text()").extract_first()
            if sales_address is not None:
                item['sales_address'] = sales_address.replace('\n', '') \
                    .replace(' ', '').replace('，', ' ').replace(',', ' ')
            building_type = selector.xpath(u"//div[text()='建筑类型']/following-sibling::div[1]/text()").extract_first()
            if building_type is not None:
                item['building_type'] = building_type.replace('\n', '') \
                    .replace(' ', '').replace('，', ' ').replace(',', ' ')
            planning_number = selector.xpath(
                u"//div[text()='规划户数']/following-sibling::div[1]/text()").extract_first()
            if planning_number is not None:
                item['planning_number'] = planning_number.split()[0].replace('\n', '') \
                    .replace(' ', '').replace('，', ' ').replace(',', ' ')
            property_years = selector.xpath(
                u"//div[text()='产权年限']/following-sibling::div[1]/text()[1]").extract_first()
            if property_years is not None:
                item['property_years'] = property_years.replace('\n', '') \
                    .replace(' ', '').replace('，', ' ').replace(',', ' ')
            plot_ratio = selector.xpath(
                u"//div[text()='容积率']/following-sibling::div[1]/text()[1]").extract_first()
            if plot_ratio is not None:
                item['plot_ratio'] = plot_ratio.split()[0].replace('\n', '') \
                    .replace(' ', '').replace('，', ' ').replace(',', ' ')
            greening_rate = selector.xpath(u"//div[text()='绿化率']/following-sibling::div[1]/text()[1]").extract_first()
            if greening_rate is not None:
                item['greening_rate'] = greening_rate.replace('\n', '') \
                    .replace(' ', '').replace('，', ' ').replace(',', ' ')
            progress_works = selector.xpath(
                u"//div[text()='工程进度']/following-sibling::div[1]/text()[1]").extract_first()
            if progress_works is not None:
                item['progress_works'] = progress_works.split()[0] \
                    .replace('\n', '').replace(' ', '').replace('，', ' ').replace(',', ' ')
            property_price = selector.xpath(
                u"//div[text()='物业管理费']/following-sibling::div[1]/text()").extract_first()
            if property_price is not None:
                item['property_price'] = property_price.replace('\n', '') \
                    .replace(' ', '').replace('，', ' ').replace(',', ' ')
            property_company = selector.xpath(
                u"//div[text()='物业公司']/following-sibling::div[1]/a/text()").extract_first()
            if property_company is not None:
                item['property_company'] = property_company.replace('\n', '') \
                    .replace(' ', '').replace('，', ' ').replace(',', ' ')
            parking_number = selector.xpath(
                u"//div[text()='车位数']/following-sibling::div[1]/text()").extract_first()
            if parking_number is not None:
                item['parking_number'] = parking_number.replace('\n', '') \
                    .replace(' ', '').replace('，', ' ').replace(',', ' ')
            parking_rate = selector.xpath(
                u"//div[text()='车位比']/following-sibling::div[1]/text()").extract_first()
            if parking_rate is not None:
                item['parking_rate'] = parking_rate.replace('\n', '') \
                    .replace(' ', '').replace('，', ' ').replace(',', ' ')
            map_url = u"https://m.anjuke.com/{}/loupan/{}/".format(city_id[0], city_id[1])
            yield scrapy.Request(url=map_url, callback=self.parse_map_info, headers={
                'user_agent': "Mozilla/5.0 (iPhone; CPU iPhone OS 9_1 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.0 Mobile/13B143 Safari/601.1"},
                                 meta={"item": deepcopy(item), 'request_url': map_url})

    def parse_map_info(self, response):
        # 解析地图信息
        request_url = response.meta["request_url"]
        print(request_url, response.url)
        item = deepcopy(response.meta['item'])
        map_url = response.selector.xpath("//div[@class='lpinfo']/a[contains(@class,'wui-line')]/@href").extract_first()
        if map_url is not None:
            lng_lat = re.findall(r".*?lng=(.*?)&lat=(.*?)&id=.*?", map_url)
            if len(lng_lat) > 0:
                item['map_lng'] = lng_lat[0][0]
                item['map_lat'] = lng_lat[0][1]
        print("save new_house_data successful!")
        return item

    def parse_resold_houses(self, response):
        #  解析该城市下的首页小区列表
        city_house_url = response.meta['request_url']
        retry_time = response.meta["retry_time"]
        selector = Selector(response)
        print(city_house_url, response.url)
        house_num = selector.xpath("//div[@class='sortby']/span/em[2]/text()").extract_first()
        if house_num is None:
            if retry_time < 4:
                retry_time += 1
                # self.ip_blackset.add(response.meta['proxy'].replace(r'https://', ''))
                self.num_black += 1
                if self.num_black % 10 == 0:
                    print('had show verification code %s times' % self.num_black)
                yield scrapy.Request(url=city_house_url, callback=self.parse_resold_houses, dont_filter=True,
                                     errback=self.log_error,
                                     meta={'request_url': city_house_url, 'retry_time': retry_time})
            else:
                self.logger.error("%s had show verification code %s times" % (city_house_url, retry_time))
        elif int(house_num) == 0:
            return
        elif int(house_num) < 1500:
            div_list = selector.xpath("//div[@id='list-content']/div[@class='li-itemmod']")
            for resold_house in div_list:
                # 解析出单个小区信息
                house_url = 'https://anjuke.com' + resold_house.xpath(
                    "./div[@class='li-info']/h3/a/@href").extract_first()
                # 判断小区url是否在请求过的url列表中
                if house_url not in self.resold_urls:
                    avg_price = resold_house.xpath("./div[@class='li-side']/p/strong/text()").extract_first()
                    chain_month = resold_house.xpath("./div[@class='li-side']/p[2]/text()").extract_first()
                    resold_number = resold_house.xpath("./div//p[2]/span/a/text()").extract_first()
                    yield scrapy.Request(url=house_url, callback=self.parse_resold_house_info,
                                         errback=self.log_error, meta={'request_url': house_url, 'avg_price': avg_price,
                                                                       'chain_month': chain_month,
                                                                       'resold_number': resold_number, 'retry_time': 0})
                else:
                    print('resold_house already exist!')
            next_url = selector.xpath(
                "//div[@class = 'page-content']//a[contains(@class,'aNxt')]/@href").extract_first()
            if next_url is not None:
                yield scrapy.Request(url=next_url, callback=self.parse_last_area, errback=self.log_error,
                                     meta={'request_url': next_url, 'retry_time': 0})
        else:
            area_list = selector.xpath(
                "//div[contains(@class,'items-list')]/div[1]/span[@class = 'elems-l']/a/@href").extract()
            for area_url in area_list[1:]:
                yield scrapy.Request(url=area_url, callback=self.parse_resold_area,
                                     errback=self.log_error, meta={'request_url': area_url, 'retry_time': 0})

    def parse_resold_area(self, response):
        # 该城市一级区域地址url
        city_url = response.meta['request_url']
        retry_time = response.meta["retry_time"]
        selector = Selector(response)
        print(city_url, response.url)
        house_num = selector.xpath("//div[@class='sortby']/span/em[2]/text()").extract_first()
        if house_num is None:
            if retry_time < 4:
                retry_time += 1
                # self.ip_blackset.add(response.meta['proxy'].replace(r'https://', ''))
                self.num_black += 1
                if self.num_black % 10 == 0:
                    print('had show verification code %s times' % self.num_black)
                yield scrapy.Request(url=city_url, callback=self.parse_resold_area, dont_filter=True,
                                     errback=self.log_error, meta={'request_url': city_url, 'retry_time': retry_time})
            else:
                self.logger.error("%s had show verification code %s times" % (city_url, retry_time))
        elif int(house_num) == 0:
            return
        elif int(house_num) < 1500:
            div_list = selector.xpath("//div[@id='list-content']/div[@class='li-itemmod']")
            for resold_house in div_list:
                # 解析出单个小区信息
                house_url = 'https://anjuke.com' + resold_house.xpath(
                    "./div[@class='li-info']/h3/a/@href").extract_first()
                # 判断小区url是否在请求过的url列表中
                if house_url not in self.resold_urls:
                    avg_price = resold_house.xpath("./div[@class='li-side']/p/strong/text()").extract_first()
                    chain_month = resold_house.xpath("./div[@class='li-side']/p[2]/text()").extract_first()
                    resold_number = resold_house.xpath("./div//p[2]/span/a/text()").extract_first()
                    yield scrapy.Request(url=house_url, callback=self.parse_resold_house_info,
                                         errback=self.log_error, meta={'request_url': house_url, 'avg_price': avg_price,
                                                                       'chain_month': chain_month,
                                                                       'resold_number': resold_number, 'retry_time': 0})
                else:
                    print('resold_house already exist!')
            next_url = selector.xpath(
                "//div[@class = 'page-content']//a[contains(@class,'aNxt')]/@href").extract_first()
            if next_url is not None:
                yield scrapy.Request(url=next_url, callback=self.parse_last_area, errback=self.log_error,
                                     meta={'request_url': next_url, 'retry_time': 0})
        else:
            # 提取三个分类列表，构造最终小区的列表页url
            area_list = selector.xpath(
                "//div[contains(@class,'items-list')]/div[1]/span[@class = 'elems-l']/div/a/@href").extract()
            second_list = selector.xpath(
                "//div[contains(@class,'items-list')]/div[2]/span[@class = 'elems-l']/a/@href").extract()
            price_list = selector.xpath(
                "//div[contains(@class,'items-list')]/div[3]/span[@class = 'elems-l']/a/@href").extract()
            for area in area_list[1:]:
                for second_url in second_list[1:]:
                    second = second_url.replace(city_url, '').replace(r'/', '')
                    for price_url in price_list[1:]:
                        price = price_url.replace(city_url, '')
                        area_url = '{}{}-{}'.format(area, second, price)
                        yield scrapy.Request(url=area_url, callback=self.parse_last_area,
                                             errback=self.log_error, meta={'request_url': area_url, 'retry_time': 0})

    def parse_last_area(self, response):
        # 解析最终小区的列表页url， 提取单个小区的url
        area_url = response.meta['request_url']
        retry_time = response.meta["retry_time"]
        print(area_url, response.url)
        selector = Selector(response)
        house_num = selector.xpath("//div[@class='sortby']/span/em[2]/text()").extract_first()
        if house_num is None:
            if retry_time < 4:
                retry_time += 1
                # self.ip_blackset.add(response.meta['proxy'].replace(r'https://', ''))
                self.num_black += 1
                if self.num_black % 10 == 0:
                    print('had show verification code %s times' % self.num_black)
                yield scrapy.Request(url=area_url, callback=self.parse_last_area, dont_filter=True,
                                     errback=self.log_error, meta={'request_url': area_url, 'retry_time': retry_time})
            else:
                self.logger.error("%s had show verification code %s times" % (area_url, retry_time))
        elif int(house_num) == 0:
            return
        else:
            div_list = selector.xpath("//div[@id='list-content']/div[@class='li-itemmod']")
            for resold_house in div_list:
                # 解析出单个小区信息
                house_url = 'https://anjuke.com' + resold_house.xpath(
                    "./div[@class='li-info']/h3/a/@href").extract_first()
                # 判断小区url是否在请求过的url列表中
                if house_url not in self.resold_urls:
                    avg_price = resold_house.xpath("./div[@class='li-side']/p/strong/text()").extract_first()
                    chain_month = resold_house.xpath("./div[@class='li-side']/p[2]/text()").extract_first()
                    resold_number = resold_house.xpath("./div//p[2]/span/a/text()").extract_first()
                    yield scrapy.Request(url=house_url, callback=self.parse_resold_house_info,
                                         errback=self.log_error, meta={'request_url': house_url, 'avg_price': avg_price,
                                                                       'chain_month': chain_month,
                                                                       'resold_number': resold_number, 'retry_time': 0})
                else:
                    print('resold_house already exist!')
            next_url = selector.xpath(
                "//div[@class = 'page-content']//a[contains(@class,'aNxt')]/@href").extract_first()
            if next_url is not None:
                yield scrapy.Request(url=next_url, callback=self.parse_last_area, errback=self.log_error,
                                     meta={'request_url': next_url, 'retry_time': 0})

    def parse_resold_house_info(self, response):
        # 解析小区详情页的信息
        house_url = response.meta['request_url']
        avg_price = response.meta['avg_price']
        chain_month = response.meta['chain_month']
        resold_number = response.meta['resold_number']
        retry_time = response.meta["retry_time"]
        print(house_url, response.url)
        selector = Selector(response)
        house_title = selector.xpath("//div[@class='comm-title']/h1/text()").extract_first()
        # 判断是否出现验证码，是则重发请求
        if house_title is None:
            if retry_time < 4:
                retry_time += 1
                # self.ip_blackset.add(response.meta['proxy'].replace(r'https://', ''))
                self.num_black += 1
                if self.num_black % 10 == 0:
                    print('had show verification code %s times' % self.num_black)
                yield scrapy.Request(url=house_url, callback=self.parse_resold_house_info, dont_filter=True,
                                     errback=self.log_error, meta={'request_url': house_url, 'avg_price': avg_price,
                                                                   'chain_month': chain_month,
                                                                   'resold_number': resold_number,
                                                                   'retry_time': retry_time})
            else:
                self.logger.error("%s had show verification code %s times" % (house_url, retry_time))
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
            item['house_title'] = house_title.replace('\n', '').replace(' ', '').replace(',', ' ').replace('，', ' ')
            if avg_price is not None:
                item['avg_price'] = avg_price.replace('\n', '').replace(' ', '').replace(',', ' ').replace('，', ' ')
            item['chain_month'] = chain_month
            building_years = selector.xpath(
                u"//dt[text()='建造年代：']/following-sibling::dd[1]/text()").extract_first()
            if building_years is not None:
                item['building_years'] = building_years.replace('\n', '') \
                    .replace(' ', '').replace(',', ' ').replace('，', ' ')
            item['resold_number'] = resold_number.replace('(', '').replace(')', '').replace('\n', '') \
                .replace(' ', '').replace(',', ' ').replace('，', ' ')
            house_address = selector.xpath("//div[@class='comm-title']/h1/span/text()").extract_first()
            if house_address is not None:
                item['house_address'] = house_address.replace('\n', '') \
                    .replace(' ', '').replace(',', ' ').replace('，', ' ')
            property_type = selector.xpath("//dl[@class='basic-parms-mod']/dd[1]/text()").extract_first()
            if property_type is not None:
                item['property_type'] = property_type.replace('\n', '') \
                    .replace(' ', '').replace(',', ' ').replace('，', ' ')
            property_price = selector.xpath("//dl[@class='basic-parms-mod']/dd[2]/text()").extract_first()
            if property_price is not None:
                item['property_price'] = property_price.replace('\n', '') \
                    .replace(' ', '').replace(',', ' ').replace('，', ' ')
            total_area = selector.xpath("//dl[@class='basic-parms-mod']/dd[3]/text()").extract_first()
            if total_area is not None:
                item['total_area'] = total_area.replace('\n', '') \
                    .replace(' ', '').replace(',', ' ').replace('，', ' ')
            total_houses = selector.xpath("//dl[@class='basic-parms-mod']/dd[4]/text()").extract_first()
            if total_houses is not None:
                item['total_houses'] = total_houses.replace('\n', '') \
                    .replace(' ', '').replace(',', ' ').replace('，', ' ')
            parking_number = selector.xpath("//dl[@class='basic-parms-mod']/dd[6]/text()").extract_first()
            if parking_number is not None:
                item['parking_number'] = parking_number.replace('\n', '') \
                    .replace(' ', '').replace(',', ' ').replace('，', ' ')
            plot_ratio = selector.xpath("//dl[@class='basic-parms-mod']/dd[7]/text()").extract_first()
            if plot_ratio is not None:
                item['plot_ratio'] = plot_ratio.replace('\n', '') \
                    .replace(' ', '').replace(',', ' ').replace('，', ' ')
            greening_rate = selector.xpath("//dl[@class='basic-parms-mod']/dd[8]/text()").extract_first()
            if greening_rate is not None:
                item['greening_rate'] = greening_rate.replace('\n', '') \
                    .replace(' ', '').replace(',', ' ').replace('，', ' ')
            developers = selector.xpath("//dl[@class='basic-parms-mod']/dd[9]/text()").extract_first()
            if developers is not None:
                item['developers'] = developers.replace('\n', '') \
                    .replace(' ', '').replace(',', ' ').replace('，', ' ')
            property_company = selector.xpath("//dl[@class='basic-parms-mod']/dd[10]/text()").extract_first()
            if property_company is not None:
                item['property_company'] = property_company.replace('\n', '') \
                    .replace(' ', '').replace(',', ' ').replace('，', ' ')
            print(' save resold_house_data successful!')
            yield item

    def parse_avg_price(self, response):
        request_url = response.meta["request_url"]
        retry_time = response.meta["retry_time"]
        print(request_url, response.url)
        selector = Selector(response)
        city_name = selector.xpath("//h1[@class='hTitle']/text()").extract_first()
        if city_name is None:
            if retry_time < 4:
                # self.ip_blackset.add(response.meta['proxy'].replace(r'https://', ''))
                retry_time += 1
                self.num_black += 1
                if self.num_black % 10 == 0:
                    print('had show verification code %s times' % self.num_black)
                yield scrapy.Request(url=request_url, callback=self.parse_new_info, dont_filter=True,
                                     meta={'request_url': request_url, 'retry_time': retry_time})
            else:
                self.logger.error("%s had show verification code %s times" % (request_url, retry_time))
        else:
            item = CityAvgItem()
            item['city_name'] = city_name[0:2]
            item['avg_price'] = selector.xpath("//h2[@class='highLight']/em/text()").extract_first()
            item['last_price'] = selector.xpath(
                "//h2[@class='highLight']/following-sibling::h2[1]/em/text()").extract_first()
            print("save avg_price_data successful!")
            yield item

    def log_error(self, failure):
        if failure.check(HttpError):
            response = failure.value.response
            self.logger.error('HttpError on %s', response.url)
        elif failure.check(DNSLookupError):
            request = failure.request
            self.logger.error('DNSLookupError on %s', request.url)
        elif failure.check(TimeoutError, TCPTimedOutError):
            request = failure.request
            if 'anjuke.com/community/view/' in request.url:
                retry_time = request.meta['retry_time']
                if retry_time < 4:
                    house_url = request.meta['request_url']
                    avg_price = request.meta['avg_price']
                    chain_month = request.meta['chain_month']
                    resold_number = request.meta['resold_number']
                    yield scrapy.Request(url=house_url, callback=self.parse_resold_house_info, dont_filter=True,
                                         errback=self.log_error, meta={'request_url': house_url, 'avg_price': avg_price,
                                                                       'chain_month': chain_month,
                                                                       'resold_number': resold_number, 'retry_time': 0})
                else:
                    self.logger.error("%s had show verification code %s times" % (request.url, retry_time))
            elif 'anjuke.com/community/' in request.url:
                retry_time = request.meta['retry_time']
                if retry_time < 4:
                    yield scrapy.Request(url=request.url, callback=self.parse, errback=self.log_error,
                                         dont_filter=True,
                                         meta={'request_url': request.url, 'retry_time': retry_time})
                else:
                    self.logger.error("%s had show verification code %s times" % (request.url, retry_time))
            elif 'fang.anjuke.com/loupan/canshu' in request.url:
                retry_time = request.meta['retry_time']
                if retry_time < 4:
                    item = deepcopy(request.meta["item"])
                    city_id = deepcopy(request.meta["city_id"])
                    yield scrapy.Request(url=item['house_url'], callback=self.parse_new_info, dont_filter=True,
                                         meta={"item": deepcopy(item), "city_id": deepcopy(city_id),
                                               'request_url': request.url, 'retry_time': retry_time})
                else:
                    self.logger.error("%s had show verification code %s times" % (request.url, retry_time))
            elif 'fang.anjuke.com/loupan/' in request.url:
                retry_time = request.meta['retry_time']
                if retry_time < 4:
                    yield scrapy.Request(url=request.url, callback=self.parse, dont_filter=True,
                                         errback=self.log_error,
                                         meta={'request_url': request.url, 'retry_time': retry_time})
                else:
                    self.logger.error("%s had show verification code %s times" % (request.url, retry_time))
            else:
                self.logger.error('TimeoutError on %s', request.url)
                # 使用代理时将超时ip加入黑名单
                # self.ip_blackset.add(request.meta['proxy'].replace(r'https://', ''))
        else:
            pass
