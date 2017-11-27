# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import os

from scrapy import signals
from scrapy.exporters import CsvItemExporter
from ajk_zebra.items import ResoldHouseItem, NewHouseItem, HouseUrlItem
import time


class ResoldHousePipeline(object):
    # 二手房信息保存为csv
    def __init__(self):
        self.files = {}
        self.file_path = './data/resold.%d.csv' % int(time.time())

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def spider_opened(self, spider):
        file = open(self.file_path, 'w+b')
        self.files[spider] = file
        kwargs = {
            'fields_to_export': ['city_name', 'house_title', 'house_address', 'avg_price',
                                 'chain_month', 'resold_number', 'building_years', 'developers',
                                 'property_company', 'parking_number', 'plot_ratio', 'greening_rate', 'property_price',
                                 'property_type', 'property_price', 'total_area', 'total_houses', 'house_url',
                                 'map_lng', 'map_lat', 'detail_community']}

        self.exporter = CsvItemExporter(file, include_headers_line=True, **kwargs)
        self.exporter.start_exporting()

    def spider_closed(self, spider):
        self.exporter.finish_exporting()
        file = self.files.pop(spider)
        file.close()

    def process_item(self, item, spider):
        if isinstance(item, ResoldHouseItem):
            self.exporter.export_item(item)
        return item


class NewHousePipeline(object):
    # 新房信息保存为csv
    def __init__(self):
        self.files = {}
        self.file_path = './data/new.%d.csv' % int(time.time())

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def spider_opened(self, spider):
        file = open(self.file_path, 'w+b')
        self.files[spider] = file
        kwargs = {
            'fields_to_export': ['city_name', 'house_title', 'house_address', 'property_type', 'feature', 'total_price',
                                 'reference_price', 'sales_telephone', 'developers', 'min_payment', 'sales_date',
                                 'completion_date', 'sales_address', 'building_type', 'planning_number',
                                 'property_years', 'plot_ratio', 'greening_rate', 'progress_works', 'property_price',
                                 'property_company', 'parking_number', 'parking_rate', 'house_url', 'map_lng',
                                 'map_lat']}

        self.exporter = CsvItemExporter(file, include_headers_line=True, **kwargs)
        self.exporter.start_exporting()

    def spider_closed(self, spider):
        self.exporter.finish_exporting()
        file = self.files.pop(spider)
        file.close()

    def process_item(self, item, spider):
        if isinstance(item, NewHouseItem):
            self.exporter.export_item(item)
        return item


# 保存一线城市已抓取小区详情页
class FirstHouseUrlPipeline(object):
    def process_item(self, item, spider):
        if isinstance(item, ResoldHouseItem):
            # 从内存以追加的方式打开文件，并写入对应的数据
            with open('./data/house_url.txt', 'a') as f:
                f.write(item['house_url'] + '\n')
        return item


# 保存二线城市已抓取小区详情页
class ResoldHouseUrlPipeline(object):
    def process_item(self, item, spider):
        if isinstance(item, ResoldHouseItem):
            # 从内存以追加的方式打开文件，并写入对应的数据
            with open('./data/resold_url.txt', 'a') as f:
                f.write(item['house_url'] + '\n')
        return item


# 保存一线城市下小区总数小于1500的城市url
class FirstCityPipeline(object):
    def process_item(self, item, spider):
        if isinstance(item, HouseUrlItem):
            # 从内存以追加的方式打开文件，并写入对应的数据
            with open('./data/city_first_url.txt', 'a') as f:
                f.write(item['city_house_url'] + '\n')
        return item


# 保存二线城市下小区总数小于1500的城市url
class SecondCityPipeline(object):
    def process_item(self, item, spider):
        if isinstance(item, HouseUrlItem):
            # 从内存以追加的方式打开文件，并写入对应的数据
            with open('./data/city_second_url.txt', 'a') as f:
                f.write(item['city_house_url'] + '\n')
        return item
