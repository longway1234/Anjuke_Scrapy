# -*- coding: utf-8 -*-
# Define here the models for your scraped items
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html
import scrapy


# 新楼盘
class NewHouseItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    # define the fields for your item here like:
    city_name = scrapy.Field()  # 城市名称
    house_url = scrapy.Field()  # 楼盘链接
    house_title = scrapy.Field()  # 楼盘名称
    property_type = scrapy.Field()  # 物业类型
    feature = scrapy.Field()  # 特色
    reference_price = scrapy.Field()  # 参考单价单位(元/㎡ )
    total_price = scrapy.Field()  # 楼盘总价(万元/套起)
    developers = scrapy.Field()  # 开发商
    house_address = scrapy.Field()  # 楼盘地址
    sales_telephone = scrapy.Field()  # 销售电话
    min_payment = scrapy.Field()  # 最低首付
    sales_date = scrapy.Field()  # 开盘时间
    completion_date = scrapy.Field()  # 交房时间
    sales_address = scrapy.Field()  # 售楼处地址
    building_type = scrapy.Field()  # 建筑类型
    planning_number = scrapy.Field()  # 规划户数
    property_years = scrapy.Field()  # 产权年限
    plot_ratio = scrapy.Field()  # 容积率
    greening_rate = scrapy.Field()  # 绿化率
    progress_works = scrapy.Field()  # 工程进度
    property_price = scrapy.Field()  # 物业费
    property_company = scrapy.Field()  # 物业公司
    parking_number = scrapy.Field()  # 停车位
    parking_rate = scrapy.Field()  # 车位比
    map_lng = scrapy.Field()  # 地图经度
    map_lat = scrapy.Field()  # 地图维度


# 二手房小区
class ResoldHouseItem(scrapy.Item):
    # define the fields for your item here like:
    city_name = scrapy.Field()  # 城市
    house_title = scrapy.Field()  # 小区名称 (包含','需要替换)
    house_address = scrapy.Field()  # 小区位置(包含','需要替换)
    avg_price = scrapy.Field()  # 小区均价
    chain_month = scrapy.Field()  # 环比上月
    resold_number = scrapy.Field()  # 二手房房源数
    building_years = scrapy.Field()  # 建造年代
    developers = scrapy.Field()  # 开发商  (包含','需要替换)
    property_company = scrapy.Field()  # 物业公司(包含','需要替换)
    parking_number = scrapy.Field()  # 停车位 (包含','需要替换)
    plot_ratio = scrapy.Field()  # 容积率
    greening_rate = scrapy.Field()  # 绿化率
    property_type = scrapy.Field()  # 物业类型
    property_price = scrapy.Field()  # 物业费
    total_area = scrapy.Field()  # 总建面积
    total_houses = scrapy.Field()  # 总户数
    detail_community = scrapy.Field()  # 小区介绍(包含','需要替换)
    house_url = scrapy.Field()  # 小区url
    map_lng = scrapy.Field()  # 地图经度
    map_lat = scrapy.Field()  # 地图维度


# 小区列表页url
class HouseUrlItem(scrapy.Item):
    # define the fields for your item here like:
    city_house_url = scrapy.Field()  # 城市


# aoi结果 'fields_to_export': ['name', 'area', 'adcode', 'location(lng,lat)', 'type', 'id']
class Aoi(scrapy.Item):
    id = scrapy.Field()
    name = scrapy.Field()
    adcode = scrapy.Field()
    location = scrapy.Field()
    area = scrapy.Field()
    type = scrapy.Field()


# poi结果 'fields_to_export': ['name', 'location(lng,lat)', 'address', 'businessarea', 'type', 'id']
class Poi(scrapy.Item):
    id = scrapy.Field()
    name = scrapy.Field()
    type = scrapy.Field()
    location = scrapy.Field()
    address = scrapy.Field()
    businessarea = scrapy.Field()


# aoi明细 'fields_to_export': ['id', 'name', 'city_code', 'tag_display_std', 'industry', 'type_code', 'new_type', \
# 'longitude', 'latitude', 'bound', 'shape_region', 'average_cost', 'average_cost_name']
class AoiDetail(scrapy.Item):
    id = scrapy.Field()
    name = scrapy.Field()
    city_code = scrapy.Field()
    tag_display_std = scrapy.Field()
    industry = scrapy.Field()
    type_code = scrapy.Field()
    new_type = scrapy.Field()
    longitude = scrapy.Field()
    latitude = scrapy.Field()
    bound = scrapy.Field()
    shape_region = scrapy.Field()
    average_cost = scrapy.Field()
    average_cost_name = scrapy.Field()


# 品牌
class Brand(scrapy.Item):
    name = scrapy.Field()
    location = scrapy.Field()
    product = scrapy.Field()
    cat = scrapy.Field()
