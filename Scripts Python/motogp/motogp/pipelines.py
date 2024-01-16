# pipelines.py is where the item yielded by the spider gets passed, it’s mostly 
# used to clean the text and connect to file outputs or databases (CSV, JSON SQL, etc).

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


class MotogpPipeline:
    def process_item(self, item, spider):
        return item
