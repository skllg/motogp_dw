import scrapy
import csv


class motogpspiderSpider(scrapy.Spider):
    name = 'rider_bio_spider'

    def start_requests(self):
        # Read URLs from CSV file
        with open('list.csv', 'r') as file:
            reader = csv.reader(file)
            for row in reader:
                yield scrapy.Request(url=row[0], callback=self.parse)

    def parse(self, response):
        # Extract data from the specified class names
        rider_data = {'url': response.url, 'data': []}
        rows = response.css('.rider-bio__table .rider-bio__row')

        for row in rows:
            key = row.css('.rider-bio__key::text').get().strip()
            value = row.css('.rider-bio__value::text').get().strip()
            rider_data['data'].append({'key': key, 'value': value})

        yield rider_data