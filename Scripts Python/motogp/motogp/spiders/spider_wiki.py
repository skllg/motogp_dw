
import scrapy

class WikipediaSpider(scrapy.Spider):
    name = 'wikipedia_spider'
    season = 2023
    start_urls = [f'https://en.wikipedia.org/wiki/{season}_MotoGP_World_Championship#Teams_and_riders']

    def parse(self, response):
        # Locate the table using XPath
        table_rows = response.xpath('//tr[td[contains(@style, "background:#ccffcc;")]]')

        for row in table_rows:
            background_color = row.xpath('.//td[contains(@style, "background:#ccffcc;")]/@style').extract_first()
            title = row.xpath('.//td[contains(@style, "background:#ccffcc;")]//a/@title').extract()
            last_td = row.xpath('.//td[contains(@style, "background:#ccffcc;")]/following-sibling::td[last()]/text()').extract_first()

            # Remove leading/trailing whitespaces
            title_proc = str(title).split(",")

            last_td = last_td.strip() if last_td else ''

            # Append the results to a CSV file
            yield {
                'Title': f'Wildcards_{2023}',
                'rider_name': title_proc[1].replace(']', ''),
                'rounds_wildcard': str(last_td),
            }

# To run the spider and save the results to a CSV file, use the following command:
# scrapy runspider your_spider_file.py -o output.csv
