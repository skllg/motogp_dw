
import scrapy

class WikipediaSpider(scrapy.Spider):
    name = 'wikipedia_spider'
    # https://en.wikipedia.org/wiki/2003_Grand_Prix_motorcycle_racing_season

    def start_requests(self):
        for season in range(2002,2024):
            if season > 2011:               
                start_urls = f'https://en.wikipedia.org/wiki/{season}_MotoGP_World_Championship#Teams_and_riders'
            else:
                start_urls = f'https://en.wikipedia.org/wiki/{season}_Grand_Prix_motorcycle_racing_season#Participants'
            # start_urls = 'https://en.wikipedia.org/wiki/2003_Grand_Prix_motorcycle_racing_season#Participants'

            yield scrapy.Request(url=str(start_urls),cb_kwargs={'season':season}, callback=self.parse)
        
            
            # yield scrapy.Request(url=str(start_urls),cb_kwargs={'season':season}, callback=self.parse)
    def parse(self, response,season):
        

        table_rows = response.xpath('//tr[td[contains(@style, "background:#ccffcc;") or contains(@style, "background:#cfc;")]]')

        for row in table_rows:
            background_color = row.xpath('.//td[contains(@style, "background:#ccffcc;") or contains(@style, "background:#cfc;")]/@style').extract_first()
            title = row.xpath('.//td[contains(@style, "background:#ccffcc;") or contains(@style, "background:#cfc;")]//a/@title').extract()
            last_td = row.xpath('.//td[contains(@style, "background:#ccffcc;") or contains(@style, "background:#cfc;")]/following-sibling::td[last()]').extract_first()
            
            last_td = ''.join(scrapy.Selector(text=last_td).xpath('//text()').extract()).strip()
            
            title_proc = str(title).split(",")
            last_td = last_td.strip() if last_td else ''

            # Append the results to a CSV file 'rider_name': title_proc[1].replace(']', ''),
            
            yield {
                'Title': f'Wildcards',
                'season': season,
                'rider_name': title_proc[1].replace(']', ''),
                'rounds_wildcard': str(last_td),
            }

# To run the spider and save the results to a CSV file, use the following command:
# scrapy runspider your_spider_file.py -o output.csv
