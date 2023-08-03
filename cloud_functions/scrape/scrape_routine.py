from cloud_functions.scrape.vegas_insider.scraper import VegasInsider


def scrape_routine():
    vegas_insider = VegasInsider()
    vegas_insider.scrape()
    return "Success"
