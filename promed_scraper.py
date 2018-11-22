# pylint: disable=print-statement

import pprint
import sys; sys.path.append('./gritsapi/')
from gritsapi.scraper.scrape_promed import scrape_promed_url
# from __future__ import print_function
# define settings for the pretty printer and the printer itself
width = 80
pp = pprint.PrettyPrinter(indent=4, width=width)

# function to pretty print the parsed page with a delimiter if needed
def print_result(obj, print_delimiter):
  pp.pprint(parsed)
  if print_delimiter:
	print "=" * width + "\n"

# define proxies to use (RKI internal)
proxies = {
	'http': 'http://fw-bln.rki.ivbb.bund.de:8020',
	'https': 'http://fw-bln.rki.ivbb.bund.de:8020',
}

import requests
get_request = requests.get('https://www.promedmail.org/ajax/runSearch.php?kwby1=summary&search=&date1=02/01/2018&date2=10/24/2018&feed_id=1', proxies = proxies)
content = get_request.content
ids = re.findall(r"id\d+", content)

parsed = []
for id in ids:
  # parse promed page into dictionary using grits-api
  parsed.append(scrape_promed_url("https://www.promedmail.org/post/" + str(id), proxies))
  # print the parsed page
