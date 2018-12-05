# nlp-surveillance
A tool to extract the most important entites from a epidemiological text and to analyze them.

## Scraper
A scraper for [WHO DONs](https://www.who.int/csr/don/en/) is there. One for ProMed is in progress

## Annotator
The script annotator uses EpiTator to annotate a string. It extracts the most important entities such as geonames, 
case numbers, and diseases. A list of function with parameter can be given to the annotate() function.

## Wikipedia Country List
The wiki_country_parser is responsible to scrape, format, and save the Liste der Staaten der Welt. It
functions as a lookup to translate German country names to English. Only then there are comparable with 
the output of EpiTator.
