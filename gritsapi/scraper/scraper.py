__version__ = '0.0.3'
import urllib2, httplib
import chardet
import httplib2
import scrape_promed
import sys, exceptions, socket
import datetime

class OpenURLHandler(urllib2.HTTPRedirectHandler):
    def http_request(self, request):
        request.add_header("User-agent", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.154 Safari/537.36")
        return request
    def http_error_301(self, req, fp, code, msg, headers):  
        result = urllib2.HTTPRedirectHandler.http_error_301(
            self, req, fp, code, msg, headers)
        if result is None: return result
        if not hasattr(result, 'redirects'):
            result.redirects = []
        result.redirects.append({
            'url' : req.get_full_url(),
            'code' : code
        })
        return result
    def http_error_302(self, req, fp, code, msg, headers):
        result = urllib2.HTTPRedirectHandler.http_error_302(
            self, req, fp, code, msg, headers)
        if result is None: return result
        if not hasattr(result, 'redirects'):
            result.redirects = []
        result.redirects.append({
            'url' : req.get_full_url(),
            'code' : code
        })
        return result
primary_opener = urllib2.build_opener(OpenURLHandler())

class BackupOpenURLHandler(OpenURLHandler):
    """
    Using add_unredirected_header avoids 403s on some sites.
    It seems to be an idiosyncracy of the way some sites are configured.
    More info here:
    http://stackoverflow.com/questions/23602996/why-does-setting-the-user-agent-in-an-unredirected-header-avoid-403s
    """
    def http_request(self, request):
        request.add_unredirected_header("User-agent", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.154 Safari/537.36")
        return request
backup_opener = urllib2.build_opener(BackupOpenURLHandler())

def open_url(opener, url):
    url = httplib2.iri2uri(url)
    result = {
        'sourceUrl' : url
    }
    try:
        resp = opener.open(url, timeout=60)
        if hasattr(resp, 'redirects'):
            # The recursive call to scrape_main here is inefficient because two
            # requests could be sent to the page we are redirected to. Recursive
            # calls before redirection would require a major refactoring.
            result = scrape_main(resp.url)
            result['redirects'] = resp.redirects
            result['sourceUrl'] = url
            return result
        result['code'] = resp.code
        result['msg'] = resp.msg
        result['url'] = resp.url
        if resp.getcode() >= 300:
            result['unscrapable'] = True
            return result
        else:
            html = resp.read()
            if not html:
                result.update({
                    'unscrapable' : True,
                    'exception' : "No html returned"
                })
                return result
            detected_encoding = chardet.detect(html)['encoding']
            encoding = detected_encoding if detected_encoding else 'utf-8'
            result['htmlContent'] = unicode(
                html.decode(
                    encoding=encoding,
                    errors='replace'
                )
            )
            result['encoding'] = encoding
            return result

    except (urllib2.HTTPError, urllib2.URLError):
        errDescription = '\n'.join([str(i) for i in sys.exc_info()])
        result.update({
            'unscrapable' : True,
            'exception' : "URLLibError: Could not scrape url: " + errDescription
        })
        return result
    except (httplib.IncompleteRead, httplib.BadStatusLine):
        errDescription = '\n'.join([str(i) for i in sys.exc_info()])
        result.update({
            'unscrapable' : True,
            'exception' : "HTTPLibError: Could not scrape url: " + errDescription
        })
        return result
    except (socket.timeout, socket.error):
        errDescription = '\n'.join([str(i) for i in sys.exc_info()])
        result.update({
            'unscrapable' : True,
            'exception' : "SocketError:Could not scrape url: " + errDescription
        })
        return result
    except UnicodeEncodeError:
        # I think using iri2uri will prevent this, so we can probably get rid of this case.
        # http://stackoverflow.com/questions/4389572/how-to-fetch-a-non-ascii-url-with-python-urlopen
        errDescription = '\n'.join([str(i) for i in sys.exc_info()])
        result.update({
            'unscrapable' : True,
            'exception' : errDescription
        })
        return result
    except exceptions.KeyboardInterrupt as e:
        raise e
    except:
        errDescription = '\n'.join([str(i) for i in sys.exc_info()])
        print "Unknown error:Could not scrape url: ", url
        print errDescription
        raise Exception("Unknown error")

def scrape_main(url):
    parsed_url = None
    try:
        # Can this handle unicode urls?
        parsed_url = urllib2.urlparse.urlparse(url)
    except:
        raise Exception("urlparse exception")
    
    if parsed_url.path.endswith('.pdf'):
        print "Could not scrape url because it is a PDF: " + url
        return {
            'sourceUrl' : url,
            'unscrapable' : True,
            'exception' : "We can't scrape PDFs yet..."
        }
    
    if not parsed_url or not parsed_url.hostname:
        print "Could not parse url: " + url
        return {
            'sourceUrl' : url,
            'unscrapable' : True
        }
    if 'promed' in parsed_url.hostname:
        return_value = scrape_promed.scrape_promed_url(url)
        return_value['sourceUrl'] = url
        return return_value
    if 'empres-i.fao.org' in parsed_url.hostname:
        return {
            'sourceUrl' : url,
            'unscrapable' : True,
            'exception' : "We don't scrape empres-i reports."
            # The reason is that they contain mostly structured data that isn't
            # useful for training text classifiers.
        }
    if 'twitter.com' in parsed_url.hostname:
        return {
            'sourceUrl' : url,
            'unscrapable' : True,
            'exception' : "We don't scrape twitter reports."
            # Two reasons:
            # 1. They require using a special API to access the content.
            # 2. The content requires different NLP techniques to be useful.
        }
    if 'news.google.com' in parsed_url.hostname:
        parsed_qs = urllib2.urlparse.parse_qs(parsed_url.query)
        source_url = parsed_qs.get('url')[0]
        testparse = urllib2.urlparse.urlparse(source_url)
        if not testparse or not testparse.hostname:
            print url, "has a bad url parameter"
        if source_url:
            result = scrape_main(source_url)
            result['googleNews'] = True
            return result
        else:
            print "Could not extract url parameter from: " + url
            return {
                'sourceUrl' : source_url,
                'unscrapable' : True,
                'googleNews' : True
            }
    else:
        result = open_url(primary_opener, url)
        if result.get('unscrapable'):
            result = open_url(backup_opener, url)
        return result

def scrape(url):
    scrape_time = datetime.datetime.now()
    result = scrape_main(url)
    result['scrapeDate'] = scrape_time
    result['scraperVersion'] = __version__
    return result
