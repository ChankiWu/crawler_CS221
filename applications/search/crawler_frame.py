import logging
from datamodel.search.Haodoz1Qiancw1Xinhew1_datamodel import Haodoz1Qiancw1Xinhew1Link, OneHaodoz1Qiancw1Xinhew1UnProcessedLink, add_server_copy, get_downloaded_content
from datamodel.search.Robot import Robot
from spacetime.client.IApplication import IApplication
from spacetime.client.declarations import Producer, GetterSetter, Getter, ServerTriggers
from lxml import html,etree
import re, os
from time import time
from uuid import uuid4
import validators


from urlparse import urlparse, parse_qs
from uuid import uuid4

logger = logging.getLogger(__name__)
LOG_HEADER = "[CRAWLER]"

TRAP_POOL = {"calendar.ics.uci.edu", "drzaius.ics.uci.edu/cgi-bin/cvsweb.cgi/", "flamingo.ics.uci.edu/releases/"
    , "fano.ics.uci.edu/", "ironwood.ics.uci.edu", "djp3-pc2.ics.uci.edu/LUCICodeRepository/",
             "archive.ics.uci.edu/ml", "www.ics.uci.edu/~xhx/project/MotifMap/"}

@Producer(Haodoz1Qiancw1Xinhew1Link)
@GetterSetter(OneHaodoz1Qiancw1Xinhew1UnProcessedLink)
@ServerTriggers(add_server_copy, get_downloaded_content)
class CrawlerFrame(IApplication):

    def __init__(self, frame):
        self.starttime = time()
        self.app_id = "Haodoz1Qiancw1Xinhew1"
        self.frame = frame


    def initialize(self):
        self.count = 0
        l = Haodoz1Qiancw1Xinhew1Link("http://www.ics.uci.edu/")
        print l.full_url
        self.frame.add(l)

    def update(self):
        unprocessed_links = self.frame.get(OneHaodoz1Qiancw1Xinhew1UnProcessedLink)
        if unprocessed_links:
            link = unprocessed_links[0]
            print "Got a link to download:", link.full_url
            downloaded = link.download()
            links = extract_next_links(downloaded)
            for l in links:
                # if is_valid(l):
                self.frame.add(Haodoz1Qiancw1Xinhew1Link(l))

    def shutdown(self):
        print (
            "Time time spent this session: ",
            time() - self.starttime, " seconds.")

# this function would be called multiple times
def extract_next_links(rawDataObj):
    outputLinks = []
    url_size = 200
    #get external links
    if(rawDataObj.error_message == None):
        page = etree.HTML(rawDataObj.content)
        print('\nEnter in extract_next_links\n')
        for link in page.xpath("//@href"):
            if is_valid(link):
              print(link)
              outputLinks.append(link)
              url_size -= 1
              if url_size < 0:
                 break

    else:
        print('Got Error links and the error is: ' , rawDataObj.error_message)
        
    '''
    rawDataObj is an object of type UrlResponse declared at L20-30
    datamodel/search/server_datamodel.py
    the return of this function should be a list of urls in their absolute form
    Validation of link via is_valid function is done later (see line 42).
    It is not required to remove duplicates that have already been downloaded. 
    The frontier takes care of that.
    
    Suggested library: lxml
    '''
    return outputLinks

def is_valid(url):
    '''
    Function returns True or False based on whether the url has to be
    downloaded or not.
    Robot rules and duplication rules are checked separately.
    This is a great place to filter out crawler traps.
    '''

    # check trap
    if url == None or url == '':
        return False

    for trap in TRAP_POOL:
        if url.__contains__(trap):
            return False

    parsed = urlparse(url)
    path = parsed.path
    query = parsed.query

    split = path.split('.php')
    if len(split) > 2:
        return False

    # filter out dynamatic request url
    if (path.__contains__('.php') and path.__contains__('?')) or (
            path.__contains__('.php') and not path.endswith('.php')):
        return False

    if parsed.scheme not in set(["http", "https"]):
        return False

    try:
        return ".ics.uci.edu" in parsed.hostname \
            and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico|jpg|svg" + "|png|tiff?|mid|mp2|mp3|mp4"\
            + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf|webm" \
            + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
            + "|thmx|mso|arff|rtf|jar|csv|json|xml|eot|otf|ttf|woff|woff2"\
            + "|rm|smil|wmv|swf|wma|zip|rar|gz)$", path.lower()) and not \
                   re.match(".*\.(css|js|bmp|gif|jpe?g|ico|jpg|svg" + "|png|tiff?|mid|mp2|mp3|mp4"\
            + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf|webm" \
            + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
            + "|thmx|mso|arff|rtf|jar|csv|json|xml|eot|otf|ttf|woff|woff2"\
            + "|rm|smil|wmv|swf|wma|zip|rar|gz)$", query.lower())

    except TypeError:
        print ("TypeError for ", parsed)
        return False