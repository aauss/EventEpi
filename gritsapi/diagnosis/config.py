#This was the original entry
#mongo_url = 'localhost'

import os
from urlparse import urlparse

if 'MONGO_URL' in os.environ:
  host = urlparse(os.environ['MONGO_URL']).hostname
  mongo_url = host
else:
  mongo_url = 'localhost'


