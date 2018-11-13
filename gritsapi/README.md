# grits-api

This project provides the backend for the GRITS [diagnostic-dashboard](https://github.com/ecohealthalliance/diagnostic-dashboard). The main API which it furnishes, accessible at `/diagnose`, takes an incoming document and returns a differential disease diagnosis and numerous extracted features for that document.

This project also provides resources for training the classifier model used to make disease predictions, and for managing long-running classification tasks over large corpora.

# Dependencies

Aside from the requirments noted in [requirements.txt](requirements.txt) which may be installed as usual with `pip install -r requirements.txt`, this project also relies on the annotation library [annie](https://github.com/ecohealthalliance/annie).

# Installation and set-up

## Full setup with virtualenv

These instructions will get `grits-api` working under a Python virtualenv.

First, get a copy of the Girder data (backed up in S3 - the bucket is girder-data/proddump/girder). This will give you the file item.bson.

Next, start mongo on port 27017 by running `mongod` and restore the girder database:

    mongorestore --host=127.0.0.1 --port=27017 -d girder PATH/TO/item.bson

Clone grits-api

    git clone git@github.com:ecohealthalliance/grits-api.git
    cd grits-api

Get a copy of `config.py` from someone at EHA (this contains sensitive AWS authentication information).

If you do not have `virtualenv`, first install it globally.

    sudo pip install virtualenv

Now create and enter the virtual environment. All `pip` and `python` commands from here should be run from within the environment. Leave the environment with the `deactivate` command.

    virtualenv venv
    source venv/bin/activate

Install `grits-api` dependencies and `nose`.

    pip install -r requirements.txt
    pip install nose

If lxml fails to install, run (in bash) `STATIC_DEPS=true pip install lxml`

Clone and install `annie`.

    cd ../
    git clone git@github.com:ecohealthalliance/annie.git
    cd annie
    pip install -r requirements.txt
    python setup.py install

Clone other required repos:

    cd ../
    git clone https://github.com/ecohealthalliance/jvm-nlp.git
    git clone https://github.com/ecohealthalliance/diagnostic-dashboard.git

Train the GRITS api:

    cd grits-api
    mkdir current_classifier
    python train.py -pickle_dir current_classifier

Download NLTK data (within a python interpreter):

    import nltk
    nltk.download()

A window will prompt you to choose what to download; download everything.

Start the server:

    python server.py -debug

Start the diagnostic dashboard:

    cd ../diagnostic-dashboard
    meteor


## As part of total GRITS deployment

You may elect to install all GRITS components at once (this backend, the front-end [diagnostic-dashboard](https://github.com/ecohealthalliance/diagnostic-dashboard), and the [girder](https://github.com/ecohealthalliance/girder) database) by following the instructions in the [grits-deploy-ansible](https://github.com/ecohealthalliance/grits-deploy-ansible) project.

The provided ansible playbook will install all dependencies, include nltk data and annie, and use `supervisorctl` to launch the API server and celery processes for managing diagnoses. There are 3 celery task queues, `priority`, `process` and `diagnose`. The process queue is for scraping and extracting articles prior to diagnosis. We recommend running a single threaded worker process on the process queue because it primarily makes http requests, so it spends most of it's time idling. The diagnose queue should have several worker processes as it is very CPU intensive. The priority queue is for both processing and diagnosing articles and should have a dedicated worker process for immediatly diagnosing individual articles. See the supervisor config in the grits-deploy-ansible for examples of how to initialize the various types of workers.


## Standalone

To run this project in isolation, without deploying the entire GRITS suite, clone this repository:

    $ git@github.com:ecohealthalliance/grits-api.git

Copy the default config to the operative version and edit it to suit your environment:

    $ cp config.sample.py config.py

Install the pip requirements:

    $ sudo pip install -r requirements.txt

Get the [annie](https://github.com/ecohealthalliance/annie) project and make sure it's in your pythonpath.

Start a celery worker:

	$ celery worker -A tasks -Q priority --loglevel=INFO --concurrency=2

Start the server:

	# The -debug flag will run a celery worker synchronously in the same process,
	# so you can debug without starting a separate worker process.
	$ python server.py

# Testing

To run the tests:

    git clone -b fetch_4-18-2014 git@github.com:ecohealthalliance/corpora.git
    cd test
    python -m unittest discover

Many tests are based on the comments in this document:
https://docs.google.com/document/d/12N6hIDiX6pvIBfr78BAK_btFxqHepxbrPDEWTCOwqXk/edit


# Classifier Data

## Using existing classifier data

A corpus of HealthMap articles in the girder database is used to train the classifier. It must be manually downloaded and restored to the db. The database collection can be obtained from S3, in the bucket girder-data/proddump/girder. 
One additional file is required to operate the classifier: ontologies.p.
It will be downloaded from our S3 bucket by default, however that bucket might not
be available to you, or it might no longer exist. In that case, ontologies.p can be
generated by running the mine_ontologies.py.
The HealthMap data, however, can no longer be generated from a script.

The corpora directory includes code for iterating over HealthMap data stored
in a girder database, scraping and cleaning the content of the linked source articles,
and generating pickles from it.

# Training the classifier

New data may be generated using the `train.py` script:

    $ python train.py

This script relies on having the HealthMap articles available in the girder database.


## License
Copyright 2016 EcoHealth Alliance

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
