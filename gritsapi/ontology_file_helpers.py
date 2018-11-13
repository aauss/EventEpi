from boto.s3.connection import S3Connection, Location
import boto
import config
import glob


def get_minor_version(fileName):
    pieces = fileName.split(".")
    minorVersion = pieces[len(pieces)-2]
    return minorVersion

# returns all of the file name except for the minor version.  So for "ontologies-0.1.5.p" it would return "ontologies-0.1."
def get_ontology_filename_prefix(fileName):
    pieces = fileName.split(".")
    return ".".join(pieces[:len(pieces)-2])

# @memoized
def get_ontology_files():
    conn = S3Connection(config.aws_access_key, config.aws_secret_key)
    bucket = conn.get_bucket('classifier-data')
    return bucket.list(prefix="ontologies-")

#this returns the two most recent pickles.  Can we rely on same return order every time?
def get_ontologies_to_compare():
    ontologies = glob.glob("ontologies-*.p")
    return ontologies[-1], ontologies[-2]

def push_latest_ontology_file(ontologyFile):
    print "uploading new pickle to S3: ", ontologyFile.name
    # conn = boto.s3.connect_to_region('us-east-1', aws_access_key_id=config.aws_access_key, aws_secret_access_key=config.aws_secret_key)
    # conn = S3Connection(config.aws_access_key, config.aws_secret_key)
    conn = S3Connection(config.aws_access_key, config.aws_secret_key, host="s3.amazonaws.com")
    bucket = conn.get_bucket('classifier-data')
    key = boto.s3.key.Key(bucket, ontologyFile.name )
    with open(ontologyFile.name) as f:
        key.send_file(f)
    print "done uploading new pickle!"

def get_next_ontology_file_name():
    versionNumbers = []
    ontologies = get_ontology_files()
    fileNamePrefix = get_ontology_filename_prefix(list(ontologies)[-1].name)
    for ontology in ontologies:
        # print get_ontology_filename_prefix(ontology.name)
        version = get_minor_version(ontology.name)
        if version.isdigit():
            versionNumbers.append(version)
    # need to figure out how to increment when we want to create a new pickle (not just update another one)
    return fileNamePrefix + "." + `(max(int(s) for s in versionNumbers))` + ".p"
    # return fileNamePrefix + "." + `(max(int(s) for s in versionNumbers) + 1)` + ".p"