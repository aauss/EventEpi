### Setup: 

```
pip install readability-lxml pyyaml cssselect futures beautifulsoup4
```

### Usage:

The most recent data can be found in this repository's fetch_mm-dd-yyyy branches.

Every entry in the HM data should generate a resource file, but the file might
just contain the meta data and indicate an exception happened while scraping the article.

Fetch the corpus while periodically saving the url counts and an offset you can resume from in state.json:

```
python fetch_corpora.py -username girderUname -password girderPass -state_file state.json
```

Fetch the corpus beyond the given offset and log the output
(Warning: Only duplicate urls found during the current run will be detected unless a statefile is used):

```
python fetch_corpora.py -username girderUname -password girderPass -offset 300 > log 2>&1
```

Iterate over all the training data resources in the fetched corpus:

```python
import iterate_resources
for resource in iterate_resources.iterate_resources("healthmap/train"):
    #process resource
```

### TODO:

Maybe put resource files into sub-directories with only a few hundred files each?
