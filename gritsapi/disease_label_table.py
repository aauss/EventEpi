import csv
import os

__table__ = None

# To update the csv from our google sheet use:
# wget --no-check-certificate --output-document=disease_label_table.csv 'https://docs.google.com/spreadsheet/ccc?key=1MvkBBsvGP6Ax_bPfQJupjRiPDN803IpG1vB-iFzsr6M&output=csv'
def get_table():
    global __table__
    if __table__:
        return __table__
    __table__ = []
    curdir = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(curdir, "disease_label_table.csv")) as f:
        header = csv.reader(f, delimiter=',', quoting=csv.QUOTE_MINIMAL).next()
        reader = csv.DictReader(f,
            fieldnames=header,
            delimiter=',',
            quoting=csv.QUOTE_MINIMAL)
        for row in reader:
            out_row = {}
            for key, value in row.items():
                value = value.strip()
                if value == "TRUE":
                    out_row[key] = True
                elif value == "FALSE":
                    out_row[key] = False
                elif value == "":
                    continue
                else:
                    out_row[key] = value
            if 'label' in out_row:
                __table__.append(out_row)
    return __table__

def get_promed_labels():
    labels = []
    for row in get_table():
        if row.get('is_promed_label'):
            labels.append(row['label'])
    return labels

def get_labels():
    labels = []
    for row in get_table():
        labels.append(row['label'])
    return labels

def is_in_table(disease):
    for row in get_table():
        if row['label'] == disease:
            return True
    return False

def is_not_human_disease(disease):
    for row in get_table():
        if row['label'] == disease:
            return row.get('is_not_disease') or row.get('not_human_disease')
    print "WARNING: Unknown disease label:", disease

__disease_to_parents__ = None
def get_inferred_labels(disease):
    global __disease_to_parents__
    if not __disease_to_parents__:
        __disease_to_parents__ = {}
        for row in get_table():
            if 'parent_label' in row:
                __disease_to_parents__[row['label']] = [row['parent_label']]
    inferred_labels = []
    parents = __disease_to_parents__.get(disease, [])
    unresolved_labels = list(parents)
    while len(unresolved_labels) > 0:
        label = unresolved_labels.pop()
        if label in inferred_labels: continue
        inferred_labels.append(label)
        new_parents = list(__disease_to_parents__.get(label, []))
        unresolved_labels += new_parents
    return inferred_labels
