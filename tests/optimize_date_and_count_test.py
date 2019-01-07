import pandas as pd
import numpy as np
from pandas.util.testing import assert_frame_equal
from nlp_surveillance.optimize_date_and_count import _get_edb_with_combined_link_columns, _remove_invalid_entries


def test_get_edb_with_combined_link_columns():
    dummy_edb = pd.DataFrame({'dates': ['2018-09-01', '2018-09-02', '2018-09-03', '2018-09-04'],
                              'link1': ['http://some_url1', 'http://some_url2', 'http://some_url3', None],
                              'link2': ['http://other_url1, comma in column', None, np.nan, None]})

    links_combined = _get_edb_with_combined_link_columns(dummy_edb)
    expected = pd.DataFrame({'dates': ['2018-09-01', '2018-09-02', '2018-09-03', '2018-09-04'],
                             'links': [['http://some_url1', 'http://other_url1', 'comma in column'],
                                       ['http://some_url2', None],
                                       ['http://some_url3', np.nan],
                                       np.nan]})

    assert_frame_equal(links_combined, expected)


def test_remove_invalid_entries():
    to_clean = pd.DataFrame({'Datenstand für Fallzahlen gesamt*': ['2018-09-01', '2018-09-02'],
                             'links': [['<http://apps.who.int/iris/bitstream/10665/260468/1/OEW10-39032018.pdf>',
                                        np.nan, 'Fälle aus 5 Departments: Atacora (9)', 'https://www.who.int'],
                                       ['https://www.who.int/asda']]
                             })

    entries_removed = _remove_invalid_entries(to_clean, to_optimize='date')
    expected = pd.DataFrame({'Datenstand für Fallzahlen gesamt*': ['2018-09-01', '2018-09-02'],
                             'links': [
                                 ['http://apps.who.int/iris/bitstream/10665/260468/1/OEW10-39032018.pdf',
                                  'https://www.who.int'],
                                 ['https://www.who.int/asda']
                             ]})
    assert_frame_equal(entries_removed, expected)


