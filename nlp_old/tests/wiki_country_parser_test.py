from nlp_surveillance.wiki_country_parser import get_wiki_countries_df


def test_wiki_country_parser():
    assert get_wiki_countries_df().iloc[11].tolist() == ['Andorra', 'Fürstentum Andorra', 'Andorra la Vella',
                                                         'Andorra', ['AND', 'AD'], ['FA']], \
        "Unexpected output of wiki data"
