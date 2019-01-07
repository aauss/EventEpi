from nlp_surveillance.utils.text_from_url import clean_text, _lemmatize


def test_clean_text():
    actual = clean_text("These are a sample sentences, showing off the stop words filtration.")
    expected = ['These', 'sample', 'sentence', 'showing', 'stop', 'word', 'filtration']
    assert actual == expected


def test_lemmatize():
    words = ['houses', 'Hannah', '12']
    actual = [_lemmatize(word) for word in words]
    expected = ['house', 'Hannah', '12']
    assert actual == expected
