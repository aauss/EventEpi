from flask import Flask, render_template, request, jsonify, send_from_directory
import json
import os
import sys
import pickle
import time
from datetime import datetime
from nltk.tokenize import word_tokenize
sys.path.insert(0, '/home/auss/github/nlp-surveillance')
sys.path.insert(0, '/home/auss/github/nlp-surveillance/web_app')
from embedder import MeanEmbeddingTransformer

from nlp_surveillance.scraper.text_extractor import extract_cleaned_text_from_url
from nlp_surveillance.classifier.summarize import annotate_and_summarize
from nlp_surveillance.pipeline import TrainNaiveBayes

app = Flask(__name__)


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/js/<path:path>')
def send_js(path):
    return send_from_directory('js', path)


@app.route('/css/<path:path>')
def send_css(path):
    return send_from_directory('css', path)


@app.route('/images/<path:path>')
def send_png(path):
    return send_from_directory('images', path)


@app.route('/summarize', methods=['POST'])
def summarize(url=None):
    if not url:
        url = str(request.form.get('url_input', 0))
    text = extract_cleaned_text_from_url(url)
    parsed = annotate_and_summarize(text,
                                    TrainNaiveBayes('dates').data_output(),
                                    TrainNaiveBayes('counts').data_output(),
                                    )
    relevance = calculate_relevanz(text)
    parsed_as_dict = {'country': parsed["geoname"][0],
                      'confirmed': int(sum(parsed["counts"][0])/len(parsed["counts"][0])),
                      'disease': parsed["diseases"][0],
                      'date': parsed["date"][0][0][0].strftime("%d, %B, %Y"),
                      'relevance': relevance,
                      'input_date': datetime.now().strftime('%Y-%b-%d'),
                      'url': url,
                      }
    parsed_formatted = (f'In {parsed_as_dict["country"]} are around {parsed_as_dict["confirmed"]} confirmed cases '
                        f'of {parsed_as_dict["disease"]} as of {parsed_as_dict["date"]}')
    parsed_formatted = 'test'
    data = {'parsed_formatted': str(parsed_formatted),
            'as_dict': parsed_as_dict,
            }

    if not os.path.isfile('js/table.json'):
        with open('js/table.json', 'w') as f:
            data_dict = {'data': [data['as_dict']]}
            json.dump(data_dict, f)
    else:
        with open('js/table.json', 'r') as f:
            data_dict = json.load(f)
        with open('js/table.json', 'w') as f:
            data_dict['data'].append(data['as_dict'])
            json.dump(data_dict, f)
    data = jsonify(data)
    return data


def calculate_relevanz(text):
    token = word_tokenize(text)
    met = MeanEmbeddingTransformer()
    transform = met.fit_transform([token])
    with open('../mlp_clf.pkl', 'rb') as f:
        mlp_clf = pickle.load(f)
    return mlp_clf.predict_proba(transform)[:, 1][0].round(decimals=2)


@app.route('/rerun_who', methods=['GET'])
def rerun_who():
    print('I am used')
    time.sleep(5)
    with open('js/who.json', 'r') as f:
        who_dict = json.load(f)
    with open('js/table.json', 'r') as f:
        data_dict = json.load(f)
    with open('js/table.json', 'w') as f:
        data_dict['data'].extend(who_dict['data'])
        json.dump(data_dict, f)
    data = jsonify(data_dict)
    return '', 204


if __name__ == '__main__':
    app.run(debug=True)
