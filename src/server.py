from flask import Flask, render_template, request

app = Flask(__name__)


def group(result):
    out = {}
    count = {}
    for r in result:
        key = r[0][1]
        url = r[0][0]
        treatment = r[0][2]
        level = r[0][3]
        if key not in out:
            out[key] = list()
            count[key] = 0
        out[key].append((level, treatment, url))
        count[key] += 1
    import operator
    keys = [i[0] for i in sorted(count.items(), key=operator.itemgetter(1))]
    return out, keys[::-1]


@app.route('/')
def index():
    return render_template('find.html',search_param_name='Карточки', search_param='card')


@app.route('/', methods=['POST'])
def my_form_post():
    from find import cards
    q = request.form['x']
    search_type = request.form['search_param']
    result = [
              # [("http://google.com", "Ветрянка", "А хрен его знает", 0)],
              [("http://google.com", "Оспа", "А хрен его знает", 4)],
              [("http://google.com", "Ветрянка", "А хрен его знает", 0)],
              [("http://google.com", "Ветрянка", "А хрен его знает", 0)],
              [("http://google.com", "Геморой", "А хрен его знает", 3)],
              [("http://google.com", "Геморой", "А хрен его знает", 3)],
              [("http://google.com", "Геморой", "А хрен его знает", 3)],
              [("http://google.com", "Геморой", "А хрен его знает", 3)],
              [("http://google.com", "Геморой", "А хрен его знает", 3)],
        [("http://google.com", "Акне", "А хрен его знает", 0)]]
    result, keys = group(result)
    divs = ['<div style="display: flex;" class="row bs-callout ' for _ in result.keys()]
    for i, key in enumerate(keys):
        items = result[key]
        if items[0][0] == 4:
            divs[i] += 'bs-callout-danger">'
        elif items[0][0] == 1:
            divs[i] += 'bs-callout-middle">'
        elif items[0][0] == 3:
            divs[i] += 'bs-callout-low">'
        else:
            divs[i] += 'bs-callout-nothing">\n'

        if len(items) == 1:
            divs[i] += '<div class="container col-sm-8 ">'
            divs[i] += '\n<h4>' + key + '</h4>'
            divs[i] += '\n<p>\n' + items[0][1] + '\n</p>'
            divs[i] += '\n<a href="' + items[0][2] + '">' + items[0][2] + '</a>'
            divs[i] += '\n</div>\n'
            if not items[0][0]:
                divs[i] += """  <div class="container col-sm-4" style="margin-left: 5%;">
                                        <div class="row">
                                            <h3>Оцените тяжесть болезни </br>
                                                <small>Чем выше тем хуже</small>
                                            </h3>
                                        </div>
                                        <div class="row lead">
                                            <div id="stars" class="starrr"></div>
                                            <h3>
                                                <small><span id="count"></span></small>
                                            </h3>
                                        </div>
                                    </div>"""
            else:
                divs[i] += '<div class="container col-sm-4"></div>'
        else:
            if not items[0][0]:
                divs[i] += '<div class="container col-sm-8 ">'
            else:
                divs[i] += '<div class="container col-sm-12 ">'
            divs[i] += '\n<h4>' + key + '</h4>'
            divs[i] += '<div class="panel-group" id="accordion">'
            for j, item in enumerate(items):
                divs[i] += """
                                <div class="panel panel-default">
                                  <div class="panel-heading">
                                    <h4>
                                      <a data-toggle="collapse" data-parent="#accordion" href="#collapse{0}"> Лечение</a>
                                    </h4>
                                  </div>
                                  <div id="collapse{0}" class="panel-collapse collapse">
                                    <div class="panel-body">
                            """.format(i * 100 + j)
                divs[i] += '\n<p>\n' + item[1] + '\n</p>'
                divs[i] += '\n<a href="' + item[2] + '">' + item[2] + '</a>'
                divs[i] += """</div>
                            </div>"""
                divs[i] += '</div>'
            divs[i] += '</div>'
            divs[i] += '</div>'
            if not items[0][0]:
                divs[i] += """  <div class="container col-sm-4" style="margin-left: 5%;">
                                        <div class="row">
                                            <h3>Оцените тяжесть болезни </br>
                                                <small>Чем выше тем хуже</small>
                                            </h3>
                                        </div>
                                        <div class="row lead">
                                            <div id="stars" class="starrr"></div>
                                            <h3>
                                                <small><span id="count"></span></small>
                                                <span id="disease" style="visibility: hidden">{0}</span>
                                            </h3>
                                        </div>
                                    </div>""".format(key)
        divs[i] += '</div>'
    search_name = 'Карточки'
    if search_type == 'list':
        search_name = 'Список'
    return render_template('find.html', divs=divs, query=q, search_param_name=search_name, search_param=search_type)


@app.route('/level', methods=['POST'])
def level():
    disease = request.get_json()
    print(disease)
    return ''

if __name__ == "__main__":
    app.run(debug=True)
