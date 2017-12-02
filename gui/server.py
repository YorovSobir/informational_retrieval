from flask import Flask, render_template

app = Flask(__name__)


@app.route('/')
def index():
    from find_base import get_cards
    result = get_cards("сыпь на голове температура")
    temp = result[0]
    result[0] = result[4]
    result[4] = temp
    temp = result[1]
    result[1] = result[9]
    result[9] = temp
    divs = ['<div style="display: flex;" class="row bs-callout ' for _ in result]
    for i, res in enumerate(result):
        if res[0][3] == 1:
            divs[i] += 'bs-callout-danger">'
        elif res[0][3] == 72:
            divs[i] += 'bs-callout-middle">'
        elif res[0][3] == 89:
            divs[i] += 'bs-callout-low">'
        else:
            divs[i] += 'bs-callout-nothing">\n'
        divs[i] += '<div class="container col-sm-8 ">'
        divs[i] += '\n<h4>' + res[0][1] + '</h4>'
        divs[i] += '\n<p>\n' + res[0][2] + '\n</p>'
        divs[i] += '\n<a href="' + res[0][0] + '">' + res[0][0] + '</a>'
        divs[i] += '\n</div>\n'
        if not res[0][3]:
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
        divs[i] += '</div>'
    return render_template('find.html', divs=divs)


if __name__ == "__main__":
    app.run(debug=True)
