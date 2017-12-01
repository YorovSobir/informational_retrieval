from flask import Flask, render_template

app = Flask(__name__)


@app.route('/')
def index():
    from find_base import get_cards
    result = get_cards("снижение массы тела склонность к кровотечениям")
    divs = ['<div class="bs-callout ' for _ in result]
    for i, res in enumerate(result):
        if res[0][3] == 1:
            divs[i] += 'bs-callout-danger">'
        elif res[0][3] == 72:
            divs[i] += 'bs-callout-middle">'
        elif res[0][3] == 89:
            divs[i] += 'bs-callout-low">'
        else:
            divs[i] += 'bs-callout-nothing">'

        divs[i] += '\n<h4>' + res[0][1] + '</h4>'
        divs[i] += '\n<p>\n' + res[0][2] + '\n</p>'
        divs[i] += '\n<a href="' + res[0][0] + '">' + res[0][0] + '</a>'
        divs[i] += '\n</div>'

    return render_template('find.html', divs=divs)


if __name__ == "__main__":
    app.run(debug=True)
