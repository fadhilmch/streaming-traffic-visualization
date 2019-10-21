import dash
import dash_core_components as dcc
import dash_html_components as html
import os
import dash_dangerously_set_inner_html

app = dash.Dash()

with open("test.html", "r", encoding='utf-8') as f:
    text= f.read()

app.layout = html.Div([
    dash_dangerously_set_inner_html.DangerouslySetInnerHTML('<h1>Title</h1>'),
    dash_dangerously_set_inner_html.DangerouslySetInnerHTML(text),
    
])

@app.server.route('/test.html')
def serve_static(resource):
    return flask.send_from_directory(STATIC_PATH, resource)

if __name__ == '__main__':
    app.run_server(debug=True)