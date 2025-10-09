from quart import Quart, request, jsonify
from quart_cors import cors
from data_preprocessing.main import *
import ast
import inspect

app = Quart(__name__)
app = cors(app)  # Enable CORS for Grafana requests

@app.route('/data', methods=['GET'])
async def get_data():
    """
    Endpoint to return DataFrame data for both sync and async functions
    Usage: http://localhost:3003/data?fn=exam_wise_top_scorers
    """
    fn_name = request.args.get('fn')
    if 'params' in request.args:
        params = ast.literal_eval(request.args.get('params'))
    else:
        params = {}
        
    # Check if function exists
    if fn_name in globals() and callable(globals()[fn_name]):
        try:
            func = globals()[fn_name]
            
            # Check if function is async
            if inspect.iscoroutinefunction(func):
                # It's an async function - await it
                df = await func(**params)
            else:
                # It's a sync function - call normally
                df = func(**params)

            json_data = dataframe_to_json(df)
            return json_data
        except Exception as e:
            return jsonify({"error": str(e)})
    else:
        return jsonify({"error": f"Function '{fn_name}' not found."}), 400

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=3005, debug=False, workers= 1)
