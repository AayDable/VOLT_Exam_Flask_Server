from quart import Quart, request, jsonify, send_file,make_response, Request
from quart_cors import cors
from data_preprocessing.main import *
import ast
import inspect
import os
from cache import cache_manager
import base64
import secrets 
from context import current_batch_id

app = Quart(__name__)
app = cors(app, expose_headers=['Content-Disposition'])  # Enable CORS for Grafana requests

def extract_params(request: Request):
    params = request.args.to_dict(flat=True)
    params.pop('fn', None)

    for key, val in params.items():
        if isinstance(val, str) and val.startswith('"""') and val.endswith('"""'):
            params[key] = val[3:-3]

    return params

def check_authorization():
    # Check Authorization header
    auth_header = request.headers.get('Authorization')
    
    if not auth_header:
        return jsonify({"error": "Unauthorized - No token provided"}), 401
    
    if not auth_header.startswith('Bearer '):
        return jsonify({"error": "Unauthorized - Invalid token format"}), 401
    
    # Extract token (remove "Bearer " prefix)
    provided_token = auth_header[7:]
    
    # Use constant-time comparison to prevent timing attacks
    if secrets.compare_digest(provided_token, os.getenv('GF_DATASOURCE_KEY')):
        return True
    else:
        return False
    
@app.route('/data', methods=['GET'])
async def get_data():
    """
    Endpoint to return DataFrame data for both sync and async functions
    Usage: http://localhost:3003/data?fn=exam_wise_top_scorers
    """
    if check_authorization() == False:
        return jsonify({"error": "Unauthorized - Invalid token"}), 403

    try:
        fn_name = request.args.get('fn')
        # if 'params' in request.args:
        #     params = ast.literal_eval(request.args.get('params'))
        # else:
        #     params = {}
        params = extract_params(request)

        batch_id = params.get("batch_id")
        params.pop("batch_id")

        # Set the value for this request
        token = current_batch_id.set(batch_id)

        # Check if function exists
        if fn_name in globals() and callable(globals()[fn_name]):
            func = globals()[fn_name]

            if 'user_id' in request.headers:
                params['user_id'] = request.headers.get('user_id').upper()
                
            # Check if function is async
            if inspect.iscoroutinefunction(func):
                # It's an async function - await it
                df = await func(**params)
            else:
                # It's a sync function - call normally
                df = func(**params)

            json_data = dataframe_to_json(df)
            return json_data
        else:
            return jsonify({"error": f"Function '{fn_name}' not found."}), 400
    except Exception as e:
        return jsonify({"error": str(e)})
    finally:
        current_batch_id.reset(token)

@app.route('/pdf_data')
async def pdf_data():
    """
    Endpoint to return PDF data for both sync and async functions
    Usage: http://localhost:3003/pdf_data?fn=report_card_trainee
    """

    if check_authorization() == False:
        return jsonify({"error": "Unauthorized - Invalid token"}), 403

    try:
        fn_name = request.args.get('fn')

        params = extract_params(request)

        batch_id = params.get("batch_id")
        params.pop("batch_id")

        # Set the value for this request
        token = current_batch_id.set(batch_id)

        # Check if function exists
        if fn_name in globals() and callable(globals()[fn_name]):
            func = globals()[fn_name]

            if 'user_id' in request.headers:
                params['user_id'] = request.headers.get('user_id').upper()
                
            # Check if function is async
            if inspect.iscoroutinefunction(func):
                pdf_bytes = await func(**params)
            else:
                pdf_bytes = func(**params)

            # Encode the PDF bytes directly
            encoded_pdf = base64.b64encode(pdf_bytes).decode('utf-8')

            return jsonify({
                "filename": f"{fn_name}.pdf",
                "contentType": "application/pdf",
                "blob": encoded_pdf
            })
        else:
            return jsonify({"error": f"Function '{fn_name}' not found."}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        current_batch_id.reset(token)

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=3005, debug=False, workers= 1)
