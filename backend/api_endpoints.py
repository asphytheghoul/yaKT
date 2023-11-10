def handle_request(path, request_handler, record_manager):
   if path == "/create_record":
       # extract record_id and record from request_handler
       record_manager.create_record(record_id, record)
       request_handler.send_response(200)
       request_handler.end_headers()
   elif path == "/read_record":
       # extract record_id from request_handler
       record = record_manager.read_record(record_id)
       request_handler.send_response(200)
       request_handler.end_headers()
       request_handler.wfile.write(bytes(str(record), 'utf-8'))
   elif path == "/delete_record":
       # extract record_id from request_handler
       record_manager.delete_record(record_id)
       request_handler.send_response(200)
       request_handler.end_headers()
